# Copyright 2016-2018 Workiva Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# system imports
import json
import importlib
from threading import RLock
import uuid
import logging
import time
import random

# library imports
from botocore.exceptions import ClientError

# application imports
from aws_lambda_fsm.state import State
from aws_lambda_fsm.transition import Transition
from aws_lambda_fsm.config import get_current_configuration
from aws_lambda_fsm.aws import send_next_event_for_dispatch
from aws_lambda_fsm.aws import store_checkpoint
from aws_lambda_fsm.aws import start_retries
from aws_lambda_fsm.aws import stop_retries
from aws_lambda_fsm.aws import set_message_dispatched
from aws_lambda_fsm.aws import get_message_dispatched
from aws_lambda_fsm.aws import increment_error_counters
from aws_lambda_fsm.aws import acquire_lease
from aws_lambda_fsm.aws import release_lease
from aws_lambda_fsm.constants import MACHINE
from aws_lambda_fsm.constants import CONFIG
from aws_lambda_fsm.constants import STATE
from aws_lambda_fsm.constants import OBJ
from aws_lambda_fsm.constants import SYSTEM_CONTEXT
from aws_lambda_fsm.constants import PAYLOAD
from aws_lambda_fsm.constants import AWS
from aws_lambda_fsm.constants import ERRORS


class Object(object):
    pass


_local = Object()
_lock = RLock()
_local.machines = None

logger = logging.getLogger(__name__)


class FSM(object):
    """
    State Machine Factory.
    """

    def __init__(self, config_dict=None):
        """
        Constructs the factory and initializes the State/Transition caches.

        :param config_dict: a dict as returned by aws_lambda_fsm.config.get_current_configuration
        """
        reload_machines = (config_dict is not None)
        if not reload_machines:
            config_dict = get_current_configuration()
        with _lock:
            if _local.machines is None or reload_machines:
                self.machines = None
                self._init_(config_dict=config_dict)
                _local.machines = self.machines
            else:
                self.machines = _local.machines

    def _init_(self, config_dict):
        """
        Initializes the State/Transition caches.

        :param config_dict: a dict as returned by aws_lambda_fsm.config.get_current_configuration
        """
        self.machines = {} if (self.machines is None) else self.machines
        for machine_dict in config_dict[CONFIG.MACHINES]:

            if 'import' in machine_dict:
                another_config_dict = get_current_configuration(filename=machine_dict[CONFIG.IMPORT])
                self._init_(another_config_dict)
                continue

            try:
                # initialize the machine dict
                machine_name = machine_dict[CONFIG.NAME]
                if machine_name not in self.machines:
                    self.machines[machine_name] = {MACHINE.STATES: {}, MACHINE.TRANSITIONS: {}}
                    # pseudo-init, pseudo-final
                    self.machines[machine_name][MACHINE.STATES][STATE.PSEUDO_INIT] = State(STATE.PSEUDO_INIT)
                    self.machines[machine_name][MACHINE.STATES][STATE.PSEUDO_FINAL] = State(STATE.PSEUDO_FINAL)

                # set the max-retries
                self.machines[machine_name][MACHINE.MAX_RETRIES] = \
                    int(machine_dict.get(CONFIG.MAX_RETRIES, CONFIG.DEFAULT_MAX_RETRIES))

                # fetch these for use later
                pseudo_init = self.machines[machine_name][MACHINE.STATES][STATE.PSEUDO_INIT]
                pseudo_final = self.machines[machine_name][MACHINE.STATES][STATE.PSEUDO_FINAL]

                # iterate over each state, creating a singleton
                for state_dict in machine_dict[CONFIG.STATES]:
                    state_name = state_dict[CONFIG.NAME]
                    entry_action = self._get_action(state_dict.get(CONFIG.ENTRY_ACTION))
                    do_action = self._get_action(state_dict.get(CONFIG.DO_ACTION))
                    exit_action = self._get_action(state_dict.get(CONFIG.EXIT_ACTION))
                    state = State(state_name,
                                  entry_action=entry_action,
                                  do_action=do_action,
                                  exit_action=exit_action,
                                  initial=state_dict.get(CONFIG.INITIAL),
                                  final=state_dict.get(CONFIG.FINAL))
                    self.machines[machine_name][MACHINE.STATES][state_name] = state

                    # pseudo-transitions
                    if state_dict.get(CONFIG.INITIAL):
                        self._add_transition(machine_name, pseudo_init, state, STATE.PSEUDO_INIT)
                    if state_dict.get(CONFIG.FINAL):
                        self._add_transition(machine_name, state, pseudo_final, STATE.PSEUDO_FINAL)

                # iterate over each transition, creating a singleton
                for state_dict in machine_dict[CONFIG.STATES]:
                    state_name = state_dict[CONFIG.NAME]
                    state = self.machines[machine_name][MACHINE.STATES][state_name]
                    for transition_dict in state_dict.get(CONFIG.TRANSITIONS, []):
                        action = self._get_action(transition_dict.get(CONFIG.ACTION))
                        event = transition_dict[CONFIG.EVENT]
                        target_name = transition_dict[CONFIG.TARGET]
                        target = self.machines[machine_name][MACHINE.STATES][target_name]
                        self._add_transition(machine_name, state, target, event, action=action)

            except (ImportError, ValueError), e:  # pragma: no cover
                logger.warning('Problem importing machine "%s": %s', machine_name, e)
                self.machines.pop(machine_name, None)

    def _add_transition(self, machine_name, source, target, event, action=None):
        """
        A helper function to and an aws_lambda_fsm.transition.Transition instance to the machine.

        :param machine_name: a str name for the machine.
        :param source: an aws_lambda_fsm.state.State instance.
        :param target: an aws_lambda_fsm.state.State instance.
        :param event: a str event.
        :param action: an optional aws_lambda_fsm.action.Action instance.
        """
        transition_name = source.name + '->' + target.name + ':' + event
        transition = Transition(transition_name, target, action=action)
        self.machines[machine_name][MACHINE.TRANSITIONS][transition_name] = transition
        source.add_transition(transition, event)

    def _get_action(self, action_string):
        """
        A helper function to dynamically import and construct an aws_lambda_fsm.action.Action instance.

        :param action_string: a str like 'path.to.ActionClass'
        :return: an instance of path.to.ActionClass.
        """
        if action_string:
            parts = action_string.split('.')
            module_name = '.'.join(parts[0:-1])
            class_name = parts[-1]
            module = importlib.import_module(module_name)
            return getattr(module, class_name)(action_string)

    def create_FSM_instance(self, machine_name,
                            initial_system_context=None,
                            initial_user_context=None,
                            initial_state_name=STATE.PSEUDO_INIT):
        """
        Factory method to create a State Machine instance (Context)

        :param machine_name: a str machine name.
        :param initial_state_name: a str state name.
        :return: an aws_lambda_fsm.fsm.Context instance.
        """
        initial_state = self.machines[machine_name][MACHINE.STATES][initial_state_name]
        max_retries = self.machines[machine_name][MACHINE.MAX_RETRIES]
        return Context(machine_name,
                       initial_system_context=initial_system_context,
                       initial_user_context=initial_user_context,
                       initial_state=initial_state,
                       max_retries=max_retries)


def _run_once_sucessfully(f):
    """
    Decorator that uses a cache to flag an action as already executed, and check to
    ensure that it hasn't been executed before running (in the event of retries,
    multiple failures).

    :param f: a function/method to run only once.
    """
    def inner(self, *args, **kwargs):

        # abort if these message has already been processed and another event message
        # has already been emitted to drive the state machine forward
        primary = get_message_dispatched(self.correlation_id, self.steps, primary=True)
        secondary = get_message_dispatched(self.correlation_id, self.steps, primary=False)
        dispatched = primary or secondary

        if dispatched:
            self._queue_error(ERRORS.DUPLICATE, 'Message has been processed already (%s).' % dispatched)
            return

        f(self, *args, **kwargs)

        # once the message is emitted, we want to make sure the current event is never sent again.
        # the approach here is to simply use a cache to set a key like "correlation_id-steps"
        primary = set_message_dispatched(self.correlation_id, self.steps, self.retries, primary=True)
        secondary = set_message_dispatched(self.correlation_id, self.steps, self.retries, primary=False)
        dispatched = primary and secondary  # 'and' is correct here. it just triggers an alarm.

        if not dispatched:
            self._queue_error(ERRORS.CACHE, 'Unable set message dispatched for idempotency.')

    return inner


class Context(dict):
    """
    State Machine.
    """

    def __init__(self, name,
                 initial_system_context=None,
                 initial_user_context=None,
                 initial_state=None,
                 max_retries=None):
        """
        Construct a state machine instance.

        :param name: a str name for the state machine.
        :param initial_system_context: a dict of initial data for the system context.
        :param initial_user_context: a dict of initial data for the context.
        :param initial_state: an aws_lambda_fsm.state.State instance.
        """
        # only the current state and transition are stored as attributes
        # every other property is store in the __system_context dictionary
        # to make serialization easier.
        self.current_state = initial_state
        self.current_transition = None

        # init the user dict
        if initial_user_context:
            self.update(initial_user_context)

        # init the system dict
        self.__system_context = {}
        if initial_system_context:
            self.__system_context.update(initial_system_context)

        # immutable
        self.__system_context[SYSTEM_CONTEXT.MACHINE_NAME] = \
            self.__system_context.get(SYSTEM_CONTEXT.MACHINE_NAME, name)
        self.__system_context[SYSTEM_CONTEXT.MAX_RETRIES] = \
            self.__system_context.get(SYSTEM_CONTEXT.MAX_RETRIES, max_retries)

        self._errors = {}

    # Immutable properties

    @property
    def name(self):
        return self.__system_context[SYSTEM_CONTEXT.MACHINE_NAME]

    @property
    def correlation_id(self):
        if SYSTEM_CONTEXT.CORRELATION_ID not in self.__system_context:
            self.__system_context[SYSTEM_CONTEXT.CORRELATION_ID] = uuid.uuid4().hex
        return self.__system_context[SYSTEM_CONTEXT.CORRELATION_ID]

    @property
    def max_retries(self):
        return self.__system_context[SYSTEM_CONTEXT.MAX_RETRIES]

    # Mutable properties

    @property
    def current_event(self):
        return self.__system_context.get(SYSTEM_CONTEXT.CURRENT_EVENT)

    @current_event.setter
    def current_event(self, current_event):
        self.__system_context[SYSTEM_CONTEXT.CURRENT_EVENT] = current_event

    @property
    def steps(self):
        if SYSTEM_CONTEXT.STEPS not in self.__system_context:
            self.__system_context[SYSTEM_CONTEXT.STEPS] = 0
        return self.__system_context[SYSTEM_CONTEXT.STEPS]

    @steps.setter
    def steps(self, steps):
        self.__system_context[SYSTEM_CONTEXT.STEPS] = steps

    @property
    def retries(self):
        if SYSTEM_CONTEXT.RETRIES not in self.__system_context:
            self.__system_context[SYSTEM_CONTEXT.RETRIES] = 0
        return self.__system_context[SYSTEM_CONTEXT.RETRIES]

    @retries.setter
    def retries(self, retries):
        self.__system_context[SYSTEM_CONTEXT.RETRIES] = retries

    # lease_primary indicates which cache system (primary or secondary) is used
    # to store the lease for this machine's correlation_id. in failover, we want
    # to continue to use the same system for the remainder of the machine's
    # execution.

    @property
    def lease_primary(self):
        if SYSTEM_CONTEXT.LEASE_PRIMARY not in self.__system_context:
            self.__system_context[SYSTEM_CONTEXT.LEASE_PRIMARY] = True
        return self.__system_context[SYSTEM_CONTEXT.LEASE_PRIMARY]

    @lease_primary.setter
    def lease_primary(self, lease_primary):
        self.__system_context[SYSTEM_CONTEXT.LEASE_PRIMARY] = lease_primary

    # Serialization helpers

    def system_context(self):
        return dict(self.__system_context)

    def user_context(self):
        return dict(self)

    def to_payload_dict(self):
        system_context = self.system_context()
        system_context[SYSTEM_CONTEXT.CURRENT_STATE] = self.current_state.name
        user_context = self.user_context()
        return {
            PAYLOAD.VERSION: PAYLOAD.DEFAULT_VERSION,
            PAYLOAD.SYSTEM_CONTEXT: system_context,
            PAYLOAD.USER_CONTEXT: user_context
        }

    @staticmethod
    def from_payload_dict(payload):
        user_context = payload[PAYLOAD.USER_CONTEXT]
        system_context = payload[PAYLOAD.SYSTEM_CONTEXT]
        return FSM().create_FSM_instance(
            system_context[SYSTEM_CONTEXT.MACHINE_NAME],
            initial_user_context=user_context,
            initial_system_context=system_context,
            initial_state_name=system_context[SYSTEM_CONTEXT.CURRENT_STATE]
        )

    # Protected helper methods

    def _start_retries(self, retry_data, obj, recovering=False):
        """
        Saves the current payload, with modified retry information, to DynamoDB
        so that a query can pick up the items, and re-execute the payload at a
        future point.

        :param retry_data: a dict like {'system_context': {...}, 'user_context': {...}}
        :param obj: a dict
        """
        retry_system_context = retry_data[PAYLOAD.SYSTEM_CONTEXT]
        serialized = json.dumps(retry_data, sort_keys=True)

        for primary in [True, False]:
            try:
                # save the retry entity
                # https://www.awsarchitectureblog.com/2015/03/backoff.html
                # "full jitter"
                cap, base, attempt = 60., 1., retry_system_context[SYSTEM_CONTEXT.RETRIES]
                sleep = random.uniform(0, min(cap, base * 2 ** attempt))
                return start_retries(
                    self,
                    time.time() + sleep,
                    serialized,
                    primary=primary,
                    recovering=recovering
                )
            except ClientError:
                # log an error to at least expose the error
                self._queue_error(
                    ERRORS.ERROR,
                    'Unable to save last payload for retry (primary=%s).' % primary,
                    exc_info=True)

    def _stop_retries(self, obj):
        """
        Deletes the retry entity to stop retries. They will no longer appear
        in the query, and no longer get retried.

        :param obj: a dict.
        """
        # if was was a retry, delete the entity tracking the retries. we _could_ simply
        # just delete, since dyanamodb is idempotent, but that consumes write capacity,
        # which we want to preserve.
        if obj[OBJ.SOURCE] == AWS.DYNAMODB_RETRY:

            for primary in [True, False]:
                try:
                    return stop_retries(
                        self,
                        primary=primary
                    )
                except ClientError:
                    # if deleting the entity fails, the entity will be picked up again
                    # and retried. however, the idempotency code will detect the message
                    # has been already executed, and nothing terrible will happen.
                    self._queue_error(
                        ERRORS.ERROR,
                        'Unable to terminate retries (primary=%s).' % primary,
                        exc_info=True)

    def _store_checkpoint(self, obj):
        """
        Saves the last response from Context._send_next_event_for_dispatch so that
        a terminated machine can be started back up using the saved information.

        :param obj: a dict.
        """
        # save the last successful dispatch to aws. on kinesis, the sent data looks like
        # {u'ShardId': u'shardId-000000000000', u'SequenceNumber': u'49559000...18786'} and thus
        # has sufficient information to go and seek the record directory from kinesis, and to
        # restart the fsm using the saved state.
        if obj.get(OBJ.SENT):

            for primary in [True, False]:
                try:
                    return store_checkpoint(
                        self,
                        json.dumps(obj[OBJ.SENT], sort_keys=True,
                                   default=lambda x: '<skipped>'),
                        primary=primary
                    )
                except ClientError:
                    # if unable to save the last sent message, then recovery/checkpointing
                    # will be missing the more recent executed state. recovering may be
                    # complicated, especially since the last transition has been marked as
                    # successfully dispatched
                    self._queue_error(
                        ERRORS.ERROR,
                        'Unable to save last sent data (primary=%s).' % primary,
                        exc_info=True)

    def _send_next_event_for_dispatch(self, serialized, obj, recovering=False):
        """
        Send the next event for dispatch to the primary/secondary stream systems.

        :param serialized: a str serialized message.
        :param obj: a dict.
        :param recovering: indicate this dispatch is in an error path
        """
        for primary in [True, False]:
            try:
                return send_next_event_for_dispatch(
                    self,
                    serialized,
                    self.correlation_id,
                    delay=obj.get(OBJ.DELAY, 0),
                    primary=primary,
                    recovering=recovering
                )
            except ClientError:
                self._queue_error(
                    ERRORS.ERROR,
                    'Unable to send next event (primary=%s).' % primary,
                    exc_info=True)
                if not primary and recovering:
                    raise

    def _queue_error(self, error_name, message, exc_info=None):
        """
        Maintains an internal dictionary of errors and the number of times they have
        occurred. Used for batching errors into a single API call to AWS.

        :param error_name: a str like "error" or "fatal"
        """
        logger.error(message, exc_info=exc_info)
        self._errors[error_name] = self._errors.get(error_name, 0) + 1

    def _send_queued_errors(self):
        """
        Takes the internal dictionary, creates some dimensions, and passes the data
        along to the AWS API aws_lambda_fsm.aws.increment_error_counters.
        """
        # create some dimensions for the error emitted in this method
        error_dimensions = {
            SYSTEM_CONTEXT.MACHINE_NAME: self.__system_context[SYSTEM_CONTEXT.MACHINE_NAME],
            SYSTEM_CONTEXT.CURRENT_STATE: self.__system_context[SYSTEM_CONTEXT.CURRENT_STATE],
            SYSTEM_CONTEXT.CURRENT_EVENT: self.__system_context[SYSTEM_CONTEXT.CURRENT_EVENT]
        }
        # emit a general error to cloudwatch to indicate something is going wrong
        if self._errors:
            increment_error_counters(
                self._errors,
                dimensions=error_dimensions
            )

    ################################################################################
    # START: dispatch logic
    ################################################################################

    @_run_once_sucessfully
    def _dispatch_to_current_state(self, event, obj):
        """
        Dispatches the event to the current state, then send the next event
        onto Kinesis/DynamoDB for subsequent processing.

        :param event: a str event.
        :param obj: a dict.
        """
        # dispatch the event using the user context only
        next_event = self.current_state.dispatch(self, event, obj)

        # if there are more events
        if next_event:

            # make a full copy
            ctx = Context.from_payload_dict(self.to_payload_dict())
            ctx.steps += 1
            ctx.retries = 0
            ctx.current_event = next_event
            serialized = json.dumps(ctx.to_payload_dict(), sort_keys=True)

            # dispatch the next event to aws kinesis/dynamodb
            sent = self._send_next_event_for_dispatch(
                serialized,
                obj
            )

            # things are falling off the rails
            if not sent:
                self._queue_error(ERRORS.DISPATCH, 'System error during dispatch. Failover to retry stream.')
                sent = self._send_next_event_for_dispatch(
                    serialized,
                    obj,
                    recovering=True
                )

            obj[OBJ.SENT] = sent

    def _dispatch(self, event, obj):
        """
        Dispatch an event to the state machine, store checkpointing info, and
        cleanup any retry information.

        :param event: a str event.
        :param obj: a dict.
        """
        self._dispatch_to_current_state(event, obj)
        self._store_checkpoint(obj)
        self._stop_retries(obj)

    def _retry(self, obj):
        """
        Handle the unhappy path errors by saving the last payload with the "retries"
        parameter increased by 1. If too many retries have been attempted, terminate
        the machine, and record a FATAL error.

        :param obj: a dict.
        """
        # fetch the original payload from the obj in-memory data. we grab the original
        # payload rather than the current context to avoid passing any vars that were
        # potentially mutated up to this point.
        payload = obj[OBJ.PAYLOAD]
        retry_data = json.loads(payload)
        retry_system_context = retry_data[PAYLOAD.SYSTEM_CONTEXT]
        retry_system_context[SYSTEM_CONTEXT.RETRIES] += 1

        # determine how many times this has been retried, and if it has been retried
        # too many times, then stop it permanently
        if retry_system_context[SYSTEM_CONTEXT.RETRIES] <= self.max_retries:
            self._queue_error(ERRORS.RETRY, 'More retries allowed (retry=%d, max=%d). Retrying.' %
                              (retry_system_context[SYSTEM_CONTEXT.RETRIES], self.max_retries))
            retried = self._start_retries(retry_data, obj)

            # things are falling off the rails
            if not retried:
                self._queue_error(ERRORS.RETRY, 'System error during retry. Failover to event stream.')
                self._start_retries(retry_data, obj, recovering=True)

        # if there are no more retries available, simply log an error, then delete
        # the retry entity from dynamodb. it will take human intervention to recover things
        # at this point.
        else:
            self._queue_error(ERRORS.FATAL, 'No more retries allowed (retry=%d, max=%d). Terminating.' %
                              (retry_system_context[SYSTEM_CONTEXT.RETRIES], self.max_retries))
            self._stop_retries(obj)

    def _dispatch_and_retry(self, event, obj):
        """
        Dispatch an event to the state machine via Context._dispatch. In the event of
        exceptions, saves off all the required retry entities via Context._retry. If
        errors occur, records the errors via Context._send_queued_errors.

        :param event: a str event.
        :param obj: a dict.
        """
        try:
            # normal, happy path
            self._dispatch(event, obj)

        except Exception:
            logger.exception('Error occurred during FSM.dispatch().')

            # not-normal, un-happy path
            self._retry(obj)

        try:
            # errors worth tracking occur in both the happy and un-happy paths
            if self._errors:
                self._send_queued_errors()

        except Exception:
            logger.exception("Error while sending errors.")

    def dispatch(self, event, obj):
        """
        Acquires an exclusive lease for the machine's correlation_id, and executes
        all the machinery of the framework and all the user code.

        In Marting Kleppmann's blog entry "How to do distributed locking"
        (http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) he points out
        that the following code is broken (in a distributed system):

            // THIS CODE IS BROKEN
            function writeData(filename, data) {
                var lock = lockService.acquireLock(filename);
                if (!lock) {
                    throw 'Failed to acquire lock';
                }

                // GARBAGE COLLECTION

                try {
                    var file = storage.readFile(filename);
                    var updated = updateContents(file, data);
                    storage.writeFile(filename, updated);
                } finally {
                    lock.release();
                }
            }

        It is broken under the following circumstances:

            time 0 - lease acquired by process 1
            time 1 - huge garbage collection pause in process 1 at "// GARBAGE COLLECTION"
            time 2 - lease expires
            time 3 - lease acquired by process 2
            time 4 - writeData executed in process 2
            time 5 - garbage collection done in process 1
            time 6 - writeData loses race in process 1

        In order to fix it, a monotonically increasing "fence token" must be supplied by
        the locking/leasing system and the storage system must be able to use the fence token
        to detect late writes.

        The framework makes the current fence token available in obj[OBJ.FENCE_TOKEN] so
        that developers writing Action code can implement the above advice.

        :param event: a str event.
        :param obj: a dict.
        """
        fence_token = None

        try:
            # attempt to acquire the lease and execute the state transition
            fence_token = acquire_lease(self.correlation_id, self.steps, self.retries,
                                        primary=self.lease_primary)

            # 0 indicates system error, False indicates lease acquisition failure
            #
            # >>> 0 == False
            # True
            # >>> 0 is 0
            # True
            # >>> 0 is 0L
            # False
            # >>>
            #
            if fence_token is 0 or fence_token is 0L:
                self._queue_error(ERRORS.CACHE, 'System error acquiring primary=%s lease.' % self.lease_primary)
                self.lease_primary = not self.lease_primary
                fence_token = acquire_lease(self.correlation_id, self.steps, self.retries,
                                            primary=self.lease_primary)

            if not fence_token:
                # could not get the lease. something is going wrong
                self._queue_error(ERRORS.CACHE, 'Could not acquire lease. Retrying.')
                self._retry(obj)
            else:
                # lease acquired, execute the state transition

                # NOTE: idempotency
                #
                # In the happy path, each state transition is associated with a
                # correlation_id and a monotonically increasing step value. So,
                # a typical machine executes with the following values for
                # step, retry count, and fence token
                #
                # | correlation_id | steps | retries | fence |
                # +----------------+-------+---------+-------+
                # | abc123         | 0     | 0       | 1     |
                # | abc123         | 1     | 0       | 2     |
                # | abc123         | 2     | 0       | 3     |
                # | abc123         | 3     | 0       | 4     |
                #
                # In the event of system or user-code error, the framework may
                # retry a given step multiple times
                #
                # | correlation_id | steps | retries | fence |
                # +----------------+-------+---------+-------+
                # | abc123         | 0     | 0       | 1     |
                # | abc123         | 1     | 0       | 2     |
                # | abc123         | 1     | 1       | 3     | # retry
                # | abc123         | 1     | 2       | 4     | # retry
                # | abc123         | 2     | 0       | 5     |
                # | abc123         | 3     | 0       | 6     |
                #
                # so user-code should ensure it is idempotent (details below).
                #
                # The most interesting case arises when the AWS Lambda function
                # is re-executed with a duplicate message, poentially out-of-order.
                # This can happen for any number of reasons, since AWS Lambda ensure
                # at-least-once delivery of messages. In this case, one may see messages
                # like the following
                #
                # | correlation_id | steps | retries | fence |
                # +----------------+-------+---------+-------+
                # | abc123         | 0     | 0       | 1     |
                # | abc123         | 1     | 0       | 2     |
                # | abc123         | 1     | 2       | 3     | # out-of-order retry
                # | abc123         | 1     | 1       | 4     | # out-of-order retry
                # | abc123         | 1     | 2       | 5     | # duplicate message
                # | abc123         | 2     | 0       | 6     |
                # | abc123         | 3     | 0       | 7     |
                #
                # User code can use (correlation_id, steps) as an idempotency token
                # for any resource unique to a specific state machine, and
                # (correlation_id, steps, fence) as an idempotency token for any
                # global resource.
                #
                # In the latter case, the global storage/resource system needs to
                # understand fence tokens.

                # make the fence token available
                if isinstance(fence_token, (int, long)):
                    obj[OBJ.FENCE_TOKEN] = fence_token

                self._dispatch_and_retry(event, obj)

        finally:
            released = release_lease(self.correlation_id, self.steps, self.retries, fence_token,
                                     primary=self.lease_primary)
            if not released:
                self._queue_error(ERRORS.CACHE, 'Could not release lease.')

    def initialize(self):
        """
        Dispatches a "pseudo-init" event to start the machine.
        """
        self.dispatch(self.name, STATE.PSEUDO_INIT, None)

    ################################################################################
    # END: dispatch logic
    ################################################################################
