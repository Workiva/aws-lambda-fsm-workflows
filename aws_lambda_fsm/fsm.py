# system imports
import json
import importlib
from threading import RLock
import uuid
import logging
import time

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
from aws_lambda_fsm.aws import cache_add
from aws_lambda_fsm.aws import cache_get
from aws_lambda_fsm.aws import cache_delete
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
        return Context(machine_name,
                       initial_system_context=initial_system_context,
                       initial_user_context=initial_user_context,
                       initial_state=initial_state)


def _run_once(f):
    """
    Decorator that uses a cache with .get()/.set() to flag an action as
    already executed, and check to ensure that it hasn't been executed
    before running (in the event of retries, multiple failures).

    :param f: a function/method to run only once.
    """
    def inner(self, *args, **kwargs):

        # abort if these message has already been processed and another event message
        # has already been emitted to drive the state machine forward
        if get_message_dispatched(self.correlation_id,
                                  self.steps):
            self._queue_error(ERRORS.DUPLICATE, 'Message has been processed already.')
            return

        f(self, *args, **kwargs)

        # once the message is emitted, we want to make sure the current event is never sent again.
        # the approach here is to simply use a cache to set a key like "correlation_id-steps-retries"
        dispatched = set_message_dispatched(
            self.correlation_id,
            self.steps
        )
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
                 max_retries=500):
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

        self.max_retries = max_retries
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

    def _start_retries(self, retry_data, obj):
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
                return start_retries(
                    self,
                    time.time() + retry_system_context[SYSTEM_CONTEXT.RETRIES] * 1.0,
                    serialized,
                    primary=primary
                )
            except ClientError:
                # log an error to at least expose the error
                self._queue_error(ERRORS.ERROR, 'Unable to save last payload for retry.', exc_info=True)

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
                    self._queue_error(ERRORS.ERROR, 'Unable to terminate retries.', exc_info=True)

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
                    self._queue_error(ERRORS.ERROR, 'Unable to save last sent data.', exc_info=True)

    def _send_next_event_for_dispatch(self, serialized, obj):
        """
        Send the next event for dispatch to the primary/secondary stream systems.

        :param serialized: a str serialized message.
        :param obj: a dict.
        """
        for primary in [True, False]:
            try:
                return send_next_event_for_dispatch(
                    self,
                    serialized,
                    self.correlation_id,
                    delay=obj.get(OBJ.DELAY, 0),
                    primary=primary
                )
            except ClientError:
                self._queue_error(ERRORS.ERROR, 'Unable to send next event.', exc_info=True)
                if not primary:
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

    @_run_once
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
        logger.exception('Error occurred during FSM.dispatch().')

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
            self._queue_error(ERRORS.RETRY, 'More retries allowed. Retrying.')
            self._start_retries(retry_data, obj)

        # if there are no more retries available, simply log an error, then delete
        # the retry entity from dynamodb. it will take human intervention to recover things
        # at this point.
        else:
            self._queue_error(ERRORS.FATAL, 'No more retries allowed. Terminating.')
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
        Acquires an exclusive lock for the machine's correlation_id, and executes
        all the machinery of the framework and all the user code. If the lock cannot
        be acquired (it is non-blocking), then and error is logged/recorded.

        :param event: a str event.
        :param obj: a dict.
        """

        # create a guid to add to memcache
        guid = uuid.uuid4().hex
        added = False

        try:
            # successful atomic add means the lock has been acquired
            added = cache_add(self.correlation_id, guid)
            if added is False:
                # could not get the lock. something is going very wrong with this
                # state machine.
                self._queue_error(ERRORS.CACHE, 'Could not obtain exclusive lock.')

            # we can actually detect connectivity errors if a 0 is returned
            elif added == 0:
                self._queue_error(ERRORS.CACHE, 'Executing without exclusive lock.')

            # execute the primary logic
            self._dispatch_and_retry(event, obj)

        finally:
            # if nothing was added, there is nothing to delete
            if added:
                # grab the current value of the lock in order to see if we
                # still own the lock. eg. memcache ejection could cause this value
                # to differ from the value we added earlier
                guid_in_cache = cache_get(self.correlation_id)
                if guid_in_cache == guid:
                    cache_delete(self.correlation_id)
                else:
                    self._queue_error(ERRORS.CACHE, 'No longer own exclusive lock.')

    def initialize(self):
        """
        Dispatches a "pseudo-init" event to start the machine.
        """
        self.dispatch(self.name, STATE.PSEUDO_INIT, None)

    ################################################################################
    # END: dispatch logic
    ################################################################################
