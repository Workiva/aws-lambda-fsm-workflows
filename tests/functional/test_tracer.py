# Copyright 2016 Workiva Inc.
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
import mock

# library imports

# application imports
from aws_lambda_fsm.fsm import FSM
from aws_lambda_fsm.config import get_current_configuration
from tests.functional.utils import AWSStub
from tests.functional.utils import BaseFunctionalTest

AWS = AWSStub()


@mock.patch("aws_lambda_fsm.client.send_next_event_for_dispatch", wraps=AWS.send_next_event_for_dispatch)
@mock.patch("aws_lambda_fsm.fsm.send_next_event_for_dispatch", wraps=AWS.send_next_event_for_dispatch)
@mock.patch("aws_lambda_fsm.fsm.set_message_dispatched", wraps=AWS.set_message_dispatched)
@mock.patch("aws_lambda_fsm.fsm.get_message_dispatched", wraps=AWS.get_message_dispatched)
@mock.patch("aws_lambda_fsm.fsm.acquire_lease", wraps=AWS.acquire_lease)
@mock.patch("aws_lambda_fsm.fsm.release_lease", wraps=AWS.release_lease)
@mock.patch("aws_lambda_fsm.fsm.start_retries", wraps=AWS.start_retries)
@mock.patch("aws_lambda_fsm.fsm.increment_error_counters", wraps=AWS.increment_error_counters)
@mock.patch("aws_lambda_fsm.fsm.store_checkpoint", wraps=AWS.store_checkpoint)
class Test(BaseFunctionalTest):

    def setUp(self):
        AWS.reset()
        FSM(get_current_configuration('./fsm.yaml'))

    ################################################################################
    # START: machine_name="tracer"
    ################################################################################

    def test_tracer_primary_stream_down(self, *args):
        self._execute(AWS, "tracer", {}, primary_stream_chaos=1.0)

        # check answer
        self.assertEqual(101, AWS.all_sources.trace(('count',))[-1][-1][-1])

        # check all the secondary message streams processed data
        self.assertTrue(len(AWS.primary_stream_source.all_messages) == 1)
        self.assertTrue(len(AWS.secondary_stream_source.all_messages) > 1)
        self.assertTrue(len(AWS.primary_retry_source.all_messages) > 1)
        self.assertTrue(len(AWS.secondary_retry_source.all_messages) == 0)

    def test_tracer_primary_retry_down(self, *args):
        self._execute(AWS, "tracer", {}, primary_retry_chaos=1.0)

        # check answer
        self.assertEqual(101, AWS.all_sources.trace(('count',))[-1][-1][-1])

        # check all the secondary message streams processed data
        self.assertTrue(len(AWS.primary_stream_source.all_messages) > 1)
        self.assertTrue(len(AWS.secondary_stream_source.all_messages) == 0)
        self.assertTrue(len(AWS.primary_retry_source.all_messages) == 0)
        self.assertTrue(len(AWS.secondary_retry_source.all_messages) > 1)

    def test_tracer_secondary_stream_down(self, *args):
        self._execute(AWS, "tracer", {}, secondary_stream_chaos=1.0)

        # check answer
        self.assertEqual(101, AWS.all_sources.trace(('count',))[-1][-1][-1])

        # check the primary message streams processed data
        self.assertTrue(len(AWS.primary_stream_source.all_messages) > 1)
        self.assertTrue(len(AWS.secondary_stream_source.all_messages) == 0)
        self.assertTrue(len(AWS.primary_retry_source.all_messages) > 1)
        self.assertTrue(len(AWS.secondary_retry_source.all_messages) == 0)

    def test_tracer_secondary_retry_down(self, *args):
        self._execute(AWS, "tracer", {}, secondary_retry_chaos=1.0)

        # check answer
        self.assertEqual(101, AWS.all_sources.trace(('count',))[-1][-1][-1])

        # check all the secondary message streams processed data
        self.assertTrue(len(AWS.primary_stream_source.all_messages) > 1)
        self.assertTrue(len(AWS.secondary_stream_source.all_messages) == 0)
        self.assertTrue(len(AWS.primary_retry_source.all_messages) > 1)
        self.assertTrue(len(AWS.secondary_retry_source.all_messages) == 0)

    def test_tracer_total_stream_downage(self, *args):
        self._execute(AWS, "tracer", {}, primary_stream_chaos=1.0, secondary_stream_chaos=1.0)

        # check no streams processed data
        self.assertTrue(len(AWS.primary_stream_source.all_messages) == 1)
        self.assertTrue(len(AWS.secondary_stream_source.all_messages) == 0)
        self.assertTrue(len(AWS.primary_retry_source.all_messages) > 1)
        self.assertTrue(len(AWS.secondary_retry_source.all_messages) == 0)

    def test_tracer_total_retry_downage(self, *args):
        self._execute(AWS, "tracer", {}, primary_retry_chaos=1.0, secondary_retry_chaos=1.0)

        # check no streams processed data
        self.assertTrue(len(AWS.primary_stream_source.all_messages) >= 1)
        self.assertTrue(len(AWS.secondary_stream_source.all_messages) == 0)
        self.assertTrue(len(AWS.primary_retry_source.all_messages) == 0)
        self.assertTrue(len(AWS.secondary_retry_source.all_messages) == 0)
