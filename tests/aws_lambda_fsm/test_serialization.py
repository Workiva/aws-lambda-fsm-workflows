# Copyright 2016-2020 Workiva Inc.
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
import unittest
import json

# library imports
import mock

# application imports
from aws_lambda_fsm.serialization import json_dumps_additional_kwargs
from aws_lambda_fsm.serialization import json_loads_additional_kwargs


class Encoder(json.JSONEncoder):

    def default(self, obj):
        if 'A' == obj:
            return 'B'
        return json.JSONEncoder.default(self, obj)

    def encode(self, obj):
        if 'A' == obj:
            return 'B'
        return json.JSONEncoder.encode(self, obj)


class Decoder(json.JSONDecoder):

    def decode(self, obj):
        if 'B' == obj:
            return 'A'
        return json.JSONDecoder.decode(self, obj)


class TestJsonSerialization(unittest.TestCase):

    # json.dumps tests

    @mock.patch('aws_lambda_fsm.serialization.settings')
    def test_custom_encoder(self, mock_settings):
        mock_settings.JSON_DUMPS_ADDITIONAL_KWARGS = {'cls': Encoder}
        self.assertEquals('B', json.dumps("A", **json_dumps_additional_kwargs()))

    def test_json_dumps_additional_kwargs_defaults(self):
        self.assertEquals({'sort_keys', 'default'}, set(json_dumps_additional_kwargs().keys()))
        self.assertEquals("<not_serializable>", json_dumps_additional_kwargs()['default']('~~~'))

    @mock.patch('aws_lambda_fsm.serialization.settings')
    def json_dumps_additional_kwargs_using_settings(self, mock_settings):
        mock_settings.JSON_DUMPS_ADDITIONAL_KWARGS = {'default': lambda x: "foobar"}
        self.assertEquals({'sort_keys', 'default'}, json_dumps_additional_kwargs().keys())
        self.assertEquals("foobar", json_dumps_additional_kwargs()['default']('~~~'))

    # json.loads tests

    @mock.patch('aws_lambda_fsm.serialization.settings')
    def test_custom_decoder(self, mock_settings):
        mock_settings.JSON_LOADS_ADDITIONAL_KWARGS = {'cls': Decoder}
        self.assertEquals('A', json.loads("B", **json_loads_additional_kwargs()))

    def test_json_loads_additional_kwargs_defaults(self):
        self.assertEquals({}, json_loads_additional_kwargs())

    @mock.patch('aws_lambda_fsm.serialization.settings')
    def test_json_loads_additional_kwargs_using_settings(self, mock_settings):
        mock_settings.JSON_LOADS_ADDITIONAL_KWARGS = {'default': lambda x: "foobar"}
        self.assertEquals({'default'}, set(json_loads_additional_kwargs().keys()))
        self.assertEquals("foobar", json_loads_additional_kwargs()['default']('~~~'))
