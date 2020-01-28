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

# library imports

# application imports
from aws_lambda_fsm.config import get_settings


settings = get_settings()


def json_dumps_additional_kwargs():
    return getattr(settings, 'JSON_DUMPS_ADDITIONAL_KWARGS', {}) or \
        {'sort_keys': True, 'default': lambda x: '<not_serializable>'}


def json_loads_additional_kwargs():
    return getattr(settings, 'JSON_LOADS_ADDITIONAL_KWARGS', {}) or {}
