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
from threading import RLock
import logging

# library imports
import yaml

# application imports

_config_lock = RLock()
_config = {}

_settings_lock = RLock()
_settings = None

logger = logging.getLogger(__name__)


def get_settings():
    """
    Returns a settings object or module that supplies runtime configuration
    via the typical "settings.UPPER_CASE_PARAM" style. The default is to
    simply return the settings.py module, but allowing injection may allow
    greater integration flexibility in some environments.

    :return: a settings object or module
    """
    with _settings_lock:
        global _settings
        if not _settings:
            import settings
            _settings = settings
        return _settings


def set_settings(settings):
    """
    Sets the settings object or module.

    :param settings: a settings object or module
    """
    with _settings_lock:
        global _settings
        _settings = settings


def get_current_configuration(filename='fsm.yaml'):
    """
    Returns the current fsm configuration dictionary, taking care to cache for performance.

    TODO: schema
    TODO: validation utilities

    :return: a dict.
    """
    with _config_lock:
        global _config
        if filename not in _config:
            _config[filename] = load_config_from_yaml(filename=filename)
        return _config[filename]


def load_config_from_yaml(filename='fsm.yaml'):
    """
    Returns the current fsm configuration dictionary, loaded from file.

    :return: a dict.
    """
    yaml_file = open(filename, 'r')
    yaml_dict = yaml.load(yaml_file.read())
    return yaml_dict
