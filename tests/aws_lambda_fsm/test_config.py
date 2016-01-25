# system imports
import unittest

# library imports

# application imports
from aws_lambda_fsm.config import load_config_from_yaml
from aws_lambda_fsm.config import get_current_configuration
from aws_lambda_fsm.config import get_settings
from aws_lambda_fsm.config import set_settings
import aws_lambda_fsm.config
from tests.aws_lambda_fsm import TestSettings


class TestAction(unittest.TestCase):

    def test_load_config_from_yaml(self):
        load_config_from_yaml()

    def test_get_current_configuration(self):
        aws_lambda_fsm.config._config = {}
        get_current_configuration()

    def test_get_settings(self):
        s = get_settings()
        self.assertEqual(TestSettings, s)

    def test_get_settings_imports_if_missing(self):
        set_settings(None)
        get_settings()

    def test_set_settings(self):
        set_settings('foo')
        s = get_settings()
        self.assertEqual('foo', s)
