# system imports
import unittest

# library imports

# application imports
from aws_lambda_fsm.action import Action


class TestAction(unittest.TestCase):

    def test_execute(self):
        action = Action('name', event='foo')
        event = action.execute('', 'event')
        self.assertEqual('foo', event)
