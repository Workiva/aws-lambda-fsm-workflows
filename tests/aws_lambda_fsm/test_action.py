# system imports
import unittest

# library imports

# application imports
from aws_lambda_fsm.action import Action, max_retry_event
from aws_lambda_fsm.fsm import Context


class TestAction(unittest.TestCase):

    def test_execute(self):
        action = Action('name', event='foo')
        event = action.execute('', 'event')
        self.assertEqual('foo', event)


class TestMaxRetryEvent(unittest.TestCase):
    class MyAction(Action):
        @max_retry_event('fail')
        def execute(self, context, obj):
            raise RuntimeError('Test Exception.')

    def test_more_retries(self):
        context = Context('name', max_retries=1)
        context.retries = 0

        action = self.MyAction('name')

        with self.assertRaises(RuntimeError):
            action.execute(context, None)

    def test_max_retry(self):
        context = Context('name', max_retries=1)
        context.retries = 1

        action = self.MyAction('name')

        event = action.execute(context, None)

        self.assertEqual(event, 'fail')
