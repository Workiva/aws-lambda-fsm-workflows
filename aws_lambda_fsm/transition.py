# system imports
import logging

# library imports

# application imports

logger = logging.getLogger(__name__)


class Transition(object):
    """
    State machine transition.
    """

    def __init__(self, name, target, action=None):
        """
        Construct a state machine transition.

        :param name: a str name for the transition.
        :param target: an aws_lambda_fsm.state.State instance corresponding to the destination state of this transition.
        :param action: an optional aws_lambda_fsm.action.Action instance.
        """
        self.name = name
        self.target = target
        self.action = action

    def execute(self, context, obj):
        """
        Execute the state machine transition.

        :param context: an aws_lambda_fsm.fsm.Context instance.
        :param obj: a dict.
        """
        if self.action:
            self.action.execute(context, obj)
        context.current_state = self.target
