# system imports
import logging
logger = logging.getLogger(__name__)


class Action(object):
    """
    State machine action.
    """

    def __init__(self, name, event=None):
        """
        Construct a state machine action.

        :param name: a str name for the action.
        :param event: an optional default str event to return.
        """
        self.name = name
        self.event = event

    def execute(self, context, obj):
        """
        Execute the state machine action. This class is typically sub-classed
        to provide useful functionality.

        :param context: an aws_lambda_fsm.fsm.Context instance.
        :param obj: a dict.
        :return: a str event.
        """
        logger.info('action.name=%s', self.name)
        return self.event
