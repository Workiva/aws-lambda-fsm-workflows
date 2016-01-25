# system imports
import logging

# library imports

# application imports
from aws_lambda_fsm.action import Action
from aws_lambda_fsm.utils import ECSTaskEntryAction


class RunTaskAction(ECSTaskEntryAction):
    """
    Starts a docker task in AWS EC2 Container Service
    """
    pass


class Fireworks(Action):
    """
    Starts a docker task in AWS EC2 Container Service
    """

    def execute(self, context, obj):
        logging.info('*' * 80)
        return 'done'
