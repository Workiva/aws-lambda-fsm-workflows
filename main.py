import logging
import sys
import os

# flake8: noqa

ROOT_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
VENDOR_PATH = os.path.join(ROOT_PATH, 'lib')
if VENDOR_PATH not in sys.path:
    sys.path.insert(0, VENDOR_PATH)

logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger().setLevel(0)

from aws_lambda_fsm.handler import lambda_handler
