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
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

# application imports
from aws_lambda_fsm.action import Action


class CheckIfFileExists(Action):
    """
    Checks if the file exists.
    """

    def execute(self, context, obj):
        connection = S3Connection()
        bucket = Bucket(connection=connection, name=context['bucket'])
        key = Key(bucket=bucket, name=context['name'])
        if key.exists():
            return 'done'
        else:
            return 'missing'


class EncryptFile(Action):
    """
    Downloads, Encrpyts (with a no-op) and Uploads a file.
    """

    def execute(self, context, obj):
        connection = S3Connection()
        bucket = Bucket(connection=connection, name=context['bucket'])
        key1 = Key(bucket=bucket, name=context['name'])
        key2 = Key(bucket=bucket, name=context['name'] + '.encrypted')
        key2.set_contents_from_string(key1.get_contents_as_string())
        return 'done'


class RemoveOldFile(Action):
    """
    Removes the unencrypted file.
    """

    def execute(self, context, obj):
        connection = S3Connection()
        bucket = Bucket(connection=connection, name=context['bucket'])
        key = Key(bucket=bucket, name=context['name'])
        key.delete()
        return 'done'
