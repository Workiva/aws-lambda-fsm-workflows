#!/usr/bin/env python

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

#
# dev_ecs.py
#
# system imports
import http.server
import socketserver
import argparse
import json
import subprocess
import os
from future import standard_library
standard_library.install_aliases()

# library imports

# application imports
from aws_lambda_fsm.serialization import json_loads_additional_kwargs

# setup the command line args
parser = argparse.ArgumentParser(description='Mock AWS ECS service.')
parser.add_argument('--port', type=int, default=8888)
parser.add_argument('--image')
args = parser.parse_args()

# {
#     "cluster": "default",
#     "overrides": {
#         "containerOverrides": [
#             {
#                 "environment": [
#                     {"name": "FSM_CONTEXT", "value": "ey..."},
#                     {"name": "AWS_ACCESS_KEY_ID", "value": "AS..."},
#                     {"name": "AWS_SECRET_ACCESS_KEY", "value": "HP..."},
#                     {"name": "AWS_SESSION_TOKEN", "value": "FQo..."},
#                     {"name": "AWS_DEFAULT_REGION", "value": "us-east-1"}],
#                 "name": "rusaw"}
#         ]},
#     "taskDefinition": "rusaw:5"
# }


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):

        length = int(self.headers['content-length'])
        data = json.loads(self.rfile.read(length), **json_loads_additional_kwargs())

        subprocess_args = ['docker', 'run', '-v', '/var/run/docker.sock:/var/run/docker.sock']
        if 'VOLUME' in os.environ:
            subprocess_args.extend(['-v', os.environ['VOLUME']])
        if 'LINK' in os.environ:
            subprocess_args.extend(['--link=' + os.environ['LINK']])
        if 'NETWORK' in os.environ:
            subprocess_args.extend(['--network=' + os.environ['NETWORK']])
        co = data.get('overrides', {}).get('containerOverrides', [])
        environ = {}
        if co:
            for env in co[0].get('environment', []):
                environ[env['name']] = env['value']
                subprocess_args.extend(['-e', '%(name)s=%(value)s' % env])
        subprocess_args.extend(['-e', 'PYTHON_BIN=%s' % os.environ['PYTHON_BIN']])
        subprocess_args.append(args.image)
        subprocess.call(subprocess_args)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write('{}')

httpd = socketserver.TCPServer(("", args.port), Handler)
httpd.serve_forever()
