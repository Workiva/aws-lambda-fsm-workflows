# Copyright 2016-2017 Workiva Inc.
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

from setuptools import setup, find_packages
from pip.req import parse_requirements


def get_version():
    import imp

    pkg_meta = imp.load_source('_pkg_meta', 'aws_lambda_fsm/_pkg_meta.py')

    return pkg_meta.version


def get_requirements(filename):
    try:
        from pip.download import PipSession

        session = PipSession()
    except ImportError:
        session = None

    reqs = parse_requirements(filename, session=session)

    return [str(r.req) for r in reqs]


def get_packages():
    return find_packages(exclude=["tests.*", "tests", "examples", "examples.*"])


def read_file(filename, mode='rb'):
    with open(filename, mode) as fp:
        return fp.read()

setup_args = dict(
    name="aws-lambda-fsm",
    version=get_version(),
    author="Shawn Rusaw",
    author_email="shawn.rusaw@workiva.com",
    packages=get_packages(),
    scripts=["tools/fsm_docker_runner.py",
             "tools/fsm_sqs_to_arn.py",
             "tools/dev_lambda.py",
             "tools/dev_ecs.py",
             "tools/create_resources.py",
             "tools/create_kinesis_stream.py",
             "tools/create_dynamodb_table.py",
             "tools/create_sns_topic.py",
             "tools/create_sqs_queue.py",
             "tools/start_state_machine.py",
             "tools/yaml_to_graphviz.py"],
    url='http://github.com/Workiva/aws-lambda-fsm-workflows',
    license="http://www.apache.org/licenses/LICENSE-2.0",
    description="AWS FSMs on Lambda/Kinesis",
    long_description="",
    install_requires=get_requirements('requirements.txt'),
    tests_require=get_requirements('requirements_dev.txt'),
)


if __name__ == '__main__':
    setup(**setup_args)
