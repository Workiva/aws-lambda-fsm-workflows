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

from setuptools import setup, find_packages


def get_version():
    import imp

    pkg_meta = imp.load_source('_pkg_meta', 'aws_lambda_fsm/_pkg_meta.py')

    return pkg_meta.version


# "from pip.req import parse_requirements" no longer works in pip 10
# so this function is just a simple version that extracts the requirements
# using a loop and naive filtering.
def get_requirements(filename):
    lines = list(val.strip() for val in open(filename))
    filtered = []
    for line in lines:
        if line.startswith('-r '):
            continue
        if '#' in line:
            line = line.split('#')[0]
        line = line.strip()
        if not line:
            continue
        filtered.append(line)
    return filtered


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
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    scripts=["tools/experimental/fsm_docker_runner.py",
             "tools/dev_lambda.py",
             "tools/experimental/dev_ecs.py",
             "tools/create_resources.py",
             "tools/create_kinesis_stream.py",
             "tools/create_dynamodb_table.py",
             "tools/create_sns_topic.py",
             "tools/create_sqs_queue.py",
             "tools/start_state_machine.py",
             "tools/yaml_to_graphviz.py"],
    url='https://workiva.github.io/aws-lambda-fsm-workflows/',
    license="http://www.apache.org/licenses/LICENSE-2.0",
    description="AWS FSMs on Lambda/Kinesis",
    long_description="A Python framework for developing finite state machine-based workflows on "
                     "AWS Lambda.",
    python_requires=">=2.7, >=3.6",
    install_requires=get_requirements('requirements.txt'),
    tests_require=get_requirements('requirements_dev.txt') + get_requirements('requirements.txt'),
)


if __name__ == '__main__':
    setup(**setup_args)
