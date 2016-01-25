from setuptools import setup, find_packages
from pip.req import parse_requirements


def get_version():
    import imp

    source_dir = 'aws_lambda_fsm'

    with open('{}/_pkg_meta.py'.format(source_dir), 'rb') as fp:
        mod = imp.load_source('_pkg_meta', source_dir, fp)

    return mod.version


def get_requirements(filename):
    try:
        from pip.download import PipSession

        session = PipSession()
    except ImportError:
        session = None

    reqs = parse_requirements(filename, session=session)

    return [str(r.req) for r in reqs]


def get_packages():
    return find_packages(exclude=["tests.*", "tests", "examples"])


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
             "tools/create_kinesis_stream.py",
             "tools/create_dynamodb_table.py",
             "tools/create_sns_topic.py",
             "tools/start_state_machine.py",
             "tools/yaml_to_graphviz.py"],
    url='http://github.com/Workiva/aws-lambda-fsm',
    license="LICENSE",
    description="AWS FSMs on Lambda/Kinesis/DynamoDB",
    long_description="",
    install_requires=get_requirements('requirements.txt'),
    tests_require=get_requirements('requirements_dev.txt'),
)


if __name__ == '__main__':
    setup(**setup_args)
