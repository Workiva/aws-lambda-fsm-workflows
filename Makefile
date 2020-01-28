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

COVER_PACKAGE := `find aws_lambda_fsm -name "*.py" | grep -vE "vendor|tests|__init__|_pkg_meta|aws_lambda_fsm/utils.py" | sed s/[.]py// | sed s/[/]/./g | sed "s/aws_lambda_fsm/--cover-package aws_lambda_fsm/"`

clean:
	find . -name '*.pyc' -delete
	rm -f aws-lambda-fsm.zip
	rm -f .coverage
	ln -sf settings.py.example settings.py

check-config:
	if [ -e ~/.aws/config ]; then echo "~/.aws/config exists"; exit 1; fi

build: clean
	./tools/build_zip.sh

coverage: clean check-config
	echo ${COVER_PACKAGE}
	nosetests -a '!functional' --logging-level=ERROR --with-xunit --xunit-file=unit.xml --with-coverage ${COVER_PACKAGE} --cover-min-percentage=100

test: clean check-config
	echo ${COVER_PACKAGE}
	nosetests -a '!functional' --logging-level=ERROR --with-xunit --xunit-file=unit.xml

unit: test

functional: clean check-config
	echo ${COVER_PACKAGE}
	nosetests -a 'functional' --logging-level=ERROR --with-xunit --xunit-file=unit.xml

flake8:
	flake8 --max-line-length=120 --ignore=E000,E402 --exclude=settingslocal.py .

all: test flake8 coverage

