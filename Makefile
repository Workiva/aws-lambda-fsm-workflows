COVER_PACKAGE := `find aws_lambda_fsm -name "*.py" | grep -vE "vendor|tests|__init__|_pkg_meta" | sed s/[.]py// | sed s/[/]/./g | sed "s/aws_lambda_fsm/--cover-package aws_lambda_fsm/"`

clean:
	echo Y | pycleaner
	rm -f aws-lambda-fsm.zip
	rm -f .coverage

check-config:
	if [ -e ~/.aws/config ]; then echo "~/.aws/config exists"; exit 1; fi

build: clean
	./tools/build_zip.sh

coverage: clean check-config
	echo ${COVER_PACKAGE}
	nosetests --logging-level=ERROR --with-xunit --xunit-file=unit.xml --with-coverage ${COVER_PACKAGE} --cover-min-percentage=100

test: clean check-config
	echo ${COVER_PACKAGE}
	nosetests --logging-level=ERROR --with-xunit --xunit-file=unit.xml

flake8:
	flake8 --max-line-length=120 .

