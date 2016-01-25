#!/bin/bash -x
echo Y | pycleaner
zip -r aws-lambda-fsm.zip aws_lambda_fsm examples settings.py main.py fsm.yaml settingslocal.py
a=`pwd`
s=`echo "import yaml; print yaml.__file__.split('yaml')[0]" | python`
cd ${s}
echo Y | pycleaner
zip -r ${a}/aws-lambda-fsm.zip yaml
