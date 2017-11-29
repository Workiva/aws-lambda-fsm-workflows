FROM ubuntu:trusty

# apt stuff
RUN apt-get update
RUN apt-get -y install python-pip nodejs npm software-properties-common python-software-properties supervisor memcached
RUN apt-add-repository ppa:brightbox/ruby-ng
RUN apt-get update
RUN apt-get -y install ruby2.4

# aws fakes
RUN gem install fake_sqs -v 0.4.3
RUN npm install -g kinesalite@1.11.6
RUN ln -s `which nodejs` /usr/bin/node

COPY requirements.txt requirements.txt
COPY requirements_dev.txt requirements_dev.txt
RUN pip install -Ur requirements_dev.txt

# copy in the library
COPY aws_lambda_fsm aws_lambda_fsm
COPY tools tools
COPY examples examples
COPY setup.py setup.py
COPY fsm.yaml fsm.yaml

COPY tests/docker/settings.py settings.py
COPY tests/docker/supervisor.conf /etc/supervisor/conf.d/supervisor.conf

ENV AWS_ACCESS_KEY_ID=abc123
ENV AWS_SECRET_ACCESS_KEY=abc123

# use supervisor
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisor.conf", "-l", "/dev/null", "-y", "0"]

