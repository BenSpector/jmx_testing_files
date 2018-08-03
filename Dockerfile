# This Dockerfile will build an image that is configured
# to run the node-based program that collects MBean data
# via JMX.

FROM ubuntu:14.04

# Disable prompts from apt.
ENV DEBIAN_FRONTEND noninteractive

# Install prerequisites.
RUN apt-get update && \
    apt-get install -y -q curl make g++ && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install Node.js
RUN apt-get update && apt-get install -y curl && apt-get install -y wget
RUN \
  cd /tmp && \
  wget http://nodejs.org/dist/v7.10.1/node-v7.10.1-linux-x64.tar.gz && \
  tar -C /usr/local --strip-components 1 -xzf node-v7.10.1-linux-x64.tar.gz && \
  /usr/local/bin/node -v && \
  /usr/local/bin/npm -v

#Install Python 2.7 for node-gyp
RUN apt-get update && \
    apt-get upgrade && \
    sh -c 'bin/echo -e "yes" | apt install python2.7 python-pip'

RUN export PYTHONPATH=$PYTHONPATH:/usr/local/bin/python2.7
RUN export PATH=$PATH:/usr/local/bin/python

#Install Java for node-java
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:openjdk-r/ppa && \
	apt-get update && \
	sh -c 'bin/echo -e "yes" | apt-get install openjdk-8-jdk' && \
	sh -c 'bin/echo -e "yes" |apt-get install openjdk-8-source' && \
	export JAVA_HOME=/usr/lib/jvm/java-8-openjdk && \
	export PATH=$PATH:$JAVA_HOME/bin

COPY index.js /index.js
COPY package.json /package.json

RUN npm install -g gyp
RUN npm install

CMD ["node", "/index.js"]
