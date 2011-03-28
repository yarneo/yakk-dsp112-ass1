#!/bin/bash
date > /tmp/worker_node.txt
DEBIAN_FRONTEND=noninteractive apt-get -y update
DEBIAN_FRONTEND=noninteractive apt-get -y install default-jdk
date >> /tmp/worker_node.txt
cd ~ubuntu
sudo -u ubuntu wget -c http://dl.dropbox.com/u/1565752/DSP112/a1/worker_logging.properties
sudo -u ubuntu wget -c http://dl.dropbox.com/u/1565752/DSP112/a1/worker.jar
sudo -u ubuntu echo accessKey=REPLACED_WITH_ACCESS_KEY >> AwsCredentials.properties
sudo -u ubuntu echo secretKey=REPLACED_WITH_SECRET_KEY >> AwsCredentials.properties
sudo -u ubuntu jar uf worker.jar AwsCredentials.properties
sudo -u ubuntu java -Djava.util.logging.config.file=worker_logging.properties -jar worker.jar > worker.log 2> worker.log
