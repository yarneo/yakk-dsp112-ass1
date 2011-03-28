#!/bin/bash
date > /tmp/manager_node.txt
DEBIAN_FRONTEND=noninteractive apt-get -y update
DEBIAN_FRONTEND=noninteractive apt-get -y install default-jdk
date >> /tmp/manager_node.txt
cd ~ubuntu
sudo -u ubuntu wget -c http://dl.dropbox.com/u/1565752/DSP112/a1/manager_logging.properties
sudo -u ubuntu wget -c http://dl.dropbox.com/u/1565752/DSP112/a1/manager.jar
sudo -u ubuntu echo accessKey=REPLACED_WITH_ACCESS_KEY >> AwsCredentials.properties
sudo -u ubuntu echo secretKey=REPLACED_WITH_SECRET_KEY >> AwsCredentials.properties
sudo -u ubuntu jar uf manager.jar AwsCredentials.properties
sudo -u ubuntu java -Djava.util.logging.config.file=manager_logging.properties  -jar manager.jar > manager.log 2> manager.log
