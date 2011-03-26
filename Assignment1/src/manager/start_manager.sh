#!/bin/bash
date > /tmp/manager_node.txt
DEBIAN_FRONTEND=noninteractive apt-get -y update
DEBIAN_FRONTEND=noninteractive apt-get -y install subversion ant ivy default-jdk
date >> /tmp/manager_node.txt
cd ~ubuntu
sudo -u ubuntu svn export http://yakk-dsp112-ass1.googlecode.com/svn/trunk/ assignment1
cd assignment1
echo accessKey=REPLACED_WITH_ACCESS_KEY >> src/AwsCredentials.properties
echo secretKey=REPLACED_WITH_SECRET_KEY >> src/AwsCredentials.properties
sudo -u ubuntu ant -lib /usr/share/java run_manager > ant_log.txt 2> ant_log.txt
cd /

