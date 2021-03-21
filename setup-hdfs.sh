#!/bin/bash
HADOOP_VERSION=2.10.1

sudo wget https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -O hadoop-$HADOOP_VERSION.tar.gz
tar -zxvf hadoop-$HADOOP_VERSION.tar.gz
rm -f hadoop-$HADOOP_VERSION.tar.gz

cp conf/hadoop/* hadoop-$HADOOP_VERSION/etc/hadoop/
