#!/bin/bash
# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
set -o pipefail
set -o errtrace
set -o nounset
set -o errexit

LEIN=${LEIN:-lein}
MVN=${MVN:-mvn}
GIT=${GIT:-git}
MAKE=${MAKE:-make}

KAFKA_VERSION=${KAFKA_VERSION:-"0.11.0.0"}
REDIS_VERSION=${REDIS_VERSION:-"6.2.1"}
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.11"}
SCALA_SUB_VERSION=${SCALA_SUB_VERSION:-"12"}
#STORM_VERSION=${STORM_VERSION:-"1.0.0"}
FLINK_VERSION=${FLINK_VERSION:-"1.11.2"}
#SPARK_VERSION=${SPARK_VERSION:-"1.5.1"}
ZK_VERSION=${ZK_VERSION:-"3.4.10"}
HADOOP_VERSION=2.10.1

JAVA_VERSSION=8u261
# download java from 
# https://www.oracle.com/java/technologies/javase/javase8u211-later-archive-downloads.html#license-lightbox

BASE_DIR="~/yahoo-streaming-benchmark"
REDIS_DIR="redis-$REDIS_VERSION"
KAFKA_DIR="kafka_$SCALA_BIN_VERSION-$KAFKA_VERSION"
ZK_DIR="zookeeper-$ZK_VERSION"
FLINK_DIR="flink-$FLINK_VERSION"
HADOOP_DIR=hadoop-$HADOOP_VERSION
#SPARK_DIR="/opt/module/spark-$SPARK_VERSION-bin-hadoop2.6"

ZK_HOST="zk1"
ZK_PORT="2181"
ZK_CONNECTIONS="$ZK_HOST:$ZK_PORT"
KAFKA_HOST_PREFIX="kafka"
KAFKA_HOST_NUM=2
HADOOP_HOST="hadoop1"
YARN_HOST="hadoop4"
FLINK_HOST="flink1"
REDIS_HOST="redis1"


TOPIC=${TOPIC:-"ad-events"}
PARTITIONS=${PARTITIONS:-1}
LOAD=${LOAD:-4000}
CONF_FILE=./conf/benchmarkConf.yaml
SINGLELEVEL_CONF_FILE=./conf/singleLevelConf.yaml
FLINK_PARALLELISM=8
#test time in seconds
TEST_TIME=${TEST_TIME:-240}

pid_match() {
   local VAL=`ps -aef | grep "$1" | grep -v grep | awk '{print $2}'`
   echo $VAL
}

remote_operation() {
  local host="$1"
  shift
  local cmd="$@"
  ssh ec2-user@$host "cd $BASE_DIR; ./stream-bench.sh $cmd" &
}

start_if_needed() {
  local match="$1"
  shift
  local name="$1"
  shift
  local sleep_time="$1"
  shift
  local PID=`pid_match "$match"`

  if [[ "$PID" -ne "" ]];
  then
    echo "$name is already running..."
  else
    "$@" &
    sleep $sleep_time
  fi
}

stop_if_needed() {
  local match="$1"
  local name="$2"
  local PID=`pid_match "$match"`
  if [[ "$PID" -ne "" ]];
  then
    kill "$PID"
    sleep 1
    local CHECK_AGAIN=`pid_match "$match"`
    if [[ "$CHECK_AGAIN" -ne "" ]];
    then
      kill -9 "$CHECK_AGAIN"
    fi
  else
    echo "No $name instance found to stop"
  fi
}

fetch_untar_file() {
  local FILE="download-cache/$1"
  local URL=$2
  if [[ -e "$FILE" ]];
  then
    echo "Using cached File $FILE"
  else
	  mkdir -p download-cache/
    sudo wget -O "$FILE" "$URL"
  fi
  tar -xzvf "$FILE"
}

create_kafka_topic() {
    local count=`$KAFKA_DIR/bin/kafka-topics.sh --describe --zookeeper "$ZK_CONNECTIONS" --topic $TOPIC 2>/dev/null | grep -c $TOPIC`
    if [[ "$count" = "0" ]];
    then
        $KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper "$ZK_CONNECTIONS" --replication-factor 1 --partitions $PARTITIONS --topic $TOPIC
    else
        echo "Kafka topic $TOPIC already exists"
    fi
}

run() {
  OPERATION=$1
  if [ "SETUP" = "$OPERATION" ];
  then
#    $GIT clean -fd
#    echo 'kafka.brokers:' > $CONF_FILE
#    echo '    - "localhost"' >> $CONF_FILE
#    echo 'kafka.port: 9092' >> $CONF_FILE
#    echo 'kafka.topic: "'$TOPIC'"' >> $CONF_FILE
#    echo 'kafka.partitions: '$PARTITIONS >> $CONF_FILE
#    echo 'kafka.zookeeper.path: /' >> $CONF_FILE
#    echo >> $CONF_FILE
#    echo 'akka.zookeeper.path: /akkaQuery' >> $CONF_FILE
#    echo >> $CONF_FILE
#    echo 'zookeeper.servers:' >> $CONF_FILE
#    echo '    - "'$ZK_HOST'"' >> $CONF_FILE
#    echo 'zookeeper.port: '$ZK_PORT >> $CONF_FILE
#    echo >> $CONF_FILE
#    echo 'redis.host: "localhost"' >> $CONF_FILE
#    echo >> $CONF_FILE
#    echo 'process.hosts: 1' >> $CONF_FILE
#    echo 'process.cores: 4' >> $CONF_FILE
#    echo >> $CONF_FILE
#    echo >> $CONF_FILE
#    echo '#Flink Specific' >> $CONF_FILE
#    echo 'group.id: "flink_yahoo_benchmark"' >> $CONF_FILE
#    echo 'flink.checkpoint.interval: 60000' >> $CONF_FILE
#    echo 'add.result.sink: 1' >> $CONF_FILE
#    echo 'flink.highcard.checkpointURI: "file:///tmp/checkpoints"' >> $CONF_FILE
#    echo 'redis.threads: 20' >> $CONF_FILE
#    echo >> $CONF_FILE
#    echo '#EventGenerator' >> $CONF_FILE
#    echo 'use.local.event.generator: 1' >> $CONF_FILE
#    echo 'redis.flush: 1' >> $CONF_FILE
#    echo 'redis.db: 0' >> $CONF_FILE
#    echo 'load.target.hz: 10000000' >> $CONF_FILE
#    echo 'num.campaigns: 1000000' >> $CONF_FILE
	
    $MVN clean package -Dkafka.version="$KAFKA_VERSION" -Dflink.version="$FLINK_VERSION" -Dscala.binary.version="$SCALA_BIN_VERSION" -Dscala.version="$SCALA_BIN_VERSION.$SCALA_SUB_VERSION"

  elif [ "INSTALL" = "$OPERATION" ];
  then
    #Fetch ZooKeeper
    ZK_FILE="$ZK_DIR.tar.gz"
    fetch_untar_file "$ZK_FILE" "https://archive.apache.org/dist/zookeeper/$ZK_DIR/$ZK_FILE"

    #Fetch and build Redis
    REDIS_FILE="$REDIS_DIR.tar.gz"
    fetch_untar_file "$REDIS_FILE" "http://download.redis.io/releases/$REDIS_FILE"

    cd $REDIS_DIR
    $MAKE
    cd ..

    #Fetch Kafka
    KAFKA_FILE="$KAFKA_DIR.tgz"
    fetch_untar_file "$KAFKA_FILE" "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/$KAFKA_FILE"

    #Fetch Flink
    FLINK_FILE="$FLINK_DIR-bin-scala_${SCALA_BIN_VERSION}.tgz"
    fetch_untar_file "$FLINK_FILE" "https://archive.apache.org/dist/flink/flink-$FLINK_VERSION/$FLINK_FILE"

    run "CONFIG"

  elif [ "CONFIG" = "$OPERATION" ];
  then
    cp ~/yahoo-streaming-benchmark/conf/zookeeper/zoo.cfg zoo.cfg 
    cp conf/flink/* $FLINK_DIR/ -r
    cp conf/hadoop/* $HADOOP_DIR/etc/hadoop/
    echo "config copied"
#    cp conf/kafka/* $KAFKA_DIR/config/
  elif [ "START_ZK" = "$OPERATION" ];
  then
    start_if_needed zookeeper ZooKeeper 10 "$ZK_DIR/bin/zkServer.sh" start
  elif [ "STOP_ZK" = "$OPERATION" ];
  then
    $ZK_DIR/bin/zkServer.sh stop
    rm -rf /tmp/zookeeper
  elif [ "START_HDFS" = "$OPERATION" ];
  then
    $HADOOP_DIR/sbin/start-dfs.sh
  elif [ "STOP_HDFS" = "$OPERATION" ];
  then
    $HADOOP_DIR/sbin/stop-dfs.sh
  elif [ "START_YARN" = "$OPERATION" ];
  then
    $HADOOP_DIR/sbin/start-yarn.sh
  elif [ "STOP_YARN" = "$OPERATION" ];
  then
    $HADOOP_DIR/sbin/stop-yarn.sh
  elif [ "START_REDIS" = "$OPERATION" ];
  then
    start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server" --protected-mode no
    cd data
    $LEIN run -n --configPath ../conf/benchmarkConf.yaml
    cd ..
  elif [ "STOP_REDIS" = "$OPERATION" ];
  then
    stop_if_needed redis-server Redis
    rm -f dump.rdb
  elif [ "START_KAFKA" = "$OPERATION" ];
  then
    start_if_needed kafka\.Kafka Kafka 10 "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties"
    create_kafka_topic
  elif [ "STOP_KAFKA" = "$OPERATION" ];
  then
    $KAFKA_DIR/bin/kafka-server-stop.sh
    rm -rf /tmp/kafka-logs/
  elif [ "START_FLINK" = "$OPERATION" ];
  then
    start_if_needed org.apache.flink.runtime.jobmanager.JobManager Flink 1 $FLINK_DIR/bin/start-cluster.sh
  elif [ "STOP_FLINK" = "$OPERATION" ];
  then
    $FLINK_DIR/bin/stop-cluster.sh
  elif [ "CLEAR_LOGS" = "$OPERATION" ];
  then
    rm $FLINK_DIR/log/*
    rm $KAFKA_DIR/logs/*
    rm zookeeper.out
    rm -r $HADOOP_DIR/logs/*
  elif [ "CLEAR_CP" = "$OPERATION" ];
  then
    rm -r $FLINK_DIR/data/
    rm -r /dev/shm/flink/
  elif [ "START_LOAD" = "$OPERATION" ];
  then
    cd data
    start_if_needed leiningen.core.main "Load Generation" 1 $LEIN run -r -t $LOAD --configPath ../$CONF_FILE
    cd ..
  elif [ "STOP_LOAD" = "$OPERATION" ];
  then
    stop_if_needed leiningen.core.main "Load Generation"
    cd data
    $LEIN run -g --configPath ../$CONF_FILE || true
    cd ..
  elif [ "START_FLINK_PROCESSING" = "$OPERATION" ];
  then
    "$FLINK_DIR/bin/flink" run -p $FLINK_PARALLELISM -c flink.benchmark.AdvertisingTopologyFlinkWindows ./flink-benchmarks/target/flink-benchmarks-0.1.0.jar $CONF_FILE &
    sleep 3
  elif [ "START_FLINK_SINGLELEVEL" = "$OPERATION" ];
  then
    "$FLINK_DIR/bin/flink" run -c flink.benchmark.AdvertisingTopologyFlinkWindows ./flink-benchmarks/target/flink-benchmarks-0.1.0.jar $SINGLELEVEL_CONF_FILE &
    sleep 3
  elif [ "STOP_FLINK_PROCESSING" = "$OPERATION" ];
  then
    FLINK_ID=`"$FLINK_DIR/bin/flink" list | grep 'AdvertisingTopologyFlinkWindows' | awk '{print $4}'; true`
    if [ "$FLINK_ID" == "" ];
	  then
	  echo "Could not find streaming job to kill"
    else
      "$FLINK_DIR/bin/flink" cancel $FLINK_ID
      sleep 3
    fi
  elif [ "FLINK_DEBUG_SINGLELEVEL" = "$OPERATION" ];
  then
    run "START_ZK"
    run "START_REDIS"
    run "START_KAFKA"
    run "START_FLINK"
    run "START_FLINK_SINGLELEVEL"
    run "START_LOAD"
  elif [ "FLINK_DEBUG" = "$OPERATION" ];
  then
    run "START_ZK"
    run "START_REDIS"
    run "START_KAFKA"
    run "START_FLINK"
    run "START_FLINK_PROCESSING"
    run "START_LOAD"
  elif [ "FLINK_DEBUG_STOP" = "$OPERATION" ];
  then
    run "STOP_LOAD"
    run "STOP_FLINK_PROCESSING"
    run "STOP_FLINK"
    run "STOP_KAFKA"
    run "STOP_REDIS"
    run "STOP_ZK"
  elif [ "FLINK_TEST" = "$OPERATION" ];
  then
    run "START_ZK"
    run "START_REDIS"
    run "START_KAFKA"
    run "START_FLINK"
    run "START_FLINK_PROCESSING"
    run "START_LOAD"
    sleep $TEST_TIME
    run "STOP_LOAD"
    run "STOP_FLINK_PROCESSING"
    run "STOP_FLINK"
    run "STOP_KAFKA"
    run "STOP_REDIS"
    run "STOP_ZK"
  elif [ "CLUSTER_HDFS" = "$OPERATION" ];
  then
    remote_operation $HADOOP_HOST "START_HDFS"
    remote_operation $YARN_HOST "START_YARN"
  elif [ "CLUSTER_HDFS_STOP" = "$OPERATION" ];
  then
    remote_operation $HADOOP_HOST "STOP_HDFS"
    remote_operation $YARN_HOST "STOP_YARN"
  elif [ "CLUSTER_TEST" = "$OPERATION" ];
  then
    remote_operation $ZK_HOST "START_ZK"
    remote_operation $REDIS_HOST "START_REDIS"
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
      remote_operation $KAFKA_HOST_PREFIX$num "START_KAFKA"
    done
    remote_operation $FLINK_HOST "START_FLINK"
    remote_operation $FLINK_HOST "START_FLINK_PROCESSING"
    remote_operation ${KAFKA_HOST_PREFIX}1 "START_LOAD"
    sleep $TEST_TIME
    remote_operation ${KAFKA_HOST_PREFIX}1 "STOP_LOAD"
    remote_operation $FLINK_HOST "STOP_FLINK_PROCESSING"
    remote_operation $FLINK_HOST "STOP_FLINK"
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
      remote_operation $KAFKA_HOST_PREFIX$num "STOP_KAFKA"
    done
    remote_operation $REDIS_HOST "STOP_REDIS"
    remote_operation $ZK_HOST "STOP_ZK"
  elif [ "CLUSTER_START" = "$OPERATION" ];
  then
    remote_operation $ZK_HOST "START_ZK"
    remote_operation $REDIS_HOST "START_REDIS"
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
      remote_operation $KAFKA_HOST_PREFIX$num "START_KAFKA"
    done
    remote_operation $FLINK_HOST "START_FLINK"
    remote_operation $FLINK_HOST "START_FLINK_PROCESSING"
    remote_operation ${KAFKA_HOST_PREFIX}1 "START_LOAD"
  elif [ "CLUSTER_STOP" = "$OPERATION" ];
  then
    remote_operation ${KAFKA_HOST_PREFIX}1 "STOP_LOAD"
    remote_operation $FLINK_HOST "STOP_FLINK_PROCESSING"
    remote_operation $FLINK_HOST "STOP_FLINK"
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
      remote_operation $KAFKA_HOST_PREFIX$num "STOP_KAFKA"
    done
    remote_operation $REDIS_HOST "STOP_REDIS"
    remote_operation $ZK_HOST "STOP_ZK"
  elif [ "STOP_ALL" = "$OPERATION" ];
  then
    run "STOP_LOAD"
    run "STOP_FLINK_PROCESSING"
    run "STOP_FLINK"
    run "STOP_KAFKA"
    run "STOP_REDIS"
    run "STOP_ZK"
  else
    if [ "HELP" != "$OPERATION" ];
    then
      echo "UNKOWN OPERATION '$OPERATION'"
      echo
    fi
    echo "Supported Operations:"
    echo "SETUP: generate executable jars with \"maven package\""
    echo "INSTALL: install specified version of flink, redis, zookeeper, kafka"
    echo "CONFIG: copy config file under conf/ to corresponding software"
    echo "START_ZK: run a single node ZooKeeper instance on local host in the background"
    echo "STOP_ZK: kill the ZooKeeper instance"
    echo "START_REDIS: run a redis instance in the background"
    echo "STOP_REDIS: kill the redis instance"
    echo "START_KAFKA: run kafka in the background"
    echo "STOP_KAFKA: kill kafka"
    echo "START_LOAD: run kafka load generation"
    echo "STOP_LOAD: kill kafka load generation"
    echo "START_FLINK: run flink processes"
    echo "STOP_FLINK: kill flink processes"
    echo
    echo "START_FLINK_PROCESSING: run the flink test processing"
    echo "START_FLINK_SINGLELEVEL: run flink process with single level config"
    echo "STOP_FLINK_PROCESSSING: kill the flink test processing or single level test"
    echo
    echo "FLINK_TEST: run flink test (assumes SETUP is done)"
    echo "FLINK_DEBUG: run flink, without stopping"
    echo "FLINK_DEBUG_STOP: stop debugging flink test"
    echo "STOP_ALL: stop everything"
    echo
    echo "CLUSTER_TEST: start test on cluster (start components remotely on configured hosts)"
    echo "CLUSTER_START: start test on cluster, call CLUSTER_STOP to stop"
    echo "CLUSTER_STOP: stop the cluster"
    echo "CLUSTER_HDFS: start HDFS on cluster"
    echo "CLUSTER_HDFS_STOP: stop HDFS on cluster"
    echo
    echo "CLEAR_LOGS: clear logs of Flink, Kafka, Zookeeper, Redis"
    echo "CLEAR_CP: clear checkpoint files of Flink"
    echo "HELP: print out this message"
    echo
    exit 1
  fi
}

if [ $# -lt 1 ];
then
  run "HELP"
else
  while [ $# -gt 0 ];
  do
    run "$1"
    shift
  done
fi
