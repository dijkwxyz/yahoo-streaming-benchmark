#!/bin/bash
# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
set -o pipefail
set -o errtrace
set -o nounset
set -o errexit

LEIN=${LEIN:-~/bin/lein}
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

BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark
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

FLINK_PARALLELISM=8
TOPIC=${TOPIC:-"ad-events"}
PARTITIONS=${FLINK_PARALLELISM}
LOAD=${LOAD:-5000000}
CONF_FILE=conf/benchmarkConf.yaml
SINGLELEVEL_CONF_FILE=./conf/singleLevelConf.yaml
#test time in seconds
TEST_TIME=${TEST_TIME:-240}
TM_FAIL_INTERVAL=${TM_FAIL_INTERVAL:--1}
# start time for new TM before killing the old one when swap tm
TM_START_BUFFER=${TM_START_BUFFER:-10}

swap_flink_tm() {
  echo "### `date`: starting new TM $2"
  remote_operation $2 "START_TM"
  sleep $TM_START_BUFFER
  echo "### `date`: killing TM $1"
  remote_operation $1 "STOP_TM"
}

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

remote_operation_sync() {
  local host="$1"
  shift
  local cmd="$@"
  ssh ec2-user@$host "cd $BASE_DIR; ./stream-bench.sh $cmd"
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

    #$MVN clean package -Dkafka.version="$KAFKA_VERSION" -Dflink.version="$FLINK_VERSION" -Dscala.binary.version="$SCALA_BIN_VERSION" -Dscala.version="$SCALA_BIN_VERSION.$SCALA_SUB_VERSION"
    xdo "rm ~/yahoo-streaming-benchmark/flink-benchmarks/target/flink-benchmarks-0.1.0.jar"
    $MVN clean package
    xsync ~/yahoo-streaming-benchmark/flink-benchmarks/target/flink-benchmarks-0.1.0.jar

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
    rm -r /tmp/zookeeper
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
#    cd data
#    $LEIN run -n --configPath ../conf/benchmarkConf.yaml
#    cd ..
  elif [ "STOP_REDIS" = "$OPERATION" ];
  then
    # get results before stopping Redis
    cd results
    java -cp /home/ec2-user/yahoo-streaming-benchmark/flink-benchmarks/target/flink-benchmarks-0.1.0.jar flink.benchmark.utils.RedisDataGetter $BASE_DIR/$CONF_FILE
    cd ..
    stop_if_needed redis-server Redis
    rm -f dump.rdb
  elif [ "START_KAFKA" = "$OPERATION" ];
  then
    start_if_needed kafka\.Kafka Kafka 8 "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties"
    create_kafka_topic
  elif [ "STOP_KAFKA" = "$OPERATION" ];
  then
    $KAFKA_DIR/bin/kafka-server-stop.sh
    rm -r /tmp/kafka-logs/
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
#    cd data
    start_if_needed KafkaDataGenerator "Load Generation" 1 java -cp $BASE_DIR/flink-benchmarks/target/flink-benchmarks-0.1.0.jar flink.benchmark.generator.KafkaDataGenerator $BASE_DIR/$CONF_FILE "" 1 > load.log
    echo "INFO: start load ..."
#    start_if_needed leiningen.core.main "Load Generation" 1 $LEIN run -r -t $LOAD --configPath ../$CONF_FILE
#    cd ..
  elif [ "START_LOAD_ON_HOST" = "$OPERATION" ];
  then
    LEADER_HOST=$2
    start_if_needed KafkaDataGenerator "Load Generation" 1 java -cp $BASE_DIR/flink-benchmarks/target/flink-benchmarks-0.1.0.jar flink.benchmark.generator.KafkaDataGenerator $BASE_DIR/$CONF_FILE $LEADER_HOST $KAFKA_HOST_NUM
    echo "INFO: start load on $LEADER_HOST ..."
  elif [ "STOP_LOAD" = "$OPERATION" ];
  then
    stop_if_needed KafkaDataGenerator "Load Generation"
#    stop_if_needed leiningen.core.main "Load Generation"
    echo "INFO: stop load ..."
#    $LEIN run -g --configPath ../$CONF_FILE || true
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
  elif [ "START_TM" = "$OPERATION" ];
  then
    $BASE_DIR/$FLINK_DIR/bin/taskmanager.sh start
  elif [ "STOP_TM" = "$OPERATION" ];
  then
    stop_if_needed TaskManagerRunner TaskManager
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
    run "CLUSTER_START"
    echo "TEST_TIME=$TEST_TIME, TM_FAIL_INTERVAL=$TM_FAIL_INTERVAL"
    if [ $TM_FAIL_INTERVAL -gt 0 ]; then
      echo "### This test will Inject TM Failures"
      for ((TIME=0; TIME < $TEST_TIME / $TM_FAIL_INTERVAL; TIME += 1)); do
        if (( $TM_FAIL_INTERVAL > $TM_START_BUFFER )); then
          sleep $(( $TM_FAIL_INTERVAL - $TM_START_BUFFER ))
        else
          sleep $TM_START_BUFFER
        fi
        echo "### `date`: Injecting TM Failure"
        if (($TIME % 2 == 0)); then
          swap_flink_tm flink3 redis2
        else
          swap_flink_tm redis2 flink3
        fi
      done
      if (( $TM_FAIL_INTERVAL * $TIME < $TEST_TIME )); then
        sleep $(( $TEST_TIME - $TIME * $TM_FAIL_INTERVAL))
      fi
    else
      echo "No Failure Injection"
      sleep $TEST_TIME
    fi
    remote_operation redis2 "STOP_TM"
    run "CLUSTER_STOP"
  elif [ "CLUSTER_START" = "$OPERATION" ];
  then
    echo "### `date`: CLUSTER_START"
    cp $CONF_FILE $BASE_DIR/results/conf-copy.yaml
    remote_operation $ZK_HOST "START_ZK"
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
      remote_operation $KAFKA_HOST_PREFIX$num "START_KAFKA"
    done
    remote_operation $REDIS_HOST "START_REDIS"
    remote_operation $FLINK_HOST "START_FLINK"
    sleep 8
    remote_operation $FLINK_HOST "START_FLINK_PROCESSING"
    sleep 5
#    remote_operation ${KAFKA_HOST_PREFIX}1 "START_LOAD" ${KAFKA_HOST_PREFIX}1
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
        remote_operation $KAFKA_HOST_PREFIX$num "START_LOAD_ON_HOST" $KAFKA_HOST_PREFIX$num
    done
  elif [ "CLUSTER_STOP" = "$OPERATION" ];
  then
    echo "### `date`: CLUSTER_STOP"
#    remote_operation_sync ${KAFKA_HOST_PREFIX}1 "STOP_LOAD"
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
        remote_operation $KAFKA_HOST_PREFIX$num "STOP_LOAD"
    done
    remote_operation_sync $FLINK_HOST "STOP_FLINK_PROCESSING"
    remote_operation $FLINK_HOST "STOP_FLINK"
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
      remote_operation $KAFKA_HOST_PREFIX$num "STOP_KAFKA"
    done
    remote_operation $REDIS_HOST "STOP_REDIS"
    remote_operation $ZK_HOST "STOP_ZK"
    # wait for writing redis data to file before analyzing
    sleep 5
    ./remote.sh ANALYZE
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

if [ $# -lt 3 ];
then
  run $@
else
  TEST_TIME=$1
  TM_FAIL_INTERVAL=$2
  shift 2
  run $@
fi


#if [ $# -lt 1 ];
#then
#  run "HELP"
#else
#  while [ $# -gt 0 ];
#  do
#    run "$1"
#    shift
#  done
#fi
