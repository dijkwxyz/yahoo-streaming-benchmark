#!/bin/bash

ZK_HOST="zk1"
ZK_PORT="2181"
ZK_CONNECTIONS="$ZK_HOST:$ZK_PORT"
KAFKA_HOST_PREFIX="kafka"
KAFKA_HOST_NUM=2
HADOOP_HOST="hadoop1"
YARN_HOST="hadoop4"
FLINK_HOST="flink1"
REDIS_HOST="redis1"


remote_operation() {
  local host="$1"
  shift
  local cmd="$@"
  ssh ec2-user@$host "cd $BASE_DIR; ./stream-bench.sh $cmd" &
}

run_command() {
  OPERATION=$1
  if [ "START_ZK" = "$OPERATION" ];
  then
      remote_operation $ZK_HOST "START_ZK"
  elif [ "STOP_ZK" = "$OPERATION" ];
  then
        remote_operation $ZK_HOST "STOP_ZK"
  elif [ "START_REDIS" = "$OPERATION" ];
  then
    remote_operation $REDIS_HOST "START_REDIS"
  elif [ "START_KAFKA" = "$OPERATION" ];
  then
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
      remote_operation $KAFKA_HOST_PREFIX$num "START_KAFKA"
    done
  elif [ "START_FLINK" = "$OPERATION" ];
  then
    remote_operation $FLINK_HOST "START_FLINK"
  elif [ "START_FLINK_PROCESSING" = "$OPERATION" ];
  then
    remote_operation $FLINK_HOST "START_FLINK_PROCESSING"
  elif [ "START_LOAD" = "$OPERATION" ];
  then
    remote_operation ${KAFKA_HOST_PREFIX}1 "START_LOAD"
  elif [ "STOP_LOAD" = "$OPERATION" ];
  then
    remote_operation ${KAFKA_HOST_PREFIX}1 "STOP_LOAD"
  elif [ "STOP_FLINK_PROCESSING" = "$OPERATION" ];
  then
    remote_operation $FLINK_HOST "STOP_FLINK_PROCESSING"
  elif [ "STOP_FLINK" = "$OPERATION" ];
  then
    remote_operation $FLINK_HOST "STOP_FLINK"
  elif [ "STOP_KAFKA" = "$OPERATION" ];
  then
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
      remote_operation $KAFKA_HOST_PREFIX$num "STOP_KAFKA"
    done
  elif [ "STOP_REDIS" = "$OPERATION" ];
  then
    remote_operation $REDIS_HOST "STOP_REDIS"
  else
    if [ "HELP" != "$OPERATION" ];
    then
      echo "UNKOWN OPERATION '$OPERATION'"
      echo
    fi
    echo "Supported Operations:"
    echo "START_ZK: run_command a single node ZooKeeper instance on local host in the background"
    echo "STOP_ZK: kill the ZooKeeper instance"
    echo "START_REDIS: run_command a redis instance in the background"
    echo "STOP_REDIS: kill the redis instance"
    echo "START_KAFKA: run_command kafka in the background"
    echo "STOP_KAFKA: kill kafka"
    echo "START_FLINK: run_command flink processes"
    echo "STOP_FLINK: kill flink processes"
    echo
    echo "START_FLINK_PROCESSING: run_command the flink test processing"
#    echo "START_FLINK_SINGLELEVEL: run_command flink process with single level config"
    echo "STOP_FLINK_PROCESSSING: kill the flink test processing or single level test"
    echo
    echo "HELP: print out this message"
    echo
    exit 1
  fi
}

if [ $# -lt 1 ];
then
  run_command "HELP"
else
  while [ $# -gt 0 ];
  do
    run_command "$1"
    shift
  done
fi
