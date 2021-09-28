#!/bin/bash

BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark/
ZK_HOST="zk1"
ZK_PORT="2181"
ZK_CONNECTIONS="$ZK_HOST:$ZK_PORT"
KAFKA_HOST_PREFIX="kafka"
KAFKA_HOST_NUM=2
HADOOP_HOST="hadoop1"
YARN_HOST="hadoop4"
FLINK_HOST="flink1"
REDIS_HOST="redis1"

JAR_PATH=$BASE_DIR/flink-benchmarks/target/flink-benchmarks-0.1.0.jar
ANALYZE_MAIN_CLASS=flink.benchmark.utils.AnalyzeTool
RESULTS_DIR=$BASE_DIR/results/
FLINK_LOG_DIR=$BASE_DIR/flink-1.11.2/log/

remote_operation() {
  local host="$1"
  shift
  local cmd="$@"
  ssh ec2-user@$host "cd $BASE_DIR; ./stream-bench.sh $cmd" &
}

analyze_on_host_tm() {
  local host="$1"
  shift
  # since tm will be killed, there may be multiple log files
  ssh $host "java -cp $JAR_PATH $ANALYZE_MAIN_CLASS tm $RESULTS_DIR/ $FLINK_LOG_DIR/*.out* "
  ssh $host "cat $FLINK_LOG_DIR/flink-*.out* > $RESULTS_DIR/$host.out"
  ssh $host "cat $FLINK_LOG_DIR/flink-*.log* > $RESULTS_DIR/$host.log"
  scp ec2-user@$host:$FLINK_LOG_DIR/gc.log ec2-user@zk1:$RESULTS_DIR/$host-gc.log
  scp ec2-user@$host:$RESULTS_DIR/throughputs.txt ec2-user@zk1:$RESULTS_DIR/$host.txt
  scp ec2-user@$host:$RESULTS_DIR/heap.txt ec2-user@zk1:$RESULTS_DIR/heap-$host.txt
  scp ec2-user@$host:$RESULTS_DIR/$host.log ec2-user@zk1:$RESULTS_DIR/$host.log
  scp ec2-user@$host:$RESULTS_DIR/$host.out ec2-user@zk1:$RESULTS_DIR/$host.out
}

analyze_on_host_jm() {
  local host="$1"
  shift
  # only use current jm log
  ssh $host "java -cp $JAR_PATH $ANALYZE_MAIN_CLASS jm $RESULTS_DIR/ $FLINK_LOG_DIR/*-standalonesession-*.log"
  ssh $host "cat $FLINK_LOG_DIR/flink-*-standalonesession-*.log > $RESULTS_DIR/$host.log"
  scp ec2-user@$host:$FLINK_LOG_DIR/gc.log ec2-user@zk1:$RESULTS_DIR/$host-gc.log
  scp ec2-user@$host:$RESULTS_DIR/$host.log ec2-user@zk1:$RESULTS_DIR/$host.log
  scp ec2-user@$host:$RESULTS_DIR/checkpoints.json ec2-user@zk1:$RESULTS_DIR/checkpoints.json
  scp ec2-user@$host:$RESULTS_DIR/job.json ec2-user@zk1:$RESULTS_DIR/job.json
  scp ec2-user@$host:$RESULTS_DIR/checkpoints.txt ec2-user@zk1:$RESULTS_DIR/checkpoints.txt
}

copy_cpu_network_log() {
  local host="$1"
  shift
  scp ec2-user@$host:$RESULTS_DIR/network.txt ec2-user@zk1:$RESULTS_DIR/network-$host.txt
  scp ec2-user@$host:$RESULTS_DIR/cpu.txt ec2-user@zk1:$RESULTS_DIR/cpu-$host.txt
  scp ec2-user@$host:$RESULTS_DIR/memory.txt ec2-user@zk1:$RESULTS_DIR/memory-$host.txt
  scp ec2-user@$host:$RESULTS_DIR/disk.txt ec2-user@zk1:$RESULTS_DIR/disk-$host.txt
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
    remote_operation ${KAFKA_HOST_PREFIX}1 "START_KAFKA_TOPIC"
  elif [ "START_FLINK" = "$OPERATION" ];
  then
    remote_operation $FLINK_HOST "START_FLINK"
  elif [ "START_JOB" = "$OPERATION" ];
  then
    remote_operation $FLINK_HOST "START_FLINK_PROCESSING"
  elif [ "START_LOAD" = "$OPERATION" ];
  then
#    remote_operation ${KAFKA_HOST_PREFIX}1 "START_LOAD"
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
        remote_operation $KAFKA_HOST_PREFIX$num "START_LOAD_ON_HOST" $KAFKA_HOST_PREFIX$num
    done
  elif [ "STOP_LOAD" = "$OPERATION" ];
  then
    for ((num=1; num <=$KAFKA_HOST_NUM; num++)); do
        remote_operation $KAFKA_HOST_PREFIX$num "STOP_LOAD"
    done
  elif [ "STOP_JOB" = "$OPERATION" ];
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
  elif [ "START_APPS" = "$OPERATION" ];
  then
     run_command "START_ZK"
     run_command "START_REDIS"
     run_command "START_KAFKA"
     run_command "START_FLINK"
  elif [ "STOP_APPS" = "$OPERATION" ];
  then
     run_command "STOP_ZK"
     run_command "STOP_REDIS"
     run_command "STOP_KAFKA"
     run_command "STOP_FLINK"
  elif [ "ANALYZE_FLINK" = "$OPERATION" ];
  then
    echo "====== collecting checkpoint and recovery results from jm"
    analyze_on_host_jm $FLINK_HOST
    echo "====== collecting throughput results from tm"
    for ((num=2; num<=17; num++)); do
      analyze_on_host_tm "flink$num"
    done
  elif [ "ANALYZE" = "$OPERATION" ];
  then
    run_command "ANALYZE_FLINK"
    echo "====== collecting latency results from redis"
    scp ec2-user@$REDIS_HOST:$RESULTS_DIR/count-latency.txt ec2-user@zk1:$RESULTS_DIR/
    echo "====== collecting cpu-network data"
    copy_cpu_network_log hadoop1
    copy_cpu_network_log hadoop2
    copy_cpu_network_log hadoop3
    copy_cpu_network_log hadoop4
    copy_cpu_network_log zk1
    copy_cpu_network_log redis1
    copy_cpu_network_log kafka1
    copy_cpu_network_log kafka2
    for ((num=1; num <= 17; num++)); do
      copy_cpu_network_log flink$num
    done

    ./cpu-network-format.sh
    echo "====== analyzing data"
    java -cp $JAR_PATH $ANALYZE_MAIN_CLASS zk $RESULTS_DIR/ 2 17
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
    echo "START_JOB: run_command the flink test processing"
#    echo "START_FLINK_SINGLELEVEL: run_command flink process with single level config"
    echo "STOP_JOB: kill the flink test processing or single level test"
    #echo "STOP_FLINK_PROCESSSING: kill the flink test processing or single level test"
    echo
    echo "START_LOAD"
    echo "STOP_LOAD"
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
