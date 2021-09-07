#!/bin/bash

# used for stream-bench.sh
TEST_TIME=${TEST_TIME:-900}
TM_FAILURE_INTERVAL=${TM_FAILURE_INTERVAL:--1}

# used for conf/benchmarkConf.yaml
CHECKPOINT_INTERVAL_MS=${CHECKPOINT_INTERVAL_MS:-180000}
MTTI_MS=${MTTI_MS:-180000}
let "FAILURE_START_DELAY_MS=$CHECKPOINT_INTERVAL_MS + 60000"

MULTILEVEL_ENABLE=${MULTILEVEL_ENABLE:-true}

WINDOW_SIZE=${WINDOW_SIZE:-120}
WINDOW_SLIDE=${WINDOW_SLIDE:-1}

LOAD=${LOAD:-100000}
NUM_CAMPAIGNS=${NUM_CAMPAIGNS:-100}
USE_LOCAL_GENERATOR=${USE_LOCAL_GENERATOR:-false}
REDIS_FLUSH=${REDIS_FLUSH:-false}

#1024*1024 = 1048576
NET_THRESHOLD=${NET_THRESHOLD:-1048576}
# other
BASE_DIR=${BASE_DIR:-/home/ec2-user/yahoo-streaming-benchmark/}
CONF_FILE=${CONF_FILE:-${BASE_DIR}conf/benchmarkConf.yaml}


make_conf() {
    echo "# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.

# ====== experiment.sh config ======
# TEST_TIME=$TEST_TIME
# TM_FAILURE_INTERVAL=$TM_FAILURE_INTERVAL
#
# ============ kafka ============
kafka.brokers:
    - \"kafka1\"
    - \"kafka2\"
kafka.port: 9092
kafka.topic: \"ad-events\"
#consumer group
group.id: \"flink_yahoo_benchmark\"
#parallelism of kafka consumer
#kafka.partitions: 8
kafka.zookeeper.path: /
bootstrap.servers: \"kafka1:9092, kafka2:9092\"

# ============ zookeeper ============
zookeeper.servers:
    - \"zk1\"
zookeeper.port: 2181

# ============ redis ============
redis.host: \"redis1\"

# ============ Flink ============
window.size: $WINDOW_SIZE
window.slide: $WINDOW_SLIDE

# ============ java load generator ============
# set if running the EventGenerator directly rather than reading from Kafka
use.local.event.generator: $USE_LOCAL_GENERATOR
# flush redis before starting load, turn of if using EventGenerator due to parallelism
redis.flush: $REDIS_FLUSH
# number of events per second, per source node
load.target.hz: $LOAD
num.campaigns: $NUM_CAMPAIGNS
failure.start.delay.ms: $FAILURE_START_DELAY_MS

# ========== experiment parameters =============
mtti.ms: $MTTI_MS

# ============ checkpointing ============
flink.checkpoint.interval: $CHECKPOINT_INTERVAL_MS
multilevel.enable: $MULTILEVEL_ENABLE
#multilevel.level0.statebackend: \"memory\"
#multilevel.level0.path: \"\"
multilevel.level0.statebackend: \"fs\"
multilevel.level0.path: \"file:///dev/shm/flink/\"
multilevel.level1.statebackend: \"fs\"
multilevel.level1.path: \"file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/fs\"
multilevel.level2.statebackend: \"fs\"
multilevel.level2.path: \"hdfs://hadoop1:9000/flink/checkpoints\"
multilevel.pattern: \"1,1,2\"
singlelevel.statebackend: \"fs\"
singlelevel.path: \"hdfs://hadoop1:9000/flink/checkpoints\"
#singlelevel.path: \"file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/fs\"
" > $CONF_FILE
	}


# limit network bandwidth for experiment
#for (( num=1; num <= 4; num += 1)); do
#  ssh ec2-user@hadoop$num "sudo ~/wondershaper/wondershaper -c -a eth0"
#  ssh ec2-user@hadoop$num "sudo ~/wondershaper/wondershaper -a eth0 -u 202400 -d 404800"
#done
xdo "sudo /home/ec2-user/wondershaper/wondershaper -c -a eth0"
xdo "sudo /home/ec2-user/wondershaper/wondershaper -a eth0 -u $NET_THRESHOLD -d $NET_THRESHOLD"


for (( num=0; num < 1; num += 1 )); do
	for (( LOAD=60000; LOAD <= 60000; LOAD += 20000 )); do
	  ./clear-data.sh
	  MULTILEVEL_ENABLE=true
	  make_conf
	  echo "`date`: start experiment with LOAD = $LOAD, TIME = $TEST_TIME"
	  cat $CONF_FILE | grep multilevel.enable
	  xsync $CONF_FILE
	  ./stream-bench.sh $TEST_TIME $TM_FAILURE_INTERVAL CLUSTER_TEST
	  sleep 30
	done

done

xdo "sudo /home/ec2-user/wondershaper/wondershaper -c -a eth0"
