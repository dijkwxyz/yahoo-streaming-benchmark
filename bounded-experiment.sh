#!/bin/bash
# used for stream-bench.sh
TEST_TIME=${TEST_TIME:-360}
CPU_LOAD_ADJUSTER=3000
#LOAD=${LOAD:-15000}
LOAD=${LOAD:-250000}
#LOAD_PER_NODE=5000
#GENERATE_DATA_TIME=$TEST_TIME
GENERATE_DATA_TIME=60

FLINK_PARALLELISM=3
SLOT_PER_NODE=2
# TM failure interval in seconds
TM_FAILURE_INTERVAL=${TM_FAILURE_INTERVAL:--1}
#TM_FAILURE_INTERVAL=525

# used for conf/benchmarkConf.yaml
MTTI_MS=${MTTI_MS:-200000}
#MTTI_MS=-1
CHECKPOINT_INTERVAL_MS=${CHECKPOINT_INTERVAL_MS:-30000}
#CHECKPOINT_INTERVAL_MS=${CHECKPOINT_INTERVAL_MS:-60000}
INJECT_WITH_PROBABILITY=true
let "FAILURE_START_DELAY_MS=0"
#let "FAILURE_START_DELAY_MS=$CHECKPOINT_INTERVAL_MS + 60000"

STATE_BACKEND=fs
#STATE_BACKEND=fs
MULTILEVEL_ENABLE=${MULTILEVEL_ENABLE:-true}

WINDOW_SIZE=${WINDOW_SIZE:-30}
WINDOW_SLIDE=${WINDOW_SLIDE:-1}

STREAM_ENDLESS=false
NUM_CAMPAIGNS=${NUM_CAMPAIGNS:-640}
#NUM_CAMPAIGNS=${NUM_CAMPAIGNS:-3360}
USE_LOCAL_GENERATOR=${USE_LOCAL_GENERATOR:-false}
REDIS_FLUSH=${REDIS_FLUSH:-false}

#1024*1024 = 1048576
#NET_THRESHOLD=${NET_THRESHOLD:-209600}
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
# FLINK_PARALLELISM=$FLINK_PARALLELISM
# NET_THRESHOLD=$NET_THRESHOLD
# ============ kafka ============
kafka.brokers:
    - \"kafka1\"
    - \"kafka2\"
kafka.port: 9092
kafka.topic: \"ad-events\"
kafka.isolation.level: read_uncommitted
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
test.parallelism: $FLINK_PARALLELISM

# ============ java load generator ============
# set if running the EventGenerator directly rather than reading from Kafka
use.local.event.generator: $USE_LOCAL_GENERATOR
# flush redis before starting load, turn of if using EventGenerator due to parallelism
redis.flush: $REDIS_FLUSH
# number of events per second
load.target.hz: $LOAD
cpu.load.adjuster: $CPU_LOAD_ADJUSTER
num.campaigns: $NUM_CAMPAIGNS
num.ad.per.campaigns: 10

# ========== others =============
# sample roughly every 3 seconds
throughput.log.freq: $(( $LOAD / $FLINK_PARALLELISM ))

# ========== experiment parameters =============
mtti.ms: $MTTI_MS
failure.inject.useProbability: $INJECT_WITH_PROBABILITY
failure.start.delay.ms: $FAILURE_START_DELAY_MS
stream.endless: $STREAM_ENDLESS
test.time.seconds: $TEST_TIME
generate.data.time.seconds: $GENERATE_DATA_TIME

# ============ checkpointing ============
flink.checkpoint.interval: $CHECKPOINT_INTERVAL_MS
flink.checkpoint.min-pause: $CHECKPOINT_INTERVAL_MS
multilevel.enable: $MULTILEVEL_ENABLE
#multilevel.level0.statebackend: \"memory\"
#multilevel.level0.path: \"\"
multilevel.level0.statebackend: \"$STATE_BACKEND\"
multilevel.level0.path: \"file:///dev/shm/flink/\"
multilevel.level1.statebackend: \"$STATE_BACKEND\"
multilevel.level1.path: \"file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/fs\"
multilevel.level2.statebackend: \"$STATE_BACKEND\"
multilevel.level2.path: \"hdfs://hadoop1:9000/flink/checkpoints\"
multilevel.pattern: \"1,2\"
singlelevel.statebackend: \"$STATE_BACKEND\"
singlelevel.path: \"hdfs://hadoop1:9000/flink/checkpoints\"
#singlelevel.path: \"file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/fs\"
" > $CONF_FILE
	}


# limit network bandwidth for experiment
for (( num=1; num <= 4; num += 1)); do
  ssh ec2-user@hadoop$num "sudo ~/wondershaper/wondershaper -c -a eth0"
  ssh ec2-user@hadoop$num "sudo ~/wondershaper/wondershaper -a eth0 -u $NET_THRESHOLD -d $NET_THRESHOLD"
done
#xdo "sudo /home/ec2-user/wondershaper/wondershaper -c -a eth0"
#xdo "sudo /home/ec2-user/wondershaper/wondershaper -a eth0 -u $NET_THRESHOLD -d $NET_THRESHOLD"

FLINK_WORKER_CONF=$BASE_DIR/flink-1.11.2/conf/workers

for (( num=0; num < 2; num += 1 )); do
    #for (( LOAD=40000; LOAD <= 40000; LOAD += 10000 )); do
#    for (( FLINK_PARALLELISM=4; FLINK_PARALLELISM <= 32; FLINK_PARALLELISM += 4 )); do
    for (( FLINK_PARALLELISM=28; FLINK_PARALLELISM <= 28; FLINK_PARALLELISM += 4 )); do
	./clear-data.sh

	echo "" > $FLINK_WORKER_CONF
	for (( tm_num=0; tm_num < $FLINK_PARALLELISM / $SLOT_PER_NODE; tm_num += 1 )); do
	  echo flink$(( 17 - $tm_num )) >> $FLINK_WORKER_CONF
	done
	xsync $FLINK_WORKER_CONF
	NUM_CAMPAIGNS=$(( $FLINK_PARALLELISM * 100 ))	
#	LOAD=$(( $FLINK_PARALLELISM * $LOAD_PER_NODE / $SLOT_PER_NODE ))

	MULTILEVEL_ENABLE=false
	make_conf
	echo "`date`: start experiment with LOAD = $LOAD, TIME = $TEST_TIME"
	cat $CONF_FILE | grep multilevel.enable
	xsync $CONF_FILE
	FLINK_PARALLELISM=$FLINK_PARALLELISM ./stream-bench.sh $TEST_TIME $TM_FAILURE_INTERVAL CLUSTER_TEST
	sleep 60

    done
done

xdo "sudo /home/ec2-user/wondershaper/wondershaper -c -a eth0"

