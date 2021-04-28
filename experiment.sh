#!/bin/bash

BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark/
CONF_FILE=conf/benchmarkConf.yaml
WINDOW_SIZE=10
WINDOW_SLIDE=2
LOAD=100000
NUM_CAMPAIGNS=100
CHECKPOINT_INTERVAL_MS=10000
MULTILEVEL_ENABLE=false
USE_LOCAL_GENERATOR=false
REDIS_FLUSH=false

make_conf() {
    echo "# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.

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
" > $CONF_FILE
	}


for ((LOAD=100000; LOAD <= 200000; LOAD += 20000)); do
  echo "start experiment with LOAD = $LOAD"
  make_conf
  xsync $CONF_FILE
  ./stream-bench.sh CLUSTER_TEST
  sleep 270
done