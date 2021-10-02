#!/bin/bash

BASE_DIR=`pwd`
hadoop fs -rm -r /flink

xdo-par "rm -r /tmp/flink*"
xdo-par "rm -r /tmp/localState"
xdo-par "rm -r /tmp/blobStore-*"
xdo-par "rm -r /tmp/kafka-logs/"
xdo-par "rm $BASE_DIR/results/*.*"
xdo-par "cd $BASE_DIR; ./stream-bench.sh CLEAR_LOGS"
xdo "cd $BASE_DIR; ./stream-bench.sh CLEAR_CP"
