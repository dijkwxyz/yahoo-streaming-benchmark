#!/bin/bash

BASE_DIR=`pwd`
hadoop fs -rm -r /flink

xdo "rm -r /tmp/flink*"
xdo "rm $BASE_DIR/results/*.*"
xdo "cd $BASE_DIR; ./stream-bench.sh CLEAR_LOGS; ./stream-bench.sh CLEAR_CP"
