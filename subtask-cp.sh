#!/bin/bash

num_checkpoints=$1

JOB_ID=`flink list | grep 'AdvertisingTopologyFlinkWindows' | awk '{print $4}'`
#JOB_ID=1b26f9d011520f47d2910a0366bf2f63
OPERATOR_ID=7631abc3255269312c80a80c030a71e3

OUT_FILE=results/subtask-cp.json

echo "[" > $OUT_FILE
for ((num=1; num<$num_checkpoints; num+=1)); do
  curl http://flink1:8080/jobs/$JOB_ID/checkpoints/details/$num/subtasks/$OPERATOR_ID >> $OUT_FILE
  echo "," >> $OUT_FILE
done
curl http://flink1:8080/jobs/$JOB_ID/checkpoints/details/$num_checkpoints/subtasks/$OPERATOR_ID >> $OUT_FILE
echo "]" >> $OUT_FILE
