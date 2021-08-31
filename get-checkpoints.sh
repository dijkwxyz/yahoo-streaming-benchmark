#!/bin/bash

JOB_ID="`flink list | grep 'AdvertisingTopologyFlinkWindows' | awk '{print $4}';`"
echo $JOB_ID
curl "localhost:8080/jobs/$JOB_ID/checkpoints" -o c.json
