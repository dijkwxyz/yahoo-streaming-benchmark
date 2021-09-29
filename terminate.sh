#!/bin/bash

kill `ps -aef |grep CLUSTER_TEST |grep -v grep | awk '{print $2}'`
kill `ps -aef |grep experiment.sh |grep -v grep | awk '{print $2}'`
./stream-bench.sh CLUSTER_STOP
