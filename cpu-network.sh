#!/bin/bash
BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark/results
NETWORK_FILE=$BASE_DIR/network.txt
CPU_MEM_FILE=$BASE_DIR/cpu-mem.txt
DISK_FILE=$BASE_DIR/disk.txt

if [ $# -lt 1 ]; then
  echo "init cpu-memory-network logs..."
  rm $NETWORK_FILE
  touch $NETWORK_FILE

  rm $CPU_MEM_FILE
  touch $CPU_MEM_FILE

  rm $DISK_FILE
  touch $DISK_FILE
else
  echo "`date +%s%3N` `vmstat | tail -n 1`" >> $CPU_MEM_FILE
  echo "`date +%s%3N` `grep "eth0" /proc/net/dev`" >> $NETWORK_FILE
  echo "`date +%s%3N` `vmstat -d | tail -n 1`" >> $DISK_FILE
fi

