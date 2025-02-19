#!/bin/bash
BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark/results
NETWORK_FILE=$BASE_DIR/network.txt
CPU_FILE=$BASE_DIR/cpu.txt
MEMORY_FILE=$BASE_DIR/memory.txt
DISK_FILE=$BASE_DIR/disk.txt

if [ $# -lt 1 ]; then
  echo "init cpu-memory-network logs..."
  rm $NETWORK_FILE
  touch $NETWORK_FILE

  rm $CPU_FILE
  touch $CPU_FILE

  rm $MEMORY_FILE
  touch $MEMORY_FILE

  rm $DISK_FILE
  touch $DISK_FILE
else
  echo "`date +%s%3N` `grep "eth0" /proc/net/dev`" >> $NETWORK_FILE
  echo "`date +%s%3N` `top -n 1 -b | sed '3q;d'`" >> $CPU_FILE
  echo "`date +%s%3N` `top -n 1 -b | sed '4q;d'`" >> $MEMORY_FILE
  echo "`date +%s%3N` `vmstat -d | tail -n 1`" >> $DISK_FILE
fi

