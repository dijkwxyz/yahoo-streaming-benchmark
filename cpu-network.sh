#!/bin/bash
BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark/results
NETWORK_FILE=$BASE_DIR/network.txt
CPU_FILE=$BASE_DIR/cpu.txt
MEMORY_FILE=$BASE_DIR/memory.txt

if [ $# -lt 1 ]; then
  echo "init cpu-memory-network logs..."
  echo "" > $NETWORK_FILE
  echo "" > $CPU_FILE
  echo "" > $MEMORY_FILE
else
  echo "`date +%s%3N` `grep "eth" /proc/net/dev | head -n1`" >> $NETWORK_FILE
  echo "`date +%s%3N` `top -n 1 -b | sed '3q;d'`" >> $CPU_FILE
  echo "`date +%s%3N` `top -n 1 -b | sed '4q;d'`" >> $MEMORY_FILE
  sleep $1
fi

