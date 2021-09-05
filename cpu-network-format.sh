#!/bin/bash
BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark/results
NETWORK_FILE=$BASE_DIR/network.txt
CPU_FILE=$BASE_DIR/cpu.txt
MEMORY_FILE=$BASE_DIR/memory.txt

echo "`date +%s%3N` `grep "eth" /proc/net/dev |head -n1|sed -e 's/^[ \t]*//'| sed -n 's/  \+/ /gp' | sed -e 's/://'`" >> $NETWORK_FILE 
echo "`date +%s%3N` `top -n 1 -b | head -n4 | grep Cpu | sed 's/[^0-9\.,]//g' | sed 's/,/ /g'`" >> $CPU_FILE 
echo "`date +%s%3N` `top -n 1 -b | head -n4 | grep "KiB Mem" | sed 's/[^0-9\.,]//g' | sed 's/,/ /g'`" >> $MEMORY_FILE 

