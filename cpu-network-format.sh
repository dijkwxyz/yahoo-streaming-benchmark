#!/bin/bash
BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark/results
NETWORK_FILE=$BASE_DIR/network.txt
CPU_FILE=$BASE_DIR/cpu.txt
MEMORY_FILE=$BASE_DIR/memory.txt

sed -e 's/^[ \t]*//' $NETWORK_FILE | sed -n 's/  \+/ /gp' | sed -e 's/://' > $NETWORK_FILE.copy
mv $NETWORK_FILE.copy $NETWORK_FILE

sed 's/[^0-9\.,]//g' $CPU_FILE | sed 's/,/ /g' > $CPU_FILE.copy
mv $CPU_FILE.copy $CPU_FILE

sed 's/[^0-9\.,]//g' $MEMORY_FILE | sed 's/,/ /g' > $MEMORY_FILE.copy
mv $MEMORY_FILE.copy $MEMORY_FILE