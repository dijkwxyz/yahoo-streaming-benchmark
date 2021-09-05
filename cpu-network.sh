#!/bin/bash
BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark/results
NETWORK_FILE=$BASE_DIR/network.txt
CPU_FILE=$BASE_DIR/cpu.txt
MEMORY_FILE=$BASE_DIR/memory.txt

if [ $# -lt 1 ]; then
  echo "init cpu-memory-network logs..."
  #network
  #eth0 53544832407 365034199 0 235 0 0 0 0 18211342535 6149027 0 0 0 0 0 0
  echo "timestamp interface recv_bytes recv_packets recv_errs recv_dropped recv_fifo recv_frame recv_compressed recv_multicast sent_bytes sent_packets sent_errs sent_dropped sent_fifo sent_frame sent_compressed sent_multicast" > $NETWORK_FILE

  #top: cpu+memory
  #%Cpu(s):  1.6 us,  0.0 sy,  0.0 ni, 98.4 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st
  #KiB Mem : 16266168 total, 13684824 free,   589088 used,  1992256 buff/cache
  echo "timestamp user_space system nice idle iowait hardware_interrupt software_interrupt steal" > $CPU_FILE
  echo "timetamp total free used buff/cache" > $MEMORY_FILE
else
  echo "`date +%s%3N` `grep "eth" /proc/net/dev | head -n1`" >> $NETWORK_FILE
  echo "`date +%s%3N` `top -n 1 -b | sed '3q;d'`" >> $CPU_FILE
  echo "`date +%s%3N` `top -n 1 -b | sed '4q;d'`" >> $MEMORY_FILE
  sleep $1
fi

