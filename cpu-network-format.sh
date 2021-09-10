#!/bin/bash
BASE_DIR=/home/ec2-user/yahoo-streaming-benchmark/results
NETWORK_FILE=$BASE_DIR/network
CPU_MEM_FILE=$BASE_DIR/cpu-mem
DISK_FILE=$BASE_DIR/disk

#network
#eth0 53544832407 365034199 0 235 0 0 0 0 18211342535 6149027 0 0 0 0 0 0
#  echo "timestamp interface recv_bytes recv_packets recv_errs recv_dropped recv_fifo recv_frame recv_compressed recv_multicast sent_bytes sent_packets sent_errs sent_dropped sent_fifo sent_frame sent_compressed sent_multicast" > $NETWORK_FILE

#top: cpu+memory
#%Cpu(s):  1.6 us,  0.0 sy,  0.0 ni, 98.4 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st
#KiB Mem : 16266168 total, 13684824 free,   589088 used,  1992256 buff/cache
#  echo "timestamp user_space system nice idle iowait hardware_interrupt software_interrupt steal" > $CPU_FILE
#  echo "timetamp total free used buff/cache" > $MEMORY_FILE

#iotop: disk
#1630893722428 Total DISK READ :       0.00 B/s | Total DISK WRITE :       0.00 B/s

sed -i 's/ \+/ /gp' $NETWORK_FILE-*.txt
sed -i "1itimestamp interface recv_bytes recv_packets recv_errs recv_dropped recv_fifo recv_frame recv_compressed recv_multicast sent_bytes sent_packets sent_errs sent_dropped sent_fifo sent_frame sent_compressed sent_multicast" $NETWORK_FILE-*.txt

#sed -i 's/[^0-9\.,:]//g' $CPU_MEM_FILE-*.txt
#sed -i 's/[,:]/ /g' $CPU_MEM_FILE-*.txt
#sed -i "1itimestamp user_space system nice idle iowait hardware_interrupt software_interrupt steal" $CPU_FILE-*.txt
sed -i 's/ \+/ /g' $CPU_MEM_FILE-*.txt
sed -i "1itimestamp r b swpd free buff cache si so bi bo in cs us sy id wa st" $CPU_MEM_FILE-*.txt

sed -i 's/ \+/ /g' $DISK_FILE-*.txt
sed -i "1itimestamp disk read_total read_merged read_sectors read_ms write_total write_merged write_sectors write_ms io_cur io_sec" $DISK_FILE-*.txt


