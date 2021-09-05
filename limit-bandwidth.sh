#!/bin/bash

for (( num=1; num <= 4; num += 1)); do
  ssh ec2-user@hadoop$num "sudo ~/wondershaper/wondershaper -c -a eth0"
  ssh ec2-user@hadoop$num "sudo ~/wondershaper/wondershaper -a eth0 -u 202400 -d 404800"
done

