#!/bin/bash

for i in $(seq 1 38); do
  ssh node-$i -o StrictHostKeyChecking=no "\
  sudo sysctl -w net.ipv4.tcp_low_latency=1; \
  sudo sysctl -w net.ipv4.tcp_timestamps=0; \
  sudo sysctl -w net.ipv4.tcp_sack=1; \
  sudo sysctl -w net.ipv4.tcp_window_scaling=1; \
  sudo sysctl -w net.core.netdev_max_backlog=50000; \
  sudo sysctl -w vm.swappiness=10
"
done
