#!/bin/bash

cur_node=$1

if [ -z "$cur_node" ]; then
  echo "❌ Usage: $0 <cur_node>"
  exit 1
fi

# Set BASE_DELAY based on cur_node
if [ "$cur_node" -eq 2 ]; then
  CUR_LATENCY=20
elif [ "$cur_node" -eq 3 ]; then
  CUR_LATENCY=25
elif [ "$cur_node" -eq 4 ]; then
  CUR_LATENCY=30
elif [ "$cur_node" -eq 5 ]; then
  CUR_LATENCY=40
elif [ "$cur_node" -eq 6 ]; then
  CUR_LATENCY=30
elif [ "$cur_node" -eq 7 ]; then
  CUR_LATENCY=25
elif [ "$cur_node" -eq 8 ]; then
  CUR_LATENCY=30
else
  echo "❌ Unknown cur_node: $cur_node"
  exit 1
fi

# Detect interface (e.g., enp6s0f0 or enp6s0f1) with 10.1.1.x IP
IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')

# Restore original latency rules without jitter
sudo tc qdisc replace dev "$IFACE" parent 1:1 handle 10: netem delay ${CUR_LATENCY}ms
sudo tc qdisc replace dev "$IFACE" parent 1:2 handle 20: netem delay 0ms
sudo tc qdisc replace dev ifb0 root netem delay ${CUR_LATENCY}ms

echo "✅ Removed jitter (latency preserved for inter-site traffic)"