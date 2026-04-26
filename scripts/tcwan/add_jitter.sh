#!/bin/bash

cur_node=$1

# Default jitter range (can be customized)
JITTER=5

# Validate input
if [ -z "$cur_node" ]; then
  echo "❌ Usage: $0 <cur_node>"
  exit 1
fi

# Set BASE_DELAY based on cur_node
if [ "$cur_node" -eq 2 ]; then
  BASE_DELAY=20
elif [ "$cur_node" -eq 3 ]; then
  BASE_DELAY=25
elif [ "$cur_node" -eq 4 ]; then
  BASE_DELAY=30
elif [ "$cur_node" -eq 5 ]; then
  BASE_DELAY=40
elif [ "$cur_node" -eq 6 ]; then
  BASE_DELAY=30
elif [ "$cur_node" -eq 7 ]; then
  BASE_DELAY=25
elif [ "$cur_node" -eq 8 ]; then
  BASE_DELAY=30
else
  echo "❌ Unknown cur_node: $cur_node"
  exit 1
fi

# Configurable parameters
BASE_DELAY=20     # Base delay in ms (same as your CUR_LATENCY)
JITTER=5          # Jitter range (±JITTER ms)

# Detect the correct interface (e.g., enp6s0f0 or enp6s0f1)
IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')

# Apply jitter on incoming (inter-site) traffic via ifb0
sudo tc qdisc replace dev ifb0 root netem delay ${BASE_DELAY}ms ${JITTER}ms distribution normal

echo "✅ Added jitter of ±${JITTER}ms around ${BASE_DELAY}ms delay to ingress traffic (latency still selective)"
