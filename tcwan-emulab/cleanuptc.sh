#!/bin/bash
set -x
IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')
if [ -z "$IFACE" ]; then
  echo "❌ Could not find interface with 10.1.1.x IP"
  exit 1
fi

# Delete qdiscs from main interface
sudo tc qdisc del dev $IFACE root 2>/dev/null || true
sudo tc qdisc del dev $IFACE ingress 2>/dev/null || true

# Delete qdiscs from ifb0
sudo tc qdisc del dev ifb0 root 2>/dev/null || true

# Bring down and delete ifb0
sudo ip link set ifb0 down 2>/dev/null || true
sudo ip link delete ifb0 type ifb 2>/dev/null || true

echo "✅ All tc and ifb rules cleaned up on $IFACE"
