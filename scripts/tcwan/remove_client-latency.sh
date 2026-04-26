#!/bin/bash
set -euo pipefail

CUR_LATENCY=${1:-20}

IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')
if [ -z "${IFACE}" ]; then
  echo "❌ Could not find interface with 10.1.1.x IP"
  exit 1
fi

# Bump latency ONLY for client paths (egress + ingress)
sudo tc qdisc replace dev "$IFACE" parent 1:1 handle 10: netem delay ${CUR_LATENCY}ms
sudo tc qdisc replace dev "$IFACE" parent 1:3 handle 30: netem delay 0ms
sudo tc qdisc replace dev ifb0 root netem delay ${CUR_LATENCY}ms
sudo tc qdisc replace dev ifb2 root netem delay 0ms

echo "✅ Added ${ADD_LATENCY}ms extra latency to CLIENT traffic (servers unchanged)"