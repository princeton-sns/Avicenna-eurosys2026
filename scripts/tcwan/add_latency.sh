#!/bin/bash
ADD_LATENCY=$1
CUR_LATENCY=20  # Or pass as arg

IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')

# Apply to inter-site traffic (delayed)
sudo tc qdisc replace dev "$IFACE" parent 1:1 handle 10: netem delay $((CUR_LATENCY + ADD_LATENCY))ms
sudo tc qdisc replace dev "$IFACE" parent 1:2 handle 20: netem delay $((CUR_LATENCY + ADD_LATENCY))ms
# Apply to intra-site traffic (no delay, but now has loss)
sudo tc qdisc replace dev "$IFACE" parent 1:3 handle 30: netem delay ${ADD_LATENCY}ms
sudo tc qdisc replace dev "$IFACE" parent 1:4 handle 40: netem delay ${ADD_LATENCY}ms

# Apply to incoming traffic (ifb0)
sudo tc qdisc replace dev ifb0 root netem delay $((CUR_LATENCY + ADD_LATENCY))ms
sudo tc qdisc replace dev ifb1 root netem delay $((CUR_LATENCY + ADD_LATENCY))ms
sudo tc qdisc replace dev ifb2 root netem delay ${ADD_LATENCY}ms
sudo tc qdisc replace dev ifb3 root netem delay ${ADD_LATENCY}ms

echo "✅ Added ${ADD_LATENCY} packet latency to ALL traffic"
