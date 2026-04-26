#!/bin/bash
CUR_LATENCY=20  # Or pass as arg

IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')

# Restore original latency rules
sudo tc qdisc replace dev "$IFACE" parent 1:1 handle 10: netem delay ${CUR_LATENCY}ms
sudo tc qdisc replace dev "$IFACE" parent 1:2 handle 20: netem delay ${CUR_LATENCY}ms
sudo tc qdisc replace dev "$IFACE" parent 1:3 handle 30: netem delay 0ms
sudo tc qdisc replace dev "$IFACE" parent 1:4 handle 40: netem delay 0ms
sudo tc qdisc replace dev ifb0 root netem delay ${CUR_LATENCY}ms
sudo tc qdisc replace dev ifb1 root netem delay ${CUR_LATENCY}ms
sudo tc qdisc replace dev ifb2 root netem delay 0ms
sudo tc qdisc replace dev ifb3 root netem delay 0ms

echo "✅ Removed packet loss (latency selectivity preserved)"
