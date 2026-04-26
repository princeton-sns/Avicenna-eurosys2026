#!/bin/bash
LOSS_PERCENT=1
CUR_LATENCY=20  # Or pass as arg

IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')

# Apply to inter-site traffic (delayed)
# sudo tc qdisc replace dev "$IFACE" parent 1:1 handle 10: netem delay ${CUR_LATENCY}ms loss ${LOSS_PERCENT}%

# Apply to intra-site traffic (no delay, but now has loss)
# sudo tc qdisc replace dev "$IFACE" parent 1:2 handle 20: netem delay 0ms loss ${LOSS_PERCENT}%

# Apply to incoming traffic (ifb0)
sudo tc qdisc replace dev ifb0 root netem delay ${CUR_LATENCY}ms loss ${LOSS_PERCENT}%

echo "✅ Added ${LOSS_PERCENT}% packet loss to ALL traffic (latency still selective)"
