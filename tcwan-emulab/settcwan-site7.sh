#!/bin/bash
set -x
IFACE=enp6s0f0

# Clear previous settings
sudo tc qdisc del dev $IFACE root 2>/dev/null || true
sudo tc qdisc del dev $IFACE ingress 2>/dev/null || true
sudo tc qdisc del dev ifb0 root 2>/dev/null || true
sudo ip link set ifb0 down 2>/dev/null || true
sudo ip link delete ifb0 type ifb 2>/dev/null || true

# Create and bring up ifb0
sudo modprobe ifb
sudo ip link add ifb0 type ifb
sudo ip link set ifb0 up

# Add egress qdisc
sudo tc qdisc add dev $IFACE root handle 1: prio
sudo tc qdisc add dev $IFACE parent 1:1 handle 10: netem delay 30ms
sudo tc qdisc add dev $IFACE parent 1:2 handle 20: netem delay 0ms

# IPs to bypass delay (site-local traffic)
EXEMPT_NODES=(10.1.1.10 10.1.1.23 10.1.1.24)

# All relevant source IPs (node-1 to node-38)
ALL_IPS=()
for i in $(seq 0 38); do
  ALL_IPS+=("10.1.1.$((i+2))")
done

# Add egress filters
for ip in "${ALL_IPS[@]}"; do
  if [[ " ${EXEMPT_NODES[@]} " =~ " ${ip} " ]]; then
    echo "Bypassing egress delay for $ip"
    sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip dst $ip flowid 1:2
    sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip src $ip flowid 1:2
  else
    echo "Adding egress delay for $ip"
    sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip dst $ip flowid 1:1
    sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip src $ip flowid 1:1
  fi
done

# Add ingress qdisc on $IFACE
sudo tc qdisc add dev $IFACE ingress

# Redirect only non-exempt incoming IPs to ifb0
for ip in "${ALL_IPS[@]}"; do
  if [[ ! " ${EXEMPT_NODES[@]} " =~ " ${ip} " ]]; then
    echo "Adding ingress delay for $ip"
    sudo tc filter add dev $IFACE parent ffff: protocol ip prio 1 u32 match ip src $ip \
      action mirred egress redirect dev ifb0
  else
    echo "Bypassing ingress delay for $ip"
  fi
done

# Add ingress delay on ifb0
sudo tc qdisc add dev ifb0 root netem delay 20ms

echo "✅ 20ms delay applied to both egress and ingress, exempting site-local nodes"
