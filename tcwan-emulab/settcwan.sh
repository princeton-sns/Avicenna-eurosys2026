#!/bin/bash
set -x

IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')
if [ -z "$IFACE" ]; then
  echo "❌ Could not find interface with 10.1.1.x IP"
  exit 1
fi
cur_node=$1  # Usage: ./set_latency.sh 0

# ----------- Site membership and latency config -----------

SITE_1=(0 1 2 9 10)
SITE_2=(3 11 12)
SITE_3=(4 13 14)
SITE_4=(5 15 16)
SITE_5=(6 17 18)
SITE_6=(7 19 20)
SITE_7=(8 21 22)

LATENCY_1=20
LATENCY_2=25
LATENCY_3=30
LATENCY_4=40
LATENCY_5=30
LATENCY_6=25
LATENCY_7=30

ALL_IPS=()
for i in $(seq 0 38); do 
    ALL_IPS+=("10.1.1.$((i+2))")
done

# ----------- Detect current site -----------

in_site() {
    local node=$1
    shift
    for n in "$@"; do
        if [ "$n" == "$node" ]; then return 0; fi
    done
    return 1
}

if in_site "$cur_node" "${SITE_1[@]}"; then
    CUR_SITE=1; CUR_SITE_NODES=("${SITE_1[@]}"); CUR_LATENCY=$LATENCY_1
elif in_site "$cur_node" "${SITE_2[@]}"; then
    CUR_SITE=2; CUR_SITE_NODES=("${SITE_2[@]}"); CUR_LATENCY=$LATENCY_2
elif in_site "$cur_node" "${SITE_3[@]}"; then
    CUR_SITE=3; CUR_SITE_NODES=("${SITE_3[@]}"); CUR_LATENCY=$LATENCY_3
elif in_site "$cur_node" "${SITE_4[@]}"; then
    CUR_SITE=4; CUR_SITE_NODES=("${SITE_4[@]}"); CUR_LATENCY=$LATENCY_4
elif in_site "$cur_node" "${SITE_5[@]}"; then
    CUR_SITE=5; CUR_SITE_NODES=("${SITE_5[@]}"); CUR_LATENCY=$LATENCY_5
elif in_site "$cur_node" "${SITE_6[@]}"; then
    CUR_SITE=6; CUR_SITE_NODES=("${SITE_6[@]}"); CUR_LATENCY=$LATENCY_6
elif in_site "$cur_node" "${SITE_7[@]}"; then
    CUR_SITE=7; CUR_SITE_NODES=("${SITE_7[@]}"); CUR_LATENCY=$LATENCY_7
else
    echo "❌ Unknown node $cur_node"
    exit 1
fi

# ----------- Get current node's IP -----------

MY_IP=$(ip -4 addr show dev "$IFACE" | grep -oP '(?<=inet\s)\d+(\.\d+){3}')
echo "🔍 Detected local IP: $MY_IP"

# ----------- Compute exempt IPs (intra-site) -----------

EXEMPT_IPS=()
for nid in "${CUR_SITE_NODES[@]}"; do
    ip="10.1.1.$((nid+2))"
    if [[ "$ip" != "$MY_IP" ]]; then
        EXEMPT_IPS+=("$ip")
    fi
done

echo "✅ Node $cur_node in Site $CUR_SITE with latency $CUR_LATENCY ms"
echo "   Exempt IPs (other intra-site): ${EXEMPT_IPS[*]}"

# ----------- Clear existing tc settings -----------

sudo tc qdisc del dev $IFACE root 2>/dev/null || true
sudo tc qdisc del dev $IFACE ingress 2>/dev/null || true
sudo tc qdisc del dev ifb0 root 2>/dev/null || true
sudo ip link set ifb0 down 2>/dev/null || true
sudo ip link delete ifb0 type ifb 2>/dev/null || true

# ----------- Set up ifb0 and egress qdisc -----------

sudo modprobe ifb
sudo ip link add ifb0 type ifb
sudo ip link set ifb0 up

sudo tc qdisc add dev $IFACE root handle 1: prio
sudo tc qdisc add dev $IFACE parent 1:1 handle 10: netem delay ${CUR_LATENCY}ms
sudo tc qdisc add dev $IFACE parent 1:2 handle 20: netem delay 0ms

# ----------- Add egress filters -----------

for ip in "${ALL_IPS[@]}"; do
    if [[ "$ip" == "$MY_IP" ]]; then
        echo "Skipping self IP $ip for egress"
        continue
    fi
    if [[ " ${EXEMPT_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Bypassing egress delay for $ip"
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip dst $ip flowid 1:2
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip src $ip flowid 1:2
    else
        echo "Applying egress delay for $ip"
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip dst $ip flowid 1:1
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip src $ip flowid 1:1
    fi
done

# ----------- Add ingress redirection filters -----------

sudo tc qdisc add dev $IFACE ingress

for ip in "${ALL_IPS[@]}"; do
    if [[ "$ip" == "$MY_IP" ]]; then
        echo "Skipping self IP $ip for ingress"
        continue
    fi
    if [[ ! " ${EXEMPT_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Applying ingress delay for $ip"
        sudo tc filter add dev $IFACE parent ffff: protocol ip prio 1 u32 match ip src $ip \
            action mirred egress redirect dev ifb0
    else
        echo "Bypassing ingress delay for $ip"
    fi
done

# ----------- Set ingress delay on ifb0 -----------

sudo tc qdisc add dev ifb0 root netem delay ${CUR_LATENCY}ms

echo "✅ Done: $CUR_LATENCY ms latency applied for inter-site traffic on $IFACE"
