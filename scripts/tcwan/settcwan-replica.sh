#!/bin/bash
set -x

IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')
if [ -z "$IFACE" ]; then
  echo "❌ Could not find interface with 10.1.1.x IP"
  exit 1
fi
cur_node=$1  # Usage: ./set_latency.sh 0

# ----------- Site membership and latency config -----------

SITE_1_SERVER=(0 1 2)
SITE_1_CLIENT=(9 10)
SITE_2_SERVER=(3)
SITE_2_CLIENT=(11 12)
SITE_3_SERVER=(4)
SITE_3_CLIENT=(13 14)
SITE_4_SERVER=(5)
SITE_4_CLIENT=(15 16)
SITE_5_SERVER=(6)
SITE_5_CLIENT=(17 18)
SITE_6_SERVER=(7)
SITE_6_CLIENT=(19 20)
SITE_7_SERVER=(8)
SITE_7_CLIENT=(21 22)

SERVERS=(0 1 2 3 4 5 6 7 8)
CLIENTS=(9 10 11 12 13 14 15 16 17 18 19 20 21 22)

LATENCY_1=20
LATENCY_2=25
LATENCY_3=30
LATENCY_4=40
LATENCY_5=30
LATENCY_6=25
LATENCY_7=30

ALL_IPS=()
for i in $(seq 0 22); do 
    ALL_IPS+=("10.1.1.$((i+2))")
done

in_arr() { local x=$1; shift; for y in "$@"; do [[ "$y" == "$x" ]] && return 0; done; return 1; }

# ----------- Detect current site -----------

in_site() {
    local node=$1
    shift
    for n in "$@"; do
        if [ "$n" == "$node" ]; then return 0; fi
    done
    return 1
}

if in_site "$cur_node" "${SITE_1_SERVER[@]}"; then
    CUR_SITE=1; 
    CUR_SITE_NODES=("${SITE_1_SERVER[@]}"); 
    CUR_LATENCY=$LATENCY_1;
    CUR_SITE_CLIENTS=("${SITE_1_CLIENT[@]}")
elif in_site "$cur_node" "${SITE_2_SERVER[@]}"; then
    CUR_SITE=2; 
    CUR_SITE_NODES=("${SITE_2_SERVER[@]}"); 
    CUR_LATENCY=$LATENCY_2;
    CUR_SITE_CLIENTS=("${SITE_2_CLIENT[@]}")
elif in_site "$cur_node" "${SITE_3_SERVER[@]}"; then
    CUR_SITE=3; 
    CUR_SITE_NODES=("${SITE_3_SERVER[@]}"); 
    CUR_LATENCY=$LATENCY_3;
    CUR_SITE_CLIENTS=("${SITE_3_CLIENT[@]}")
elif in_site "$cur_node" "${SITE_4_SERVER[@]}"; then
    CUR_SITE=4; 
    CUR_SITE_NODES=("${SITE_4_SERVER[@]}"); 
    CUR_LATENCY=$LATENCY_4;
    CUR_SITE_CLIENTS=("${SITE_4_CLIENT[@]}")
elif in_site "$cur_node" "${SITE_5_SERVER[@]}"; then
    CUR_SITE=5; 
    CUR_SITE_NODES=("${SITE_5_SERVER[@]}"); 
    CUR_LATENCY=$LATENCY_5;
    CUR_SITE_CLIENTS=("${SITE_5_CLIENT[@]}")
elif in_site "$cur_node" "${SITE_6_SERVER[@]}"; then
    CUR_SITE=6; 
    CUR_SITE_NODES=("${SITE_6_SERVER[@]}"); 
    CUR_LATENCY=$LATENCY_6;
    CUR_SITE_CLIENTS=("${SITE_6_CLIENT[@]}")
elif in_site "$cur_node" "${SITE_7_SERVER[@]}"; then
    CUR_SITE=7; 
    CUR_SITE_NODES=("${SITE_7_SERVER[@]}"); 
    CUR_LATENCY=$LATENCY_7;
    CUR_SITE_CLIENTS=("${SITE_7_CLIENT[@]}")
else
    echo "❌ Unknown node $cur_node"
    exit 1
fi

# ----------- Get current node's IP -----------

MY_IP=$(ip -4 addr show dev "$IFACE" | grep -oP '(?<=inet\s)\d+(\.\d+){3}')
echo "🔍 Detected local IP: $MY_IP"

# ----------- Compute exempt IPs (intra-site) -----------

EXEMPT_SERVER_IPS=()
INTERSITE_SERVER_IPS=()
for nid in "${SERVERS[@]}"; do
    ip="10.1.1.$((nid+2))"
    if in_arr "$nid" "${CUR_SITE_NODES[@]}"; then
        if [[ "$ip" != "$MY_IP" ]]; then
            EXEMPT_SERVER_IPS+=("$ip")
        fi
    else
        INTERSITE_SERVER_IPS+=("$ip")
    fi
done

EXEMPT_CLIENT_IPS=()
INTERSITE_CLIENT_IPS=()
for nid in "${CLIENTS[@]}"; do
    ip="10.1.1.$((nid+2))"
    if in_arr "$nid" "${CUR_SITE_CLIENTS[@]}"; then
        EXEMPT_CLIENT_IPS+=("$ip")
    else
        INTERSITE_CLIENT_IPS+=("$ip")
    fi
done

echo "✅ Node $cur_node in Site $CUR_SITE with latency $CUR_LATENCY ms"
echo "   Exempt IPs (other intra-site): ${EXEMPT_SERVER_IPS[*]}"

# ----------- Clear existing tc settings -----------

sudo tc qdisc del dev $IFACE root 2>/dev/null || true
sudo tc qdisc del dev $IFACE ingress 2>/dev/null || true
sudo tc qdisc del dev ifb0 root 2>/dev/null || true
sudo ip link set ifb0 down 2>/dev/null || true
sudo ip link delete ifb0 type ifb 2>/dev/null || true
sudo tc qdisc del dev ifb1 root 2>/dev/null || true
sudo ip link set ifb1 down 2>/dev/null || true
sudo ip link delete ifb1 type ifb 2>/dev/null || true
sudo tc qdisc del dev ifb2 root 2>/dev/null || true
sudo ip link set ifb2 down 2>/dev/null || true
sudo ip link delete ifb2 type ifb 2>/dev/null || true
sudo tc qdisc del dev ifb3 root 2>/dev/null || true
sudo ip link set ifb3 down 2>/dev/null || true
sudo ip link delete ifb3 type ifb 2>/dev/null || true



# ----------- Set up ifb0 and egress qdisc -----------

sudo modprobe ifb
sudo ip link add ifb0 type ifb
sudo ip link set ifb0 up
sudo ip link add ifb1 type ifb
sudo ip link set ifb1 up
sudo ip link add ifb2 type ifb
sudo ip link set ifb2 up
sudo ip link add ifb3 type ifb
sudo ip link set ifb3 up

sudo tc qdisc add dev $IFACE root handle 1: prio bands 4
sudo tc qdisc add dev $IFACE parent 1:1 handle 10: netem delay ${CUR_LATENCY}ms
sudo tc qdisc add dev $IFACE parent 1:2 handle 20: netem delay ${CUR_LATENCY}ms
sudo tc qdisc add dev $IFACE parent 1:3 handle 30: netem delay 0ms
sudo tc qdisc add dev $IFACE parent 1:4 handle 40: netem delay 0ms

# ----------- Add egress filters -----------

for ip in "${ALL_IPS[@]}"; do
    if [[ "$ip" == "$MY_IP" ]]; then
        echo "Skipping self IP $ip for egress"
        continue
    fi

    if [[ " ${EXEMPT_SERVER_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Bypassing egress delay for $ip"
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip dst $ip flowid 1:4
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip src $ip flowid 1:4
    elif [[ " ${EXEMPT_CLIENT_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Bypassing egress delay for $ip"
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip dst $ip flowid 1:3
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip src $ip flowid 1:3
    elif [[ " ${INTERSITE_SERVER_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Applying egress delay for $ip"
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip dst $ip flowid 1:2
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip src $ip flowid 1:2
    elif [[ " ${INTERSITE_CLIENT_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Applying egress delay for $ip"
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip dst $ip flowid 1:1
        sudo tc filter add dev $IFACE protocol ip parent 1:0 prio 1 u32 match ip src $ip flowid 1:1
    else
        echo "❌ Unknown IP $ip"
        exit 1
    fi
done

# ----------- Add ingress redirection filters -----------

sudo tc qdisc add dev $IFACE ingress

for ip in "${ALL_IPS[@]}"; do
    if [[ "$ip" == "$MY_IP" ]]; then
        echo "Skipping self IP $ip for ingress"
        continue
    fi
    if [[ " ${EXEMPT_SERVER_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Applying ingress delay of 0ms for $ip"
        sudo tc filter add dev $IFACE parent ffff: protocol ip prio 1 u32 match ip src $ip \
            action mirred egress redirect dev ifb3
    elif [[ " ${EXEMPT_CLIENT_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Applying ingress delay of 0ms for $ip"
        sudo tc filter add dev $IFACE parent ffff: protocol ip prio 1 u32 match ip src $ip \
            action mirred egress redirect dev ifb2
    elif [[ " ${INTERSITE_SERVER_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Applying ingress delay of ${CUR_LATENCY}ms for $ip"
        sudo tc filter add dev $IFACE parent ffff: protocol ip prio 1 u32 match ip src $ip \
            action mirred egress redirect dev ifb1
    elif [[ " ${INTERSITE_CLIENT_IPS[*]} " =~ (^|[[:space:]])${ip}($|[[:space:]]) ]]; then
        echo "Applying ingress delay of ${CUR_LATENCY}ms for $ip"
        sudo tc filter add dev $IFACE parent ffff: protocol ip prio 1 u32 match ip src $ip \
            action mirred egress redirect dev ifb0
    else
        echo "❌ Unknown IP $ip"
        exit 1
    fi
done

# ----------- Set ingress delay on ifb0 -----------

sudo tc qdisc add dev ifb0 root netem delay ${CUR_LATENCY}ms
sudo tc qdisc add dev ifb1 root netem delay ${CUR_LATENCY}ms
sudo tc qdisc add dev ifb2 root netem delay 0ms
sudo tc qdisc add dev ifb3 root netem delay 0ms

echo "✅ Done: $CUR_LATENCY ms latency applied for inter-site traffic on $IFACE"
