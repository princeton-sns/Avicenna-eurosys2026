#!/bin/bash
set -x

# ------------- Helpers -------------
in_arr() {  # usage: in_arr <needle> "${HAYSTACK[@]}"
  local x="$1"; shift
  for y in "$@"; do [[ "$y" == "$x" ]] && return 0; done
  return 1
}

# ------------- Detect interface -------------
IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.1\.1\./ {print $2; exit}')
if [ -z "$IFACE" ]; then
  echo "❌ Could not find interface with 10.1.1.x IP"
  exit 1
fi

cur_node=$1  # Usage: ./set_latency.sh <node_id>

# ------------- Site membership and latency config -------------

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

# Build ALL_IPS strictly from node lists to avoid unknowns
ALL_IPS=()
for nid in "${SERVERS[@]}" "${CLIENTS[@]}"; do
  ALL_IPS+=("10.1.1.$((nid+2))")
done

# ------------- Determine current site (works for server or client nodes) -------------

CUR_SITE=
CUR_LATENCY=
CUR_SITE_SERVERS=()
CUR_SITE_CLIENTS=()

if in_arr "$cur_node" "${SITE_1_SERVER[@]}" || in_arr "$cur_node" "${SITE_1_CLIENT[@]}"; then
  CUR_SITE=1; CUR_LATENCY=$LATENCY_1; CUR_SITE_SERVERS=("${SITE_1_SERVER[@]}"); CUR_SITE_CLIENTS=("${SITE_1_CLIENT[@]}")
elif in_arr "$cur_node" "${SITE_2_SERVER[@]}" || in_arr "$cur_node" "${SITE_2_CLIENT[@]}"; then
  CUR_SITE=2; CUR_LATENCY=$LATENCY_2; CUR_SITE_SERVERS=("${SITE_2_SERVER[@]}"); CUR_SITE_CLIENTS=("${SITE_2_CLIENT[@]}")
elif in_arr "$cur_node" "${SITE_3_SERVER[@]}" || in_arr "$cur_node" "${SITE_3_CLIENT[@]}"; then
  CUR_SITE=3; CUR_LATENCY=$LATENCY_3; CUR_SITE_SERVERS=("${SITE_3_SERVER[@]}"); CUR_SITE_CLIENTS=("${SITE_3_CLIENT[@]}")
elif in_arr "$cur_node" "${SITE_4_SERVER[@]}" || in_arr "$cur_node" "${SITE_4_CLIENT[@]}"; then
  CUR_SITE=4; CUR_LATENCY=$LATENCY_4; CUR_SITE_SERVERS=("${SITE_4_SERVER[@]}"); CUR_SITE_CLIENTS=("${SITE_4_CLIENT[@]}")
elif in_arr "$cur_node" "${SITE_5_SERVER[@]}" || in_arr "$cur_node" "${SITE_5_CLIENT[@]}"; then
  CUR_SITE=5; CUR_LATENCY=$LATENCY_5; CUR_SITE_SERVERS=("${SITE_5_SERVER[@]}"); CUR_SITE_CLIENTS=("${SITE_5_CLIENT[@]}")
elif in_arr "$cur_node" "${SITE_6_SERVER[@]}" || in_arr "$cur_node" "${SITE_6_CLIENT[@]}"; then
  CUR_SITE=6; CUR_LATENCY=$LATENCY_6; CUR_SITE_SERVERS=("${SITE_6_SERVER[@]}"); CUR_SITE_CLIENTS=("${SITE_6_CLIENT[@]}")
elif in_arr "$cur_node" "${SITE_7_SERVER[@]}" || in_arr "$cur_node" "${SITE_7_CLIENT[@]}"; then
  CUR_SITE=7; CUR_LATENCY=$LATENCY_7; CUR_SITE_SERVERS=("${SITE_7_SERVER[@]}"); CUR_SITE_CLIENTS=("${SITE_7_CLIENT[@]}")
else
  echo "❌ Unknown node $cur_node"
  exit 1
fi

# ------------- Get current node's IP -------------

MY_IP=$(ip -4 addr show dev "$IFACE" | grep -oP '(?<=inet\s)\d+(\.\d+){3}')
echo "🔍 Detected local IP: $MY_IP"
echo "✅ Node $cur_node in Site $CUR_SITE with base inter-site latency ${CUR_LATENCY}ms"

# ------------- Compute classification IP sets -------------

EXEMPT_SERVER_IPS=()      # same-site servers (intra-site)
INTERSITE_SERVER_IPS=()   # other-site servers
for nid in "${SERVERS[@]}"; do
  ip="10.1.1.$((nid+2))"
  if in_arr "$nid" "${CUR_SITE_SERVERS[@]}"; then
    [[ "$ip" != "$MY_IP" ]] && EXEMPT_SERVER_IPS+=("$ip")
  else
    INTERSITE_SERVER_IPS+=("$ip")
  fi
done

EXEMPT_CLIENT_IPS=()      # same-site clients (intra-site)
INTERSITE_CLIENT_IPS=()   # other-site clients
for nid in "${CLIENTS[@]}"; do
  ip="10.1.1.$((nid+2))"
  if in_arr "$nid" "${CUR_SITE_CLIENTS[@]}"; then
    [[ "$ip" != "$MY_IP" ]] && EXEMPT_CLIENT_IPS+=("$ip")
  else
    INTERSITE_CLIENT_IPS+=("$ip")
  fi
done

echo "   Intra-site servers: ${EXEMPT_SERVER_IPS[*]}"
echo "   Intra-site clients: ${EXEMPT_CLIENT_IPS[*]}"

# ------------- Clear existing tc settings -------------

sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
sudo tc qdisc del dev "$IFACE" ingress 2>/dev/null || true

for d in ifb0 ifb1 ifb2 ifb3; do
  sudo tc qdisc del dev "$d" root 2>/dev/null || true
  sudo ip link set "$d" down 2>/dev/null || true
  sudo ip link delete "$d" type ifb 2>/dev/null || true
done

# ------------- Set up ifb devices -------------

sudo modprobe ifb
sudo ip link add ifb0 type ifb
sudo ip link add ifb1 type ifb
sudo ip link add ifb2 type ifb
sudo ip link add ifb3 type ifb
sudo ip link set ifb0 up
sudo ip link set ifb1 up
sudo ip link set ifb2 up
sudo ip link set ifb3 up

# ------------- Egress: prio qdisc (4 bands) with unique handles -------------

sudo tc qdisc add dev "$IFACE" root handle 1: prio bands 4
# Choose mapping: 1:1 inter-site clients, 1:2 inter-site servers, 1:3 intra-site clients, 1:4 intra-site servers
sudo tc qdisc add dev "$IFACE" parent 1:1 handle 10: netem delay ${CUR_LATENCY}ms
sudo tc qdisc add dev "$IFACE" parent 1:2 handle 20: netem delay ${CUR_LATENCY}ms
sudo tc qdisc add dev "$IFACE" parent 1:3 handle 30: netem delay 0ms
sudo tc qdisc add dev "$IFACE" parent 1:4 handle 40: netem delay 0ms

# ------------- Egress filters (explicit priorities) -------------

# Intra-site servers -> 1:4 (prio 2)
for ip in "${EXEMPT_SERVER_IPS[@]}"; do
  [[ "$ip" == "$MY_IP" ]] && continue
  sudo tc filter add dev "$IFACE" protocol ip parent 1:0 prio 2 u32 match ip dst "$ip" flowid 1:4
  sudo tc filter add dev "$IFACE" protocol ip parent 1:0 prio 2 u32 match ip src "$ip" flowid 1:4
done

# Intra-site clients -> 1:3 (prio 2)
for ip in "${EXEMPT_CLIENT_IPS[@]}"; do
  [[ "$ip" == "$MY_IP" ]] && continue
  sudo tc filter add dev "$IFACE" protocol ip parent 1:0 prio 2 u32 match ip dst "$ip" flowid 1:3
  sudo tc filter add dev "$IFACE" protocol ip parent 1:0 prio 2 u32 match ip src "$ip" flowid 1:3
done

# Inter-site servers -> 1:2 (prio 3)
for ip in "${INTERSITE_SERVER_IPS[@]}"; do
  [[ "$ip" == "$MY_IP" ]] && continue
  sudo tc filter add dev "$IFACE" protocol ip parent 1:0 prio 3 u32 match ip dst "$ip" flowid 1:2
  sudo tc filter add dev "$IFACE" protocol ip parent 1:0 prio 3 u32 match ip src "$ip" flowid 1:2
done

# Inter-site clients -> 1:1 (prio 3)
for ip in "${INTERSITE_CLIENT_IPS[@]}"; do
  [[ "$ip" == "$MY_IP" ]] && continue
  sudo tc filter add dev "$IFACE" protocol ip parent 1:0 prio 3 u32 match ip dst "$ip" flowid 1:1
  sudo tc filter add dev "$IFACE" protocol ip parent 1:0 prio 3 u32 match ip src "$ip" flowid 1:1
done

# ------------- Ingress redirection -------------

sudo tc qdisc add dev "$IFACE" ingress

# Inter-site servers -> ifb1 (prio 1)
for ip in "${INTERSITE_SERVER_IPS[@]}"; do
  [[ "$ip" == "$MY_IP" ]] && continue
  sudo tc filter add dev "$IFACE" parent ffff: protocol ip prio 1 u32 match ip src "$ip" \
    action mirred egress redirect dev ifb1
done

# Inter-site clients -> ifb0 (prio 1)
for ip in "${INTERSITE_CLIENT_IPS[@]}"; do
  [[ "$ip" == "$MY_IP" ]] && continue
  sudo tc filter add dev "$IFACE" parent ffff: protocol ip prio 1 u32 match ip src "$ip" \
    action mirred egress redirect dev ifb0
done

# Intra-site servers -> ifb3 (prio 2)
for ip in "${EXEMPT_SERVER_IPS[@]}"; do
  [[ "$ip" == "$MY_IP" ]] && continue
  sudo tc filter add dev "$IFACE" parent ffff: protocol ip prio 2 u32 match ip src "$ip" \
    action mirred egress redirect dev ifb3
done

# Intra-site clients -> ifb2 (prio 2)
for ip in "${EXEMPT_CLIENT_IPS[@]}"; do
  [[ "$ip" == "$MY_IP" ]] && continue
  sudo tc filter add dev "$IFACE" parent ffff: protocol ip prio 2 u32 match ip src "$ip" \
    action mirred egress redirect dev ifb2
done

# ------------- Ingress shaping (match egress classes) -------------

sudo tc qdisc add dev ifb0 root netem delay ${CUR_LATENCY}ms  # inter-site clients
sudo tc qdisc add dev ifb1 root netem delay ${CUR_LATENCY}ms  # inter-site servers
sudo tc qdisc add dev ifb2 root netem delay 0ms               # intra-site clients
sudo tc qdisc add dev ifb3 root netem delay 0ms               # intra-site servers

echo "✅ Done: inter-site latency = ${CUR_LATENCY}ms; intra-site latency = 0ms on ${IFACE}"