#!/bin/bash

exitfn() {
  trap - INT
  $(pwd)/experimentscripts/killall.sh || true
  sleep 1
  declare -a killArr
  for i in `seq 1 $totalNodes`; do
    echo "killing $i"
    ssh -o StrictHostKeyChecking=no node-$i "cd $prefix; $(pwd)/experimentscripts/killall.sh" &
    killArr="$killArr $!"
  done
  # -------- aggregate (use totalClients across shards) ----------
  totalClients=$((clients * shards))
  mv *.prof "${outputDir}" 2>/dev/null || true
  cd "${outputDir}"
  awkScriptPath="${prefix}/scripts/median.awk"
  latencyScriptPath="${prefix}/scripts/latency.bash"
  : > clients.tputlat.txt
  for i in $(seq 0 $((totalClients - 1))); do
    [ -f "client-$i.tputlat.txt" ] && cat "client-$i.tputlat.txt" >> clients.tputlat.txt
  done
  awk -F"\t" '{sum+=$1;}END{print sum;}' clients.tputlat.txt > tput.txt
  clients=$totalClients
  source "${latencyScriptPath}" || true
  echo -e "$(cat tput.txt)\t$(awk 'BEGIN { ORS = "\t" } { print $2 }' percentilesnew.txt)" | tee tputlat.txt
  cd "$prefix"
  python scripts/tput.py "$totalClients" "${tput_interval_in_sec}" "${outputDir}" || true
  exit 0
}
trap "exitfn" INT

set -x

proto=$1
n=$2
clients=$3
cnodes=$4
length=$5
percent_mocked=$6
append=$7
slowduration=$8
shards=${9:-1}
skewness=${10}

# ==== defaults (unchanged) ====
reqs=1000000
exec="true"
reply="true"
durable="false"
check="true"
prefix=$(pwd)
cpuprofile=""
verbose="true"
numkeys=100000
trim="0.25"
writes=100
conflicts=-1
proxyReplica=-1
thrifty="false"
once="false"
tput_interval_in_sec="1.0"
target_rps="2000"

doEpaxos="false"
doTwoLeaders="false"
doCopilot="false"
doLatentCopilot="false"
doMr99rsm="false"
doFvc="false"
if [ "$proto" == "copilot" ]; then
  doTwoLeaders="true"; doCopilot="true"
elif [ "$proto" == "latentcopilot" ]; then
  doTwoLeaders="true"; doLatentCopilot="true"
elif [ "$proto" == "epaxos" ]; then
  doEpaxos="true"
elif [ "$proto" == "avicenna" ]; then
  doAvicenna="true"
elif [ "$proto" == "fvc" ]; then
  doFvc="true"
fi

# threads per client node
threads=$((clients / cnodes))
leftover=$((clients % cnodes))

# Per-shard server files -> use an array (fixes ${serverFile${s}})
serverFile1="$(pwd)/experimentscripts/configs/serverconfig1_5r.txt"
serverFile2="$(pwd)/experimentscripts/configs/serverconfig2_5r.txt"
serverFile3="$(pwd)/experimentscripts/configs/serverconfig3_5r.txt"
serverFile4="$(pwd)/experimentscripts/configs/serverconfig4_5r.txt"
serverFile5="$(pwd)/experimentscripts/configs/serverconfig5_5r.txt"
SERVER_FILES=("$serverFile1" "$serverFile2" "$serverFile3" "$serverFile4" "$serverFile5" "$serverFile6" "$serverFile7")

# Cluster layout
masterAddr="node-1"
masterPortBase=7017
serverPortBase=7010
portStep=10               # shard k uses base + (k-1)*portStep

exp_uid="${append}_$(date +%s)"

# One shared output dir
outputDir="${prefix}/experiments/${exp_uid}/"

# Use ONE consistent remote dir/user for logs
logOutputDir="/users/qingjian/experiments/${exp_uid}"

make
mkdir -p "${outputDir}"
rm -f "${prefix}/latest"
ln -s "${outputDir}" "${prefix}/latest"

echo -e "Running ${proto} at $(date)\t${exp_uid}\tN=${n}\tclients=${clients}\tcnodes=${cnodes}\tshards=${shards}" >> "${prefix}/progress"

# ==== Cleanup ====
totalNodes=$((n + cnodes + 1))
pids=""
for i in $(seq 1 $totalNodes); do
  (ssh -o StrictHostKeyChecking=no node-$i "cd '$prefix'; $(pwd)/experimentscripts/killall.sh || true") &
  pids="$pids $!"
done
for pid in $pids; do wait $pid; done

# ==== Start Masters (one per shard) ====
for s in $(seq 1 $shards); do
  mport=$((masterPortBase + (s-1)*portStep))
  sf="${SERVER_FILES[$((s-1))]:-}"
  ssh -o StrictHostKeyChecking=no node-1 "\
    mkdir -p '${logOutputDir}'; \
    cd '$prefix'; \
    bin/master -N='${n}' -fvc='${doFvc}' -twoLeaders='${doTwoLeaders}' -port='${mport}' ${sf:+-f=${sf}} \
    > ${logOutputDir}/master-shard${s}.txt 2>&1" &
done
sleep 2

# ==== Start Servers (each server per shard, different ports) ====
for i in $(seq 2 $((n + 1))); do
  for s in $(seq 1 $shards); do
    mport=$((masterPortBase + (s-1)*portStep))
    sport=$((serverPortBase + (s-1)*portStep))
    ssh -o StrictHostKeyChecking=no node-$i "\
      mkdir -p '${logOutputDir}'; \
      cd '$prefix'; \
      bin/server -maddr='${masterAddr}' -mport='${mport}' -addr='node-$i' \
        -port='${sport}' -e='${doEpaxos}' -copilot='${doCopilot}' -latentcopilot='${doLatentCopilot}' -doavicenna='${doAvicenna}' -dofvc='${doFvc}' \
        -slowdownduration='${slowduration}' -exec='${exec}' -dreply='${reply}' -durable='${durable}' -p=20 -thrifty='${thrifty}' \
      > ${logOutputDir}/server${i}-shard${s}.txt 2>&1" &
    sleep 1
  done
done

sleep 5

# ==== Start Clients (global-unique IDs across shards) ====
unset pids
offset=$((n + 2))
clientLocalStart=0

for i in $(seq 0 $((cnodes - 1))); do
  nodeId=$((i + offset))
  processes_per_node=${threads}
  if [ "$i" -lt "$leftover" ]; then
    processes_per_node=$((threads + 1))
  fi

  for s in $(seq 1 $shards); do
    mport=$((masterPortBase + (s-1)*portStep))
    start_id=$((clientLocalStart + (s-1)*clients))
    end_id=$((start_id + processes_per_node - 1))
    if [ "$proto" == "epaxos" ] && [ "$proxyReplica" -ge 0 ]; then
      proxyReplica=$((i % n))
    fi
    ssh -o StrictHostKeyChecking=no node-${nodeId} "\
      cd '$prefix'; \
      bash $(pwd)/experimentscripts/startClients-test.sh '-maddr=${masterAddr} -mport=${mport} -q=${reqs} -check=true \
        -e=${doEpaxos} -twoLeaders=${doTwoLeaders} -doavicenna=${doAvicenna} -numKeys=${numkeys} \
        -c=${conflicts} -prefix=${outputDir} -runtime=${length} -s=${skewness}\
        -trim=${trim} -w=${writes} -proxy=${proxyReplica} -p=20 -tput_interval_in_sec=${tput_interval_in_sec} \
        -percent_mocked=${percent_mocked}' \
        ${nodeId} ${exp_uid} ${start_id} ${end_id} ${append}" &
    pids="$pids $!"
  done
  clientLocalStart=$((clientLocalStart + processes_per_node))
done

echo "Started all nodes waiting..."
for pid in $pids; do
  wait $pid
  echo "$pid done"
done

# ==== Cleanup ====
for i in $(seq 1 $totalNodes); do
  ssh -o StrictHostKeyChecking=no node-$i "cd '$prefix'; $(pwd)/experimentscripts/killall.sh || true"
done

####################
# Aggregate (single-shard logic, totalClients = clients*shards)
mv *.prof "${outputDir}" 2>/dev/null || true
cd "${outputDir}"
awkScriptPath="${prefix}/scripts/median.awk"
latencyScriptPath="${prefix}/scripts/latency.bash"

totalClients=$((clients * shards))
: > clients.tputlat.txt
for i in $(seq 0 $((totalClients - 1))); do
  [ -f "client-$i.tputlat.txt" ] && cat "client-$i.tputlat.txt" >> clients.tputlat.txt
done
awk -F"\t" '{sum+=$1;} END{print sum;}' clients.tputlat.txt > tput.txt
clients=$totalClients
source "${latencyScriptPath}" || true
echo -e "$(cat tput.txt)\t$(awk 'BEGIN { ORS = "\t" } { print $2 }' percentilesnew.txt)" | tee tputlat.txt
cd "$prefix"
python scripts/tput.py "$totalClients" "${tput_interval_in_sec}" "${outputDir}" || true

####################
# FINAL STEP: pull logs/results to node-0 and clean up
final_dir="/users/qingjian/experiments/${exp_uid}"
mkdir -p "$final_dir"
echo "Copying logs back to node-0..."

# 1) Masters
for s in $(seq 1 $shards); do
  scp node-1:${logOutputDir}/master-shard${s}.txt "$final_dir/" 2>/dev/null || true
done

# 2) Servers (match filenames with -shard suffix)
for i in $(seq 2 $((n + 1))); do
  for s in $(seq 1 $shards); do
    scp node-$i:${logOutputDir}/server${i}-shard${s}.txt "$final_dir/" 2>/dev/null || true
  done
  ssh -o StrictHostKeyChecking=no node-$i "rm -rf '${logOutputDir}' ~/experiments" 2>/dev/null || true
done

# 3) Clients (optional *.out)
for i in $(seq 0 $((cnodes - 1))); do
  nodeId=$(($i + offset))
  scp node-$nodeId:${logOutputDir}/*.out "$final_dir/" 2>/dev/null
  ssh node-$nodeId "rm -rf ~/experiments"
done

# 4) Copy summaries from shared outputDir
scp ${outputDir}/tput.txt "$final_dir/" 2>/dev/null
scp ${outputDir}/percentilesnew.txt "$final_dir/" 2>/dev/null
scp ${outputDir}/sys_tput.txt "$final_dir/" 2>/dev/null
# cp -f "${outputDir}/"*.txt "$final_dir/" 2>/dev/null || true
rm -rf experiments 2>/dev/null || true

echo "All logs collected into: $final_dir"
echo "Cleaned up temporary logs from all nodes"
