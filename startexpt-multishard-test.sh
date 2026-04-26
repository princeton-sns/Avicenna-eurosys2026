#!/bin/bash
set -euo pipefail

########################################
# Graceful interrupt: kill, aggregate combined results, pull logs
exitfn() {
  trap - INT
  ./killall.sh || true
  sleep 1

  # Kill on all nodes
  declare -a killPids=()
  for i in $(seq 1 "${totalNodes}"); do
    (ssh -o StrictHostKeyChecking=no "node-$i" "cd '$prefix'; ./killall.sh || true") &
    killPids+=("$!")
  done
  for pid in "${killPids[@]}"; do wait "$pid" || true; done

  # ---- Combine all shards (if any output exists) ----
  combined_ts=$(date +%s)
  combinedDir="${prefix}/experiments/${combined_ts}_combined_${append:-combined}"
  mkdir -p "${combinedDir}"

  mv *.prof "${combinedDir}/" 2>/dev/null || true

  : > "${combinedDir}/clients.tputlat.txt"
  for d in "${SHARD_DIRS[@]}"; do
    if compgen -G "${d}/client-*.tputlat.txt" > /dev/null; then
      cat "${d}/client-*.tputlat.txt" >> "${combinedDir}/clients.tputlat.txt"
    fi
  done

  if [ -s "${combinedDir}/clients.tputlat.txt" ]; then
    awk -F'\t' '{sum+=$1} END{print sum;}' "${combinedDir}/clients.tputlat.txt" > "${combinedDir}/tput.txt"
    (
      cd "${combinedDir}"
      awkScriptPath="${prefix}/scripts/median.awk"
      latencyScriptPath="${prefix}/scripts/latency.bash"
      source "${latencyScriptPath}"
      echo -e "$(cat tput.txt)\t$(awk 'BEGIN { ORS = "\t" } { print $2 }' percentilesnew.txt)" | tee tputlat.txt
    )
    totalClients=$(( clients * ${#SHARD_DIRS[@]} ))
    python scripts/tput.py "${totalClients}" "${tput_interval_in_sec}" "${combinedDir}" || true
  else
    echo "WARN: no client tput/latency files found; skipping aggregation" >&2
  fi

  # Pull logs back
  final_dir="/users/qingjian/experiments/${ts}_${append}_ALL"
  mkdir -p "$final_dir"
  scp -q "node-1:${logOutputDir1}/master.txt" "$final_dir/master-shard1.txt" 2>/dev/null || true
  scp -q "node-1:${logOutputDir2}/master.txt" "$final_dir/master-shard2.txt" 2>/dev/null || true
  for i in $(seq 2 $((n + 1))); do
    scp -q "node-$i:${logOutputDir1}/server-$i.txt" "$final_dir/server-$i-shard1.txt" 2>/dev/null || true
    scp -q "node-$i:${logOutputDir2}/server-$i.txt" "$final_dir/server-$i-shard2.txt" 2>/dev/null || true
  done
  echo "All logs collected into: $final_dir"

  # Cleanup remote scratch
  ssh -o StrictHostKeyChecking=no node-1 "rm -rf '${logOutputDir1}' '${logOutputDir2}'" || true
  for i in $(seq 2 $((n + 1))); do
    ssh -o StrictHostKeyChecking=no "node-$i" "rm -rf '${logOutputDir1}' '${logOutputDir2}'" || true
  done
  exit 0
}
trap "exitfn" INT

set -x

proto="${1:-multipaxos}"
n="${2:-5}"
clients="${3:-64}"
cnodes="${4:-4}"
length="${5:-120}"
percent_mocked="${6:-0}"
append="${7:-run}"
slowduration="${8:-0}"

# Paths & options
prefix="$(pwd)"
reqs=1000000
exec_opt="true"
reply="true"
durable="false"
check="true"
numkeys=100000
trim="0.25"
writes=100
conflicts=25
proxyReplica=0
thrifty="false"
tput_interval_in_sec="1.0"
target_rps="2000"

# Protocol switches
doEpaxos="false"; doTwoLeaders="false"; doCopilot="false"; doLatentCopilot="false"; doMr99rsm="false"; doFvc="false"
case "$proto" in
  copilot)       doTwoLeaders="true"; doCopilot="true" ;;
  latentcopilot) doTwoLeaders="true"; doLatentCopilot="true" ;;
  epaxos)        doEpaxos="true" ;;
  mr99rsm)       doMr99rsm="true" ;;
  fvc)           doFvc="true" ;;
  *)             : ;; # multipaxos default
esac

# Cluster layout
masterAddr="node-1"
# Shard 1
masterPort1="7017"; serverPort1="7010"
# Shard 2
masterPort2="7027"; serverPort2="7020"

# Server files (per shard)
serverFile1="/proj/cops/zijian/avicenna-throughputlatency/serverconfig1.txt"
serverFile2="/proj/cops/zijian/avicenna-throughputlatency/serverconfig2.txt"

# Per-shard experiment IDs and dirs
ts=$(date +%s)
exp_uid1="${ts}_shard1_${append}"
exp_uid2="${ts}_shard2_${append}"

outputDir1="${prefix}/experiments/${exp_uid1}"
outputDir2="${prefix}/experiments/${exp_uid2}"

logOutputDir1="/users/qingjian/experiments/${exp_uid1}"
logOutputDir2="/users/qingjian/experiments/${exp_uid2}"

mkdir -p "${outputDir1}" "${outputDir2}"

# SHARD_DIRS for later aggregation/trap
SHARD_DIRS=("${outputDir1}" "${outputDir2}")

echo -e "Running ${proto} at $(date)\t${exp_uid1}/${exp_uid2}\t${n}\t${clients}\t${cnodes}" >> "${prefix}/progress"

# Derive threads per client node
threads=$((clients / cnodes))
leftover=$((clients % cnodes))

########################################
# Cleanup on all nodes before start
totalNodes=$((n + cnodes + 1)) # master + servers (n) + client nodes (cnodes)
pids=""
for i in $(seq 1 $totalNodes); do
  (ssh -o StrictHostKeyChecking=no "node-$i" "cd '$prefix'; ./killall.sh || true") &
  pids="$pids $!"
done
for pid in $pids; do wait "$pid"; done

########################################
# Port wait helper (nc or /dev/tcp)
wait_for_port() {
  local host="$1" port="$2" tries="${3:-80}"
  if command -v nc >/dev/null 2>&1; then
    for _ in $(seq 1 "$tries"); do
      nc -z "$host" "$port" 2>/dev/null && return 0
      sleep 0.25
    done
  else
    for _ in $(seq 1 "$tries"); do
      (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1 && return 0
      sleep 0.25
    done
  fi
  echo "ERROR: $host:$port not reachable after $tries tries" >&2
  return 1
}

########################################
# Start Masters (two shards on node-1)
ssh -o StrictHostKeyChecking=no "${masterAddr}" "\
  set -e; mkdir -p '${logOutputDir1}'; cd '$prefix'; \
  nohup bin/master \
    -N=${n} \
    -fvc=${doFvc} \
    -f='${serverFile1}' \
    -port=${masterPort1} \
    -twoLeaders=${doTwoLeaders} \
    > '${logOutputDir1}/master.txt' 2>&1 < /dev/null & disown" &

ssh -o StrictHostKeyChecking=no "${masterAddr}" "\
  set -e; mkdir -p '${logOutputDir2}'; cd '$prefix'; \
  nohup bin/master \
    -N=${n} \
    -fvc=${doFvc} \
    -f='${serverFile2}' \
    -port=${masterPort2} \
    -twoLeaders=${doTwoLeaders} \
    > '${logOutputDir2}/master.txt' 2>&1 < /dev/null & disown" &

sleep 2
wait_for_port "${masterAddr}" "${masterPort1}"
wait_for_port "${masterAddr}" "${masterPort2}"

########################################
# Start Servers (same physical nodes for both shards, different ports)
for i in $(seq 2 $((n + 1))); do
  # Shard 1
  ssh -o StrictHostKeyChecking=no "node-$i" "\
    set -e; mkdir -p '${logOutputDir1}'; cd '$prefix'; \
    nohup bin/server \
      -maddr='${masterAddr}' -mport='${masterPort1}' -addr='node-$i' -port='${serverPort1}' \
      -e='${doEpaxos}' -copilot='${doCopilot}' -latentcopilot='${doLatentCopilot}' -domr99rsm='${doMr99rsm}' -dofvc='${doFvc}' \
      -slowdownduration='${slowduration}' -exec='${exec_opt}' -dreply='${reply}' -durable='${durable}' -p=20 -thrifty='${thrifty}' \
      > '${logOutputDir1}/server-$i.txt' 2>&1 < /dev/null & disown" &
  # Shard 2
  ssh -o StrictHostKeyChecking=no "node-$i" "\
    set -e; mkdir -p '${logOutputDir2}'; cd '$prefix'; \
    nohup bin/server \
      -maddr='${masterAddr}' -mport='${masterPort2}' -addr='node-$i' -port='${serverPort2}' \
      -e='${doEpaxos}' -copilot='${doCopilot}' -latentcopilot='${doLatentCopilot}' -domr99rsm='${doMr99rsm}' -dofvc='${doFvc}' \
      -slowdownduration='${slowduration}' -exec='${exec_opt}' -dreply='${reply}' -durable='${durable}' -p=20 -thrifty='${thrifty}' \
      > '${logOutputDir2}/server-$i.txt' 2>&1 < /dev/null & disown" &
  sleep 0.5
done

sleep 3

########################################
# Clients: distinct IDs per shard (no overlap)
offset=$((n + 2))        # first client node id
clientId=0               # starting id range for the first client machine
threads=$((clients / cnodes))
leftover=$((clients % cnodes))

clients_per_shard="${clients}"
id_base_shard1=0
id_base_shard2="${clients_per_shard}"

pids=""
for i in $(seq 0 $((cnodes - 1))); do
  nodeId=$((i + offset))
  processes_per_node=$threads
  if [ "$i" -lt "$leftover" ]; then
    processes_per_node=$((threads + 1))
  fi

  start_id=$clientId
  end_id=$((clientId + processes_per_node - 1))

  s1_start=$((start_id + id_base_shard1))
  s1_end=$((end_id + id_base_shard1))
  s2_start=$((start_id + id_base_shard2))
  s2_end=$((end_id + id_base_shard2))

  if [ "$proto" = "epaxos" ] && [ "$proxyReplica" -ge 0 ]; then
    proxyReplica=$((start_id % n))
  fi

  # Shard 1
  ssh -o StrictHostKeyChecking=no "node-${nodeId}" "\
    cd '$prefix'; \
    bash startClients-test.sh \
      '-maddr=${masterAddr} -mport=${masterPort1} -q=${reqs} -check=true \
       -e=${doEpaxos} -twoLeaders=${doTwoLeaders} -domr99rsm=${doMr99rsm} -numKeys=${numkeys} \
       -c=${conflicts} -prefix=${outputDir1} -runtime=${length} \
       -trim=${trim} -w=${writes} -p=20 -tput_interval_in_sec=${tput_interval_in_sec} \
       -percent_mocked=${percent_mocked}' \
      ${nodeId} ${exp_uid1} ${s1_start} ${s1_end} ${append}" &
  pids="$pids $!"

  # Shard 2
  ssh -o StrictHostKeyChecking=no "node-${nodeId}" "\
    cd '$prefix'; \
    bash startClients-test.sh \
      '-maddr=${masterAddr} -mport=${masterPort2} -q=${reqs} -check=true \
       -e=${doEpaxos} -twoLeaders=${doTwoLeaders} -domr99rsm=${doMr99rsm} -numKeys=${numkeys} \
       -c=${conflicts} -prefix=${outputDir2} -runtime=${length} \
       -trim=${trim} -w=${writes} -p=20 -tput_interval_in_sec=${tput_interval_in_sec} \
       -percent_mocked=${percent_mocked}' \
      ${nodeId} ${exp_uid2} ${s2_start} ${s2_end} ${append}" &
  pids="$pids $!"

  clientId=$((end_id + 1))
done

echo "Started all client processes; waiting..."
for pid in $pids; do wait "$pid"; done
sleep 2

final_dir="/users/qingjian/experiments/${ts}_${append}_ALL"
mkdir -p "${final_dir}"

########################################
# Cleanup runtime processes on all nodes
for i in $(seq 1 $totalNodes); do
  ssh -o StrictHostKeyChecking=no "node-$i" "cd '$prefix'; ./killall.sh || true"
done

####################
# put all throughput latency in one place for easy plotting
mv *.prof ${final_dir}
cd ${final_dir}
awkScriptPath="${prefix}/scripts/median.awk"
latencyScriptPath="${prefix}/scripts/latency.bash"
for i in $(seq 0 $((clients - 1))); do
  cat client-$i.tputlat.txt >>clients.tputlat.txt
done
# sum throughput of all clients
awk -F"\t" '{sum+=$1;}END{print sum;}' clients.tputlat.txt >tput.txt

source ${latencyScriptPath}

echo -e "$(cat tput.txt)\t$(awk 'BEGIN { ORS = "\t" } { print $2 }' percentilesnew.txt)" | tee tputlat.txt

cd $prefix
python scripts/tput.py $clients ${tput_interval_in_sec} ${final_dir}

# ####################
# # MULTI-SHARD: aggregate & copy (minimal change version)

# # 0) List your shard output/log dirs here (add more for more shards)
# SHARD_OUTPUT_DIRS=("${outputDir1}" "${outputDir2}")
# SHARD_LOG_DIRS=("${logOutputDir1}" "${logOutputDir2}")

# # 1) Aggregate per-shard (exactly like your single-shard code)
# for odir in "${SHARD_OUTPUT_DIRS[@]}"; do
#   mv *.prof "${odir}/" 2>/dev/null || true
#   cd "${odir}"
#   awkScriptPath="${prefix}/scripts/median.awk"
#   latencyScriptPath="${prefix}/scripts/latency.bash"

#   : > clients.tputlat.txt
#   # NOTE: unquoted glob so it expands
#   for f in client-*.tputlat.txt; do
#     [ -f "$f" ] || continue
#     cat "$f" >> clients.tputlat.txt
#   done

#   # sum throughput of all clients in THIS shard
#   if [ -s clients.tputlat.txt ]; then
#     awk -F'\t' '{sum+=$1;} END{print sum;}' clients.tputlat.txt > tput.txt
#   else
#     echo "WARN: no client-*.tputlat.txt found in ${odir}" >&2
#     : > tput.txt
#   fi

#   # run your latency script within the shard dir
#   # (your latency.bash expects client-*.latency.all.txt and $clients)
#   source "${latencyScriptPath}" || true

#   echo -e "$(cat tput.txt)\t$(awk 'BEGIN { ORS = "\t" } { print $2 }' percentilesnew.txt)" | tee tputlat.txt
# done

# # 2) Build a COMBINED result (tiny extension of your single-shard logic)
# combinedDir="${prefix}/experiments/${ts}_combined_${append}"
# mkdir -p "${combinedDir}"

# # combine clients.tputlat from all shards
# : > "${combinedDir}/clients.tputlat.txt"
# for odir in "${SHARD_OUTPUT_DIRS[@]}"; do
#   if [ -f "${odir}/clients.tputlat.txt" ]; then
#     cat "${odir}/clients.tputlat.txt" >> "${combinedDir}/clients.tputlat.txt"
#   fi
# done

# # total throughput across all shards
# if [ -s "${combinedDir}/clients.tputlat.txt" ]; then
#   awk -F'\t' '{sum+=$1;} END{print sum;}' "${combinedDir}/clients.tputlat.txt" > "${combinedDir}/tput.txt"
# else
#   echo "WARN: combined clients.tputlat.txt is empty" >&2
#   : > "${combinedDir}/tput.txt"
# fi

# # combine raw per-client latencies and run your latency pipeline once
# # (we just cat everything; your latency.bash will sort & percentile)
# (
#   cd "${combinedDir}"
#   : > alllatencies.txt
#   for odir in "${SHARD_OUTPUT_DIRS[@]}"; do
#     for f in "${odir}"/client-*.latency.all.txt; do
#       [ -f "$f" ] || continue
#       cat "$f" >> alllatencies.txt
#     done
#   done

#   if [ -s alllatencies.txt ]; then
#     awkScriptPath="${prefix}/scripts/median.awk"
#     latencyScriptPath="${prefix}/scripts/latency.bash"
#     # Run your script in a subshell so a missing rm inside doesn't abort the parent
#     bash "${latencyScriptPath}"
#     echo -e "$(cat tput.txt 2>/dev/null || echo 0)\t$(awk 'BEGIN { ORS = "\t" } { print $2 }' percentilesnew.txt)" | tee tputlat.txt
#   else
#     echo "WARN: no client-*.latency.all.txt found across shards" >&2
#     : > percentilesnew.txt
#     : > tputlat.txt
#   fi
# )

# # 3) Python post-processing (use total clients = clients * numShards)
# totalClients=$(( clients * ${#SHARD_OUTPUT_DIRS[@]} ))
# python scripts/tput.py "${totalClients}" "${tput_interval_in_sec}" "${combinedDir}" || true

####################
# FINAL STEP: pull all remote logs/results to node-0 and clean up
echo "Copying logs back to node-0..."

# 1) Master logs from node-1
scp -q "node-1:${SHARD_LOG_DIRS[0]}/master.txt" "${final_dir}/master-shard1.txt" 2>/dev/null || true
scp -q "node-1:${SHARD_LOG_DIRS[1]}/master.txt" "${final_dir}/master-shard2.txt" 2>/dev/null || true

# 2) Server logs from node-2..node-(n+1)
for i in $(seq 2 $((n + 1))); do
  scp -q "node-$i:${SHARD_LOG_DIRS[0]}/server-$i.txt" "${final_dir}/server-$i-shard1.txt" 2>/dev/null || true
  scp -q "node-$i:${SHARD_LOG_DIRS[1]}/server-$i.txt" "${final_dir}/server-$i-shard2.txt" 2>/dev/null || true
  ssh -o StrictHostKeyChecking=no "node-$i" "rm -rf '${SHARD_LOG_DIRS[0]}' '${SHARD_LOG_DIRS[1]}'" 2>/dev/null || true
done

# 3) Client logs from client nodes
offset=$((n + 2))
for i in $(seq 0 $((cnodes - 1))); do
  nodeId=$((i + offset))
  scp -q "node-$nodeId:${SHARD_LOG_DIRS[0]}/client-*.out" "${final_dir}/" 2>/dev/null || true
  scp -q "node-$nodeId:${SHARD_LOG_DIRS[1]}/client-*.out" "${final_dir}/" 2>/dev/null || true
  ssh -o StrictHostKeyChecking=no "node-$nodeId" "rm -rf '${SHARD_LOG_DIRS[0]}' '${SHARD_LOG_DIRS[1]}'" 2>/dev/null || true
done

# 4) Copy per-shard summaries + combined summaries to final_dir
for odir in "${SHARD_OUTPUT_DIRS[@]}"; do
  scp -q "${odir}/"*.txt "${final_dir}/" 2>/dev/null || true
done
scp -q "${combinedDir}/"*.txt "${final_dir}/" 2>/dev/null || true

# 5) Optional: remove local scratch
rm -rf experiments 2>/dev/null || true

echo "All logs collected into: ${final_dir}"
echo "Cleaned up temporary logs from all nodes"
