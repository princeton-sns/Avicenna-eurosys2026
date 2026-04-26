#!/bin/bash

exitfn() {
  trap SIGINT
  # Cleanup
  for ip in `cat config`; do
    ssh -t -t -o StrictHostKeyChecking=no slowdown@$ip "\
    mkdir -p $prefix;
    cd $prefix; ./killall.sh" &
  done
  echo replicaIps are $replicaIps 

for ip in $replicaIps; do
  rsync -avz slowdown@$ip:$outputDir* $outputDir&
done

echo clientIps are $clientIps 

for ip in $clientIps; do
  rsync -avz slowdown@$ip:$outputDir* $outputDir&
done
  ####################
  # put all throughput latency in one place for easy plotting
  cd ${outputDir}
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
  python scripts/tput.py $clients ${tput_interval_in_sec} ${outputDir}
  exit
}
trap "exitfn" INT

set -x

# protocol to run: "copilot", "latentcopilot", "epaxos", "multipaxos" (default)
proto=$1
n=$2       # number of replicas
clients=$3 # total number of client threads
cnodes=$4  # total number of client machines
masterIp=$5
slowdownDuration=0
percent_mocked=0
while getopts ":p:n:c:m:i:d:e:g:" opt; do
  case $opt in
    p) proto=$OPTARG
    ;;
    n) n=$OPTARG
    ;;
    c) clients=$OPTARG
    ;;
    m) cnodes=$OPTARG
    ;;
    i) masterIp=$OPTARG
    ;;
    d) slowdownDuration=$OPTARG
    ;;
    e) exp_uid="$OPTARG"
    ;;
    g) percent_mocked=$OPTARG
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac

  case $OPTARG in
    -*) echo "Option $opt needs a valid argument"
    exit 1
    ;;
  esac
done
if [[ $exp_uid == "" ]]; then
  exp_uid=$(date +%s)
fi
echo exp_uid=$exp_uid
# exit 1

reqs=100000 #1000
exec="true"
reply="true"
durable="false"
check="true"
cpus=8
#prefix="/proj/cops/$USER/slowdown" # path to copilot folder
prefix=$(pwd) # path to copilot folder
cpuprofile=""
verbose="true" # unused
numkeys=100000
# length="240" # experiment length
length="50" # experiment length
#length="120" # experiment length
# trim="0.25"
trim="0.25"
writes=100      # percentage of writes
#conflicts=$1   # <0: zipf ; >=0:uniform, conflict x%
#conflicts=0   # <0: zipf ; >=0:uniform, conflict x%
conflicts=25    # <0: zipf ; >=0:uniform, conflict x%
#conflicts=100   # <0: zipf ; >=0:uniform, conflict x%
proxyReplica=0  # for epaxos only
#thrifty="true" # YES thrifty
thrifty="false" # NO thrifty
once="false"
tput_interval_in_sec="1.0"
target_rps="2000" # command arrival rate for open-loop client
randomInterval="27" # roughly 1 RTT between the two pilots

doEpaxos="false"
doTwoLeaders="false"
doCopilot="false"
doLatentCopilot="false"
doMr99rsm="false"
doFvc="false"

if [ "$proto" == "copilot" ]; then
  echo "### Run Copilot protocol ###"
  doTwoLeaders="true"
  doCopilot="true"
elif [ "$proto" == "latentcopilot" ]; then
  echo "### Run Latent Copilot protocol ###"
  doTwoLeaders="true"
  doLatentCopilot="true"
elif [ "$proto" == "epaxos" ]; then
  echo "### Run EPaxos protocol ###"
  doEpaxos="true"
elif [ "$proto" == "mr99rsm" ]; then
  echo "### Run MR99RSM protocol ###"
  doMr99rsm="true"
elif [ "$proto" == "fvc" ]; then
  echo "### Run FVC protocol ###"
  doFvc="true" #defaults to paxos clients
else
  echo "### Run Multi-Paxos protocol ###"
fi

masterAddr=slowdown@$masterIp
masterPort="7087"
serverPort="7070"

# if [[ "$#" -gt 4 ]]; then
#   exp_uid="${exp_uid}"
# fi
outputDir="${prefix}/experiments/${exp_uid}/"
mkdir -p ${outputDir}
rm ${prefix}/latest
ln -s $outputDir ${prefix}/latest

echo -e "Running ${proto} at $(date)\t${exp_uid}\t${n}\t${clients}\t${cnodes}" >>${prefix}/progress

# Cleanup
totalNodes=$((n + cnodes + 1))
for ip in `cat config`; do
  (ssh -t -t -o StrictHostKeyChecking=no slowdown@$ip "\
	cd $prefix; ./killall.sh") &
  pid=$!
  pids="$pids $pid"
done
for pid in $pids; do
  wait $pid
done

iter=0
declare -a pids
declare -a clientIps
declare -a replicaIps
clientId=0
# number of client threads on each client node
threads=$((clients / cnodes))
leftover=$((clients % cnodes))
echo $threads
echo prefix is $prefix
for ip in `cat config`; do
  nodeId=$iter

  if ((iter== 0)); then
    echo starting master at slowdown@$ip
    # Start Master
    ssh slowdown@$ip -o StrictHostKeyChecking=no "\
    mkdir -p $outputDir; \
    cd $prefix; \
    pwd; \
    bin/master \
    -N=$n \
    -twoLeaders=$doTwoLeaders \
    -f=servers \
    -fvc=$doFvc" \
      2>&1 | awk '{ print "Master: "$0 }' > $outputDir/master.txt &
    iter=$((iter+1))
    sleep 2
    continue
  fi

  # Start Server
  if ((iter <= n)); then
    ssh -o StrictHostKeyChecking=no slowdown@$ip "\
      cd $prefix; mkdir -p $outputDir; bin/server -maddr=${masterIp} -mport=${masterPort} \
      -addr=$ip -port=${serverPort} -e=$doEpaxos -copilot=$doCopilot -latentcopilot=$doLatentCopilot \
      -domr99rsm=$doMr99rsm -dofvc=$doFvc -slowdownduration=$slowdownDuration -exec=$exec -dreply=$reply -durable=$durable -p=$cpus -thrifty=$thrifty |& awk '{ print \"Server-$((iter-1)): \"\$0 }' > $outputDir/server-$ip.txt 2>&1" &
          # sleep 2
    replicaIps="$replicaIps $ip"
    iter=$((iter+1))
    if ((iter == n+1)); then
      sleep 15
    fi
    continue
  fi
processes_per_node=${threads}
  if [ $((iter-1-n)) -lt "$leftover" ]; then
    processes_per_node=$((threads + 1))
  fi
  echo processes_per_node
  # Start Clients
  ssh slowdown@$ip -o StrictHostKeyChecking=no "\
    mkdir -p $prefix;
    cd $prefix;
    mkdir -p $outputDir; 
    bash startClients.sh '-maddr=${masterIp} -mport=${masterPort} -q=$reqs -check=true \
      -e=$doEpaxos -twoLeaders=$doTwoLeaders -domr99rsm=$doMr99rsm -numKeys=${numkeys} \
      -c=$conflicts -cpuprofile=${cpuprofile} -prefix=$outputDir -runtime=$length \
      -percent_mocked=${percent_mocked} \
      -trim=${trim} -w=$writes -proxy=$proxyReplica -p=$cpus -tput_interval_in_sec=${tput_interval_in_sec} -random_interval=$randomInterval' \
       $nodeId $outputDir $clientId $((clientId+processes_per_node-1))" &
      # quit
      # sleep .05
  pid=$!
  pids="$pids $pid"

  clientId=$((clientId + processes_per_node))
  clientIps="$clientIps $ip"
  echo totalNodes $totalNodes iter $iter totalNodes-iter-1 $((totalNodes-iter-1))
  if [ $((totalNodes-iter-1)) -eq 0 ]; then
    break
  fi
done

echo Started all nodes waiting...
for pid in $pids; do
  wait $pid
  echo '$pid done'
done

sleep 2


# Cleanup
for ip in `cat config`; do
  ssh -t -t -o StrictHostKeyChecking=no slowdown@$ip "\
    mkdir -p $prefix;
	cd $prefix; ./killall.sh"&
done

echo replicaIps are $replicaIps 

for ip in $replicaIps; do
  rsync -avz slowdown@$ip:$outputDir* $outputDir&
  pids="$pids $!"
done

echo clientIps are $clientIps 

for ip in $clientIps; do
  rsync -avz slowdown@$ip:$outputDir* $outputDir&
  pids="$pids $!"
done

for pid in $pids; do
  wait $pid
  echo '$pid done'
done

####################
# put all throughput latency in one place for easy plotting
cd ${outputDir}
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
python scripts/tput.py $clients ${tput_interval_in_sec} ${outputDir}
