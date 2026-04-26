set -x
args=$1
echo args $args 
nodeId=$2
echo nodeId $2
exp_uid=$3
echo exp_uid $exp_uid

logOutputDir="/users/qingjian/experiments/${exp_uid}"

mkdir -p ${logOutputDir}

echo 4 5 $4 $5

for cid in `seq $4 $5`; do
bin/clientmain $args -id=$cid \
      2>&1 | awk '{ print "Client-'$cid'(node-'$nodeId'): "$0 }' > $logOutputDir/client-$cid.out &
     # 2>&1 | awk '{ print "Client-'$cid'(node-'$nodeId'): "$0 }' &
   
# bin/clientmain $args -id=$cid \
#       2>&1 > $outputDir/client-$cid.out &
pid=$!
pids="$pids $pid"
done

echo start clients started pids: $pids
for pid in $pids; do
  wait $pid
  echo "startClients: $pid done"
done