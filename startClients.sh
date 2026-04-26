set -x
args=$1
echo args $args 
nodeId=$2
echo nodeId $2
outputDir=$3
echo outputDir $outputDir

echo 4 5 $4 $5

for cid in `seq $4 $5`; do
bin/clientmain $args -id=$cid \
      > "${outputDir}/client-$cid.txt 2>&1" &
      # 2>&1 > $outputDir/client-$cid.out &
      # 2>&1 | awk '{ print "Client-'$cid'(node-'$nodeId'): "$0 }' > $outputDir/client-$cid.out &
# bin/clientmain $args -id=$cid \
pid=$!
pids="$pids $pid"
done

echo start clients started pids: $pids
for pid in $pids; do
  wait $pid
  echo "startClients: $pid done"
done