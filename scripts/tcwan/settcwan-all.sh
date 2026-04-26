#!/bin/bash

set -x

for i in $(seq 1 8); do
  ssh node-$i -o StrictHostKeyChecking=no "\
  cd /proj/cops/zijian/avicenna-throughputlatency/scripts/tcwan;
  sudo ./settcwan-server.sh ${i};
"
done

for i in $(seq 9 22); do
  ssh node-$i -o StrictHostKeyChecking=no "\
  cd /proj/cops/zijian/avicenna-throughputlatency/scripts/tcwan;
  sudo ./settcwan.sh ${i};
"
done