#!/bin/bash

set -x

for i in $(seq 1 22); do
  ssh node-$i -o StrictHostKeyChecking=no "\
  cd /proj/cops/zijian/avicenna-throughputlatency/tcwan;
  sudo ./settcwan.sh ${i};
"
done