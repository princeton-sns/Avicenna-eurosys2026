#!/bin/bash

set -x

for i in $(seq 2 8); do
  ssh node-$i -o StrictHostKeyChecking=no "\
  cd /proj/cops-PG0/workspaces/zjqin/avicenna-throughputlatency/tcwan;
  sudo ./add_jitter.sh ${i};
"
done