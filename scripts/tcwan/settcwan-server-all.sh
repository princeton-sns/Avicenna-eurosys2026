#!/bin/bash

set -x

for i in $(seq 1 8); do
  ssh node-$i -o StrictHostKeyChecking=no "\
  cd /proj/cops-PG0/workspaces/zjqin/avicenna-throughputlatency/scripts/tcwan;
  sudo ./settcwan-server.sh ${i};
"
done