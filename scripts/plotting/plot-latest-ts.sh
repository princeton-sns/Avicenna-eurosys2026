scp $1@fs.emulab.net:/proj/sequencer/slowdowns/copilot/experiments/latest/client-0.timestamps.orig.txt .
awk '{print NR " " $2}' client-0.timestamps.orig.txt > plot-latest
python3 plot-points.py -f plot-latest
#rm plot-latest
open plot-latest.png