scp $1@fs.emulab.net:/proj/sequencer/slowdowns/copilot/experiments/latest/percentilesnew.txt percentilesnew
python3 plot-points.py -f percentilesnew -s
#rm plot-latest
open percentilesnew.png