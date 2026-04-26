protos='Avicenna FVC Reactive-Copilot Copilot-PP FVC-250us'
for proto in $protos; do
    mkdir -p $proto/tputlat
    scp chodsdon@fs.emulab.net:/proj/sequencer/slowdowns/avicenna/experiments/$proto/tputlat.txt $proto/tputlat/
    done
