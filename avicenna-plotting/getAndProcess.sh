dir=$1
outdir=$2
sysname=$3
ip=$4

mkdir -p $outdir/$dir
rsync -avz slowdown@$ip:/home/slowdown/mock/experiments/$dir/ $outdir/$dir/
python3 getLatAsUnixOffset.py $outdir/$dir/ $outdir/$dir/processedLats
gnuplot -e "filename='$outdir/$dir/processedLats'" -e "outfilename='$dir'" -e "sysname='$sysname'" plot_transient.gnu