i=1
for sys in `echo Avicenna Copilot Copilot-PP FVC`; do
# gnuplot -e "filename='avicenna/68-ll/processedLats'" -e "outfilename='avicenna-test'" -e "sysname='Avicenna'" -e "c=1" plot_longlived.gnu
gnuplot -e "filename='$sys/68-ll/processedLats'" -e "outfilename='$sys-68-ll'" -e "sysname='$sys'" -e "c=$i" plot_longlived.gnu
gnuplot -e "filename='$sys/204-ll/processedLats'" -e "outfilename='$sys-204-ll'" -e "sysname='$sys'" -e "c=$i" plot_longlived.gnu
i=$((i+1))
done

# gnuplot -e "filename='Reactive-Copilot/68-ll/processedLats'" -e "outfilename='Reactive-Copilot-68-ll'" -e "sysname='Reactive Copilot'" -e "c=$i" plot_longlived.gnu
# gnuplot -e "filename='Reactive-Copilot/204-ll/processedLats'" -e "outfilename='Reactive-Copilot-204-ll'" -e "sysname='Reactive Copilot'" -e "c=$i" plot_longlived.gnu
gnuplot -e "filename='Reactive-Copilot/68-ll/processedLats'" -e "outfilename='Reactive-Copilot-68-ll'" -e "sysname='Latent Copilot'" -e "c=$i" plot_longlived.gnu
gnuplot -e "filename='Reactive-Copilot/204-ll/processedLats'" -e "outfilename='Reactive-Copilot-204-ll'" -e "sysname='Latent Copilot'" -e "c=$i" plot_longlived.gnu