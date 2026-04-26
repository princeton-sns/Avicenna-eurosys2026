# this file makes it easy to add generating plots to an experiment script
# just replace:
#  XVAR, YVAR
# and then append your plot commands
#  e.g.: "thru_1proc/thru.graph" using 1:9 title "Unmodified" with linespoints

set   autoscale # scale axes automatically
unset log       # remove any log-scaling
unset label     # remove any previous labels
set xtic auto   # set xtics automatically
set ytic auto   # set ytics automatically

set print "-"

#upgrades seem to have destroyed the ability for gnuplot to produce pdfcairo output for me for now
#set term postscript eps enhanced color linewidth 2 rounded font "Helvetica,14"
#lset size 0.6, 0.45

# from Brighten Godfrey's Blog
# Note you need gnuplot 4.4 for the pdfcairo terminal.
# This allows you to change fonts, among other features.
set terminal pngcairo font "Helvetica,20" linewidth 4 rounded enhanced
#set grid
set border 3 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
set xtics nomirror
set ytics nomirror

#set log x
#set mxtics 10    # Makes logscale look good

# Line styles: try to pick pleasing colors, rather
# than strictly primary colors or hard-to-see colors
# like gnuplot's default yellow.  Make the lines thick
# so they're easy to see in small plots in papers.
set style line 1 lt rgb "#A00000" lw 1 pt 9
set style line 2 lt rgb "#00A000" lw 1 pt 6
set style line 3 lt rgb "#5060D0" lw 1 pt 2
set style line 4 lt rgb "#F25900" lw 1 pt 1
set style line 5 lt rgb "#00C5C5" lw 1 pt 3
set style line 6 lt rgb "#804000" lw 1 pt 4
##############
set xlabel "Throughput (Kops/sec)" offset 0,0.4
set ylabel "Median Latency (ms)" offset 1,0


set xtics nomirror
set ytics nomirror
#set key top center width 1.5
# set key top left height -1
set key top left height -1 width -2
# set key above right maxrows 4 font ",16"
#set key top left width -0.5 height -0.5
#set key bottom right width 1.5 height 1
set output 'throughput-latency.png'

#set log x
unset log x
# set xr [0:350]
set xr [*:*]
#set xtic 0,5,45
#set xr [7:768]
#set xtic 8,2,1024 offset 0,0.4
#set xtic  (.1, .2, .4, .6, .8, .9, .99) offset 0,0.4

#set log y
#set yr [0:1.5]
#set yr [0:5]
#set ytic 0,1,5
#set ytic 40,2,60
#set yr [0:250]

#set arrow from 40,0to 40,1 nohead lc rgb "red";
#set arrow from 40,0to 40,1 nohead ls 6;
#set arrow from -0.6,37.75 to 0.75,38.5 nohead
#set arrow from -0.6,38.40 to 0.75,39.15 nohead

#plot p-50
plot \
"Avicenna-25/tputlat/medians_tputlat.txt" using ($1/1000):($2/1000) title "Avicenna-25%" with linespoints ls 9, \
"Copilot/tputlat/medians_tputlat.txt" using ($1/1000):($2/1000) title "Copilot" with linespoints ls 1, \
"Copilot-PP/tputlat/medians_tputlat.txt" using ($1/1000):($2/1000) title "Copilot-Ping-Pong" with linespoints ls 2, \
"Avicenna-5/tputlat/medians_tputlat.txt" using ($1/1000):($2/1000) title "Avicenna-5%" with linespoints ls 7, \
"Reactive-Copilot/tputlat/medians_tputlat.txt" using ($1/1000):($2/1000) title "Latent Copilot" with linespoints ls 5, \
"FVC/tputlat/medians_tputlat.txt" using ($1/1000):($2/1000) title "Multi-Paxos-FVC" with linespoints ls 4, \
"Avicenna-0/tputlat/medians_tputlat.txt" using ($1/1000):($2/1000) title "Avicenna-0%" with linespoints ls 3, \

# "Avicenna/a100/tputlat/medians_tputlat.txt" using ($1/1000):($2/1000) title "Avicenna-100%" with linespoints ls 9, \
# "FVC-250us/tputlat/tputlat.txt" using ($1/1000):($51/1000) title "Fast-View-Change-250us" with linespoints ls 6, \
# "paxos/paxosNEW.tputlat/paxosNEW.tputlat.med.txt" using ($2/1000):($52/1000) title "Paxos" with linespoints ls 1, \
# "epaxos100/epaxos100.tputlat/epaxos100.tputlat.txt" using ($2/1000):($52/1000) title "EPaxos-100%" with linespoints ls 3, \
# "epaxos25/epaxos25.tputlat/epaxos25.tputlat.txt" using ($2/1000):($52/1000) title "EPaxos-25%" with linespoints ls 4, \
# "epaxos0/epaxos0.tputlat/epaxos0NEW.tputlat.txt" using ($2/1000):($52/1000) title "EPaxos-0%" with linespoints ls 5, \
# "copilot/copilot.tputlat.improved/copilot.tputlat.improved.txt" using ($2/1000):($52/1000) title "Copilot" with linespoints ls 2, \
# "paxos/paxosNEW.tputlat/paxosNEW.tputlat.med.txt" using ($2/1000):($52/1000) title "Paxos" with linespoints ls 1, \
# #"paxos/paxosNEW.tputlat/paxosNEW.tputlat.txt" using ($2/1000):($52/1000) title "Paxos" with linespoints ls 1, \
# #"copilot/copilotNEW.tputlat/copilotNEW.tputlat.txt" using ($2/1000):($52/1000) title "Copilot" with linespoints ls 2, \
##############
# set xlabel "Throughput (Kops/sec)" offset 0,0.4
# set ylabel "Median Latency (ms)" offset 1,0


# set xtics nomirror
# set ytics nomirror
# #set key top center width 1.5
# set key top right height -1
# #set key top left width -0.5 height -0.5
# #set key bottom right width 1.5 height 1
# set output 'throughput_latency_thrifty_new.pdf'

# #set log x
# unset log x
# #set xr [0:45]
# #set xtic 0,5,45
# #set xr [7:768]
# #set xtic 8,2,1024 offset 0,0.4
# #set xtic  (.1, .2, .4, .6, .8, .9, .99) offset 0,0.4

# #set log y
# set yr [0:2.2]
# set ytic 0,0.2.2
# #set ytic 40,2,60
# #set yr [0:250]

# #set arrow from 40,0to 40,1 nohead lc rgb "red";
# #set arrow from 40,0to 40,1 nohead ls 6;
# #set arrow from -0.6,37.75 to 0.75,38.5 nohead
# #set arrow from -0.6,38.40 to 0.75,39.15 nohead

# #plot p-50
# plot \
# "epaxos100/epaxos100thrifty.tputlat/epaxos100thriftyNEW.tputlat.txt" using ($2/1000):($52/1000) title "EPaxos-100%" with linespoints ls 3, \
# "epaxos25/epaxos25thrifty.tputlat/epaxos25thrifty.tputlat.txt" using ($2/1000):($52/1000) title "EPaxos-25%" with linespoints ls 4, \
# "epaxos0/epaxos0thrifty.tputlat/epaxos0thrifty.tputlat.txt" using ($2/1000):($52/1000) title "EPaxos-0%" with linespoints ls 5, \
# "copilot/copilotthrifty.tputlat.improved/copilotthrifty.tputlat.improved.txt" using ($2/1000):($52/1000) title "Copilot" with linespoints ls 2, \
# "paxos/paxosthrifty.tputlat/paxosthrifty.tputlat.txt" using ($2/1000):($52/1000) title "Paxos" with linespoints ls 1, \
# ##############

