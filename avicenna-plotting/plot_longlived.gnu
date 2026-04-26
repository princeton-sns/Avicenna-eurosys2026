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
#set terminal pdfcairo font "Helvetica,20" linewidth 4 rounded enhanced size 10 inches,3 inches
#set terminal pngcairo font "Helvetica,20" linewidth 4 rounded enhanced
set terminal pdfcairo font "Helvetica,20" linewidth 4 rounded enhanced
#set grid
set border 3 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
set xtics nomirror
set ytics nomirror

set log x
#set mxtics 10    # Makes logscale look good

# Line styles: try to pick pleasing colors, rather
# than strictly primary colors or hard-to-see colors
# like gnuplot's default yellow.  Make the lines thick
# so they're easy to see in small plots in papers.
set style line 1 lt rgb "#A00000" lw 1 pt 1
set style line 2 lt rgb "#00A000" lw 1 pt 6
set style line 3 lt rgb "#5060D0" lw 1 pt 2
set style line 4 lt rgb "#F25900" lw 1 pt 9
set style line 5 lt rgb "#00C5C5" lw 1 pt 3
set style line 6 lt rgb "#804000" lw 1 pt 4
##############
#if (!exists("exp_dir")) exp_dir='notfound'
#output_dir=exp_dir."/"
#set xlabel "Time (Second)" offset 0,0.4
#set ylabel "Command Latency (ms)" offset 1,0
#
#set xtics nomirror
#set ytics nomirror
##set key bottom right width 1.5
#set key top left width 1 height 0
#set output "transient_epaxos_linear.pdf"
#
##set xdata time
##set timefmt "%H:%M:%S"
##set format x "%S"
#unset log x
##set log x
#set xr [0:12]
#set xtics 0,2,14
##set xtic 8,2,1024 offset 0,0.4
##set xtic  (.1, .2, .4, .6, .8, .9, .99) offset 0,0.4
#
#unset log y
#set yr [0:100]
##set ytic 0,2
##set yr [0.1:1000]
##set arrow from 40,0to 40,1 nohead lc rgb "black";
##set arrow from -0.55,-0.1 to -0.45,0.1 nohead ls 4 lc rgb "black";
#
#exp_dir1="./"
#plot for [i=0:0] 'epaxos0/epaxos0.transient.clis30/c'.i.'.txt' using ($1/1000000000):($2/1000) title "EPaxos-0%"  w points ls 5 pt 5 ps 1, \
#for [i=1:29] 'epaxos0/epaxos0.transient.clis30/c'.i.'.txt' using ($1/1000000000):($2/1000) notitle w points ls 5 pt 5 ps 1, \
#for [i=0:0] 'epaxos25/epaxos25.transient.clis80.2/c'.i.'.txt' using ($1/1000000000):($2/1000) title "EPaxos-25%"  w points ls 3 pt 3 ps 1, \
#for [i=1:79] 'epaxos25/epaxos25.transient.clis80.2/c'.i.'.txt' using ($1/1000000000):($2/1000) notitle w points ls 3 pt 3, \
#for [i=0:0] 'epaxos100/epaxos100.transient.clis80/c'.i.'.txt' using ($1/1000000000):($2/1000) title "EPaxos-100%"  w points ls 4 pt 4 ps 1, \
#for [i=1:79] 'epaxos100/epaxos100.transient.clis80/c'.i.'.txt' using ($1/1000000000):($2/1000) notitle w points ls 4 pt 4 ps 1, \
###############
#set xlabel "Time (Second)" offset 0,0.4
#set ylabel "Command Latency (ms)" offset 1,0
#
#set xtics nomirror
#set ytics nomirror
##set key bottom right width 1.5
#set key top left width 1 height 0
#set output "transient_paxos_fvc_linear.pdf"
#
##set xdata time
##set timefmt "%H:%M:%S"
##set format x "%S"
#unset log x
##set log x
#set xr [0:12]
#set xtics 0,2,14
##set xtic 8,2,1024 offset 0,0.4
##set xtic  (.1, .2, .4, .6, .8, .9, .99) offset 0,0.4
#
#unset log y
#set yr [0:100]
##set ytic 0,2
##set yr [0.1:1000]
##set arrow from 40,0to 40,1 nohead lc rgb "black";
##set arrow from -0.55,-0.1 to -0.45,0.1 nohead ls 4 lc rgb "black";
#
#exp_dir1="./"
#plot for [i=0:0] 'paxos/transient/c'.i.'.txt' using ($1/1000000000):($2/1000) title "Paxos"  w points ls 1 pt 1 ps 1, \
#for [i=1:39] 'paxos/transient/c'.i.'.txt' using ($1/1000000000):($2/1000) notitle w points ls 1 pt 1 ps 1, \
#for [i=0:0] 'fvc/fvcheartbeatLATEST.transient/c'.i.'.txt'  using ($1/1000000000):($2/1000) title "FVC"  w points ls 9 pt 9 ps 1, \
#for [i=1:39] 'fvc/fvcheartbeatLATEST.transient/c'.i.'.txt' using ($1/1000000000):($2/1000) notitle w points ls 9 pt 9 ps 1, \
###############
###############
#set xlabel "Time (Second)" offset 0,0.4
#set ylabel "Command Latency (ms)" offset 1,0
#
#set xtics nomirror
#set ytics nomirror
##set key bottom right width 1.5
#set key top left width 1 height 0
#set output "transient_copilot_linear.pdf"
#
##set xdata time
##set timefmt "%H:%M:%S"
##set format x "%S"
#unset log x
##set log x
#set xr [0:12]
#set xtics 0,2,14
##set xtic 8,2,1024 offset 0,0.4
##set xtic  (.1, .2, .4, .6, .8, .9, .99) offset 0,0.4
#
#unset log y
#set yr [0:100]
##set ytic 0,2
##set yr [0.1:1000]
##set arrow from 40,0to 40,1 nohead lc rgb "black";
##set arrow from -0.55,-0.1 to -0.45,0.1 nohead ls 4 lc rgb "black";
#
#exp_dir1="./"
#plot for [i=0:0] 'copilot/transient.to10ms/c'.i.'.txt' using ($1/1000000000):($2/1000) title "Copilot"  w points ls 2 pt 2 ps 1, \
#for [i=1:29] 'copilot/transient.to10ms/c'.i.'.txt' using ($1/1000000000):($2/1000) notitle w points ls 2 pt 2 ps 1, \
##############
##############
set xlabel "Time (Second)" offset 0,0.4
set ylabel "Command Latency (ms)" offset 1,0

set xtics nomirror
set ytics nomirror
#set key bottom right width 1.5
set key top left width 1 height 0
if (!exists("filename")) filename='notfound'
if (!exists("outfilename")) outfilename='notfound'
if (!exists("sysname")) sysname='notfound'
if (!exists("c")) c=1
# set output "transient_avicenna_linear.pdf"
set output outfilename.'.pdf'


#set xdata time
#set timefmt "%H:%M:%S"
#set format x "%S"
unset log x
#set log x
set xr [*:*]
#set xr [0:12]
#set xtics 0,2,14
#set xtic 8,2,1024 offset 0,0.4
#set xtic  (.1, .2, .4, .6, .8, .9, .99) offset 0,0.4

unset log y
unset yr
set yr [0:1000]
set key top right
# set key top left
#set ytic 0,2
#set yr [0.1:1000]
#set arrow from 40,0to 40,1 nohead lc rgb "black";
#set arrow from -0.55,-0.1 to -0.45,0.1 nohead ls 4 lc rgb "black";

exp_dir1="./"
plot filename using ($1/1000000):($2/1000) title sysname w points ls c pt 2 ps 1,

# for FVC
# set log y
# unset yr
# plot filename using ($1/1000000):($2/1000) title sysname w points ls c pt 2 ps 1,