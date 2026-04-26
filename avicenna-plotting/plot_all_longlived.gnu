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
set terminal pngcairo font "Helvetica,28" linewidth 4 rounded enhanced
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
# set font ",111"
set xlabel "Time (Second)" offset 0,0.4 #font ",26"
set ylabel "Command Latency (ms)" offset 1,0 font ",26"

set xtics nomirror
set ytics nomirror
#set key bottom right width 1.5
set key top left width 1 height 0
if (!exists("filename")) filename='notfound'
if (!exists("outfilename")) outfilename='notfound'
if (!exists("sysname")) sysname='notfound'
if (!exists("c")) c=1
# set output "transient_avicenna_linear.pdf"
set output outfilename.'.png'


#set xdata time
#set timefmt "%H:%M:%S"
#set format x "%S"
unset log x
#set log x
# set xr [*:*]
# set xr [10:30]
set xr [15:25]
set xtic 2

#set xr [0:12]
#set xtics 0,2,14
#set xtic 8,2,1024 offset 0,0.4
#set xtic  (.1, .2, .4, .6, .8, .9, .99) offset 0,0.4

unset log y
unset yr
# set yr [0:1400]
# set yr [0:1200]
# set key top right
set key top left
#set ytic 0,2
#set yr [0.1:1000]
#set arrow from 40,0to 40,1 nohead lc rgb "black";
#set arrow from -0.55,-0.1 to -0.45,0.1 nohead ls 4 lc rgb "black";

set nokey

exp_dir1="./"


# set ytic 200 font ",22"
unset label
set yr [0:1400]
# set yr [0:*]
set output "Avicenna0-68-ll.png"
plot "Avicenna-0/Avicenna-0-68-ll/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
set output "Avicenna5-68-ll.png"
plot "Avicenna-5/Avicenna-5-68-ll/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
set output "Avicenna25-68-ll.png"
plot "Avicenna-25/Avicenna-25-68-ll/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# set output "Avicenna100-68-ll.pdf"
# plot "Avicenna/Avicenna-100-68-ll/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
set output "Copilot-68-ll.png"
plot "Copilot/Copilot-68-ll/processedLats" using ($1/1000000):($2/1000) title "Copilot" w points ls 2 pt 2 ps 1,
set output "Copilot-PP-68-ll.png"
plot "Copilot-PP/Copilot-PP-68-ll/processedLats" using ($1/1000000):($2/1000) title "Copilot-Ping-Pong" w points ls 3 pt 2 ps 1,
set output "FVC-68-ll.png"
plot "FVC/FVC-68-ll/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos-Fast-View-Change" w points ls 4 pt 2 ps 1,
set output "Reactive-Copilot-68-ll.png"
plot "Reactive-Copilot/Reactive-Copilot-68-ll/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos" w points ls 5 pt 2 ps 1,
set output "Multi-Paxos-68-ll.png"
plot "Multi-Paxos/Multi-Paxos-68-ll/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos" w points ls 6 pt 2 ps 1,


set yr [0:1600]
# set ytic 300
# set yr [0:*]
set output "Avicenna0-204-ll.png"
plot "Avicenna-0/Avicenna-0-204-ll/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
set output "Avicenna5-204-ll.png"
plot "Avicenna-5/Avicenna-5-204-ll/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
set output "Avicenna25-204-ll.png"
plot "Avicenna-25/Avicenna-25-204-ll/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# set output "Avicenna100-204-ll.pdf"
# plot "Avicenna/Avicenna-100-204-ll/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
set output "Copilot-204-ll.png"
plot "Copilot/Copilot-204-ll/processedLats" using ($1/1000000):($2/1000) title "Copilot" w points ls 2 pt 2 ps 1,
set output "Copilot-PP-204-ll.png"
plot "Copilot-PP/Copilot-PP-204-ll/processedLats" using ($1/1000000):($2/1000) title "Copilot-Ping-Pong" w points ls 3 pt 2 ps 1,
set output "FVC-204-ll.png"
plot "FVC/FVC-204-ll/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos-Fast-View-Change" w points ls 4 pt 2 ps 1,
set output "Reactive-Copilot-204-ll.png"
plot "Reactive-Copilot/Reactive-Copilot-204-ll/processedLats" using ($1/1000000):($2/1000) title "Latent Copilot" w points ls 5 pt 2 ps 1,
set yr [0:3000]
# set ytic 400
set output "Multi-Paxos-204-ll.png"
plot "Multi-Paxos/Multi-Paxos-204-ll/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos" w points ls 6 pt 2 ps 1,


# set xr [*:*]
# # set ytic 200 font ",22"
# unset label
# set yr [0:*]
# # set yr [0:*]
# set output "Avicenna0-68-slow-for-client.pdf"
# plot "Avicenna-0/Avicenna-0-68-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# set output "Avicenna5-68-slow-for-client.pdf"
# plot "Avicenna-5/Avicenna-5-68-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# set output "Avicenna25-68-slow-for-client.pdf"
# plot "Avicenna-25/Avicenna-25-68-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# # set output "Avicenna100-68-slow-for-client.pdf"
# # plot "Avicenna/Avicenna-100-68-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# set output "Copilot-68-slow-for-client.pdf"
# plot "Copilot/Copilot-68-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Copilot" w points ls 2 pt 2 ps 1,
# set output "Copilot-PP-68-slow-for-client.pdf"
# plot "Copilot-PP/Copilot-PP-68-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Copilot-Ping-Pong" w points ls 3 pt 2 ps 1,
# set output "FVC-68-slow-for-client.pdf"
# plot "FVC/FVC-68-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos-Fast-View-Change" w points ls 4 pt 2 ps 1,
# set output "Reactive-Copilot-68-slow-for-client.pdf"
# plot "Reactive-Copilot/Reactive-Copilot-68-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos" w points ls 5 pt 2 ps 1,
# set output "Multi-Paxos-68-slow-for-client.pdf"
# plot "Multi-Paxos/Multi-Paxos-68-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos" w points ls 6 pt 2 ps 1,


# set yr [0:*]
# # set ytic 300
# # set yr [0:*]
# set output "Avicenna0-204-slow-for-client.pdf"
# plot "Avicenna-0/Avicenna-0-204-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# set output "Avicenna5-204-slow-for-client.pdf"
# plot "Avicenna-5/Avicenna-5-204-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# set output "Avicenna25-204-slow-for-client.pdf"
# plot "Avicenna-25/Avicenna-25-204-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# # set output "Avicenna100-204-slow-for-client.pdf"
# # plot "Avicenna/Avicenna-100-204-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Avicenna" w points ls 1 pt 2 ps 1,
# set output "Copilot-204-slow-for-client.pdf"
# plot "Copilot/Copilot-204-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Copilot" w points ls 2 pt 2 ps 1,
# set output "Copilot-PP-204-slow-for-client.pdf"
# plot "Copilot-PP/Copilot-PP-204-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Copilot-Ping-Pong" w points ls 3 pt 2 ps 1,
# set output "FVC-204-slow-for-client.pdf"
# plot "FVC/FVC-204-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos-Fast-View-Change" w points ls 4 pt 2 ps 1,
# set output "Reactive-Copilot-204-slow-for-client.pdf"
# plot "Reactive-Copilot/Reactive-Copilot-204-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Latent Copilot" w points ls 5 pt 2 ps 1,
# # set yr [0:3200]
# # set ytic 400
# set output "Multi-Paxos-204-slow-for-client.pdf"
# plot "Multi-Paxos/Multi-Paxos-204-slow-for-client/processedLats" using ($1/1000000):($2/1000) title "Multi-Paxos" w points ls 6 pt 2 ps 1,

