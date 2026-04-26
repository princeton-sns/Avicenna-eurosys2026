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
set terminal pdfcairo font "Helvetica,15" linewidth 4 rounded enhanced
#set grid
set border 3 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
set xtics nomirror
set ytics nomirror

set log x
#set mxtics 15    # Makes logscale look good

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

# # old
# set style line 1 lt rgb "#A00000" lw 1 pt 1
# set style line 2 lt rgb "#00A000" lw 1 pt 6
# set style line 3 lt rgb "#5060D0" lw 1 pt 2
# set style line 4 lt rgb "#F25900" lw 1 pt 9
# set style line 5 lt rgb "#00C5C5" lw 1 pt 3
# set style line 6 lt rgb "#804000" lw 1 pt 4
##############
if (!exists("exp_dir")) exp_dir='notfound'
output_dir=exp_dir."/"
##############
set xlabel "Command Latency (ms) (log)" offset 0,0.4
set ylabel "CDF" offset 1,0

set xtics nomirror
set ytics nomirror
#set key bottom right width 1.5
set key bottom right width 1.5 height 1
set output "paxos_net.pdf"

set xlabel "Command Latency (ms)" offset 0,0.4
set ylabel "CDF" offset 1,0

set xtics nomirror
set ytics nomirror
#set key bottom right width 1.5
set key bottom right width 1.5 height 1
# set key center above font ",16" maxrows 4
set output "normal-case-no-mp.pdf"

# set grid 
unset log x
#set xr [0:4]
#set log x
set xr [*:*]
# set xr [0:1000]
#set xtic 8,2,1024 offset 0,0.4
#set xtic  (.1, .2, .4, .6, .8, .9, .99) offset 0,0.4

unset log y
#set log y
#set yr [0.4:1.6]
#set ytic 0,2
#set yr [0:250]


exp_dir1="./"
set key above right maxrows 4 font ",16"

# talk
# set nokey
# set xtic 100

# Normal case
plot \
"Avicenna-25/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-25%" with linespoints ls 9 pointinterval 15, \
"Avicenna-5/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-5%" with linespoints ls 7 pointinterval 15, \
"Avicenna-0/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-0%" with linespoints ls 3 pointinterval 15, \
"FVC/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Multi-Paxos-FVC" with linespoints ls 4 pointinterval 15, \
"Reactive-Copilot/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Latent Copilot" with linespoints ls 5 pointinterval 15, \
"Copilot-PP/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Copilot-Ping-Pong" with linespoints ls 2 pointinterval 15, \
"Copilot/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Copilot" with linespoints ls 1 pointinterval 15, \

# "Multi-Paxos/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Multi-Paxos" with linespoints ls 6 pointinterval 15, \
# "Avicenna/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-100%" with linespoints ls 3 pointinterval 15, \
# "Avicenna-50/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-50%" with linespoints ls 7 pointinterval 15, \
# "Avicenna-5/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-5%" with linespoints ls 8 pointinterval 15, \
# "Avicenna-0/normal/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-0%" with linespoints ls 9 pointinterval 15, \

set output "68-slow-for-client-no-mp.pdf"
plot \
"Avicenna-25/Avicenna-25-68-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-25%" with linespoints ls 9 pointinterval 15, \
"Avicenna-5/Avicenna-5-68-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-5%" with linespoints ls 7 pointinterval 15, \
"Avicenna-0/Avicenna-0-68-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-0%" with linespoints ls 3 pointinterval 15, \
"Reactive-Copilot/Reactive-Copilot-68-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Latent Copilot" with linespoints ls 5 pointinterval 15, \
"Copilot-PP/Copilot-PP-68-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Copilot-Ping-Pong" with linespoints ls 2 pointinterval 15, \
"FVC/FVC-68-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Multi-Paxos-FVC" with linespoints ls 4 pointinterval 15, \
"Copilot/Copilot-68-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Copilot" with linespoints ls 1 pointinterval 15, \
# "Multi-Paxos/Multi-Paxos-68-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Multi-Paxos" with linespoints ls 6 pointinterval 15, \
# "Avicenna/slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-100%" with linespoints ls 3 pointinterval 15, \
# "Avicenna-50/slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-50%" with linespoints ls 7 pointinterval 15, \
# "Avicenna-5/slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-5%" with linespoints ls 8 pointinterval 15, \
# "Avicenna-0/slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-0%" with linespoints ls 9 pointinterval 15, \

set output "204-slow-for-client-no-mp.pdf"
plot \
"Avicenna-25/Avicenna-25-204-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-25%" with linespoints ls 9 pointinterval 15, \
"Avicenna-5/Avicenna-5-204-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-5%" with linespoints ls 7 pointinterval 15, \
"Avicenna-0/Avicenna-0-204-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-0%" with linespoints ls 3 pointinterval 15, \
"Reactive-Copilot/Reactive-Copilot-204-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Latent Copilot" with linespoints ls 5 pointinterval 15, \
"Copilot-PP/Copilot-PP-204-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Copilot-Ping-Pong" with linespoints ls 2 pointinterval 15, \
"Copilot/Copilot-204-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Copilot" with linespoints ls 1 pointinterval 15, \
"FVC/FVC-204-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Multi-Paxos-FVC" with linespoints ls 4 pointinterval 15, \
# "Multi-Paxos/Multi-Paxos-204-slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Multi-Paxos" with linespoints ls 6 pointinterval 15, \
# "Avicenna/slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-100%" with linespoints ls 3 pointinterval 15, \
# "Avicenna-50/slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-50%" with linespoints ls 7 pointinterval 15, \
# "Avicenna-5/slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-5%" with linespoints ls 8 pointinterval 15, \
# "Avicenna-0/slow-for-client/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "Avicenna-0%" with linespoints ls 9 pointinterval 15, \



###
# "epaxos25/epaxos25.net.rerun/0.25ms/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "0.5" with linespoints ls 1 pointinterval 15, \
# "epaxos25/epaxos25.net.rerun/0.5ms/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "1" with linespoints ls 2 pointinterval 15, \
# "epaxos25/epaxos25.net.rerun/1ms/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "2" with linespoints ls 3 pointinterval 15, \
# "epaxos25/epaxos25.net.rerun/2.5ms/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "5" with linespoints ls 4 pointinterval 15, \
# "epaxos25/epaxos25.net.rerun/5ms/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "15" with linespoints ls 5 pointinterval 15, \
# "epaxos25/epaxos25.net.rerun/10ms/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "15" with linespoints ls 7 pointinterval 15, \
# "epaxos25/epaxos25.net.rerun/15ms/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "30" with linespoints ls 8 pointinterval 15, \
# "epaxos25/epaxos25.net.rerun/20ms/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "40" with linespoints ls 15 pointinterval 15, \
# "epaxos25/epaxos25.net.clis80/7.5ms/percentilesnew.txt" every ::0::99 using ($2/1000):($1/100) title "15" with linespoints ls 6 pointinterval 15, \
# #############
