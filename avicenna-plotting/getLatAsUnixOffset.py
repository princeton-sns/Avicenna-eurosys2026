import copy
from heapq import heappush, heappop
import numpy as np
import os
import pandas as pd
import re
import sys

dfs = []
dir = sys.argv[1]
outfile = sys.argv[2]

print(dir)
# it's a dir
if dir[-1] != '/':
    dir+='/'

print(dir)

names = ["time"]
names.append("latency")

for file in os.listdir(dir):
    if 'unix' in file:
        print("\tReading in %s..." % file)
        df = pd.read_csv(dir+file, header=None, names=names,
                delim_whitespace=True, index_col=False)
        dfs.append(df[names])

dfs = pd.concat([df for df in dfs])

# df.ID = pd.to_numeric(df.ID, errors='coerce')

# dfs.time = pd.to_numeric(dfs.time, errors='coerce')
# dfs["time"].astype(int)



dfs["time"] = dfs["time"] - dfs["time"].min() # make time start at 0
dfs = dfs.sort_values(by="time")

millisecond = 1*1000
maxts = dfs["time"].max()
dfs = dfs[dfs['time'] > 5000*millisecond]
dfs = dfs[dfs['time'] < maxts - 5000*millisecond]

dfs["time"] = dfs["time"] - dfs["time"].min() # make time start at 0

print(dfs)
np.random.seed(10)

if len(sys.argv) == 4:
    remove_n = int(sys.argv[3])
    drop_indices = np.random.choice(dfs.index, remove_n, replace=False)
    dfs = dfs.drop(drop_indices)

print("Writing to", outfile)
dfs.to_csv(outfile, sep=" ", index=False)