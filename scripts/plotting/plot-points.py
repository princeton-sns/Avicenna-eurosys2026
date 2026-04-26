import sys
import plotly.express as px
import pandas as pd
from argparse import ArgumentParser

p = ArgumentParser()
_ = p.add_argument('-s', action='store_true')
_ = p.add_argument('-f', '--file', type=str)
args = p.parse_args()

file = args.file
print("File to open %s\n" % file)

df = pd.read_csv(file, index_col=0, delim_whitespace=True)
fig = px.ecdf(df)
if args.s:
    fig.show()
fig.write_image(file+".png")


