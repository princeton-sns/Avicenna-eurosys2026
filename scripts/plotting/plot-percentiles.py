import sys
import plotly.express as px
import pandas as pd

file = sys.argv[1]
print("File to open %s\n" % file)

df = pd.read_csv(file, index_col=0, delim_whitespace=True)
fig = px.scatter(df)
# fig.show()
fig.write_image(file+".png")