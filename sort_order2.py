#!/bin/python

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os

sns.set_theme(style="whitegrid")

csv = sys.argv[1]

df = pd.read_csv(csv)
df = df[df["experiment"] != "base"]

#
# Common plot configuration
#


class Column():
    def __init__(self, name, label, color, marker):
        self.name = name
        self.color = color
        self.marker = marker
        self.label = label


# items in the legend appear in this order
columns = [
    Column("no_runs", "Segmenting",  "red", "^"),
    Column("no_segment", "Preserving runs",  "orange", "^"),
    Column("ovc", "Segmenting + preserving runs", "dodgerblue", "s"),
]

order = []
labels = {}
markers = {}
palette = {}
for column in columns:
    order.append(column.name)
    labels[column.name] = column.label
    markers[column.name] = column.marker
    palette[column.label] = column.color

# reorder
df["index"] = df["experiment"].map(lambda e: order.index(e))
df = df.sort_values("index").reset_index(drop=True)

# change labels
df["experiment"] = df["experiment"].map(lambda x: labels[x])

# remove 1e6 on y axis
df["column_comparisons"] = df["column_comparisons"] / 1000000


# plot
ax = sns.barplot(data=df, x="bits_per_row", y="duration", hue="experiment",
                 palette=palette,
                 errwidth=1.5,
                 # hue_order=order,
                 # style_order=order,
                 # markers=markers,
                 # errorbar=None)
                 )

# Labels
plt.xlabel("Number of segments")
plt.ylabel("Run time [milliseconds]")

# x ticks
labels = df["bits_per_row"].unique()
labels.sort()
ax.set_xticklabels([f"$2^{{{i}}}$" for i in labels])

handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles[0:], labels=labels[0:])

# g.add_legend(legend_data={
#     labels[key]: value for key, value in zip(labels, g._legend_data.values())
# }, ncol=4, loc="upper center")

# g.set_titles(col_template="{col_name}")

# output size
fig = plt.gcf()
fig.set_size_inches(6, 4)

try:
    os.mkdir("./img")
except OSError:
    pass

plt.savefig("img/sort_order2.png",
            # transparent=True,
            format="png", bbox_inches='tight', dpi=300)
plt.close()
