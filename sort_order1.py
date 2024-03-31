#!/bin/python

# renamed columns

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os

sns.set_theme(style="whitegrid")

csv = sys.argv[1]

df = pd.read_csv(csv)
df = df[df["experiment"] != "base"]

data_contains_stats = len(df["column_comparisons"].unique()) > 1


class Column():
    def __init__(self, name, label, color, marker):
        self.name = name
        self.color = color
        self.marker = marker
        self.label = label


# items in the legend appear in this order
columns = [
    Column("traditional", "Without OVCs",  "red", "^"),
    Column("ovc", "With OVCs", "blue", "s"),
]

order = []
labels = {}
markers = {}
for column in columns:
    order.append(column.name)
    labels[column.name] = column.label
    markers[column.name] = column.marker

# palette = {'OVC sort': "tab:cyan", 'Traditional sort': "tab:purple"}
# palette = {'OVC sort': "tab10:blue", 'Traditional sort': "tab10:red"}
palette = {'With OVCs': "dodgerblue", 'Without OVCs': "red"}

# reorder
df["index"] = df["experiment"].map(lambda e: order.index(e))
df = df.sort_values("index").reset_index(drop=True)

# col_order = ["random", "first_decides", "last_decides"]
col_order = ["first_decides", "last_decides"]

# rename data, set facet order
data_names = {
    "random": "Random",
    "first_decides": "First decides",
    "last_decides": "Last decides"
}

if "random" in df["data"]:
    col_order = ["First decides", "Random", "Last decides"]
else:
    col_order = ["First decides", "Last decides"]
df["data"] = df["data"].map(lambda x: data_names[x])

# change labels
df["experiment"] = df["experiment"].map(lambda x: labels[x])

if not data_contains_stats:

    # plot
    g = sns.FacetGrid(df, col="data", col_order=col_order)
    g.map_dataframe(sns.barplot, x="list_length", y="duration", hue="experiment",
                    palette=palette,
                    errwidth=1.5,
                    )

    # plt.errorbar(x, y, yerr=stds, capsize=3)

    # plt.rcParams.update({'lines.markeredgewidth': 1})

    g.set_xlabels("Attribute list length")
    g.set_ylabels("Run time [milliseconds]")

    g.fig.subplots_adjust(top=0.8)

    g.add_legend(legend_data={
        labels[key]: value for key, value in zip(labels, g._legend_data.values())
    }, ncol=4, loc="upper center")
    g.set_titles(col_template="{col_name}")

    # fig = plt.gcf()
    # fig.set_size_inches(4, 3)

    try:
        os.mkdir("./img")
    except OSError:
        pass

    plt.savefig("img/sort_order1.png",
                # transparent=True,
                format="png", bbox_inches='tight', dpi=300)
    plt.close()

if data_contains_stats:
    g = sns.FacetGrid(df, col="data", col_order=col_order)
    g.map_dataframe(sns.barplot, x="list_length", y="column_comparisons", hue="experiment",
                    palette=palette,
                    errwidth=1.5,
                    log=True,
                    )

    # plt.errorbar(x, y, yerr=stds, capsize=3)

    # plt.rcParams.update({'lines.markeredgewidth': 1})

    g.set_xlabels("Attribute list length")
    g.set_ylabels("Column comparisons")

    g.fig.subplots_adjust(top=0.8)

    g.add_legend(legend_data={
        labels[key]: value for key, value in zip(labels, g._legend_data.values())
    }, ncol=4, loc="upper center")

    g.set_titles(col_template="{col_name}")

    # fig = plt.gcf()
    # fig.set_size_inches(8, 16)

    plt.savefig("img/sort_order1_cc.png",
                # transparent=True,
                format="png", bbox_inches='tight', dpi=300)
    plt.close()
