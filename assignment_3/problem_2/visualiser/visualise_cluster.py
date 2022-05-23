"""
Necessary imports
"""

from matplotlib.colors import ListedColormap
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import glob
from mpl_toolkits.mplot3d import Axes3D

greet = \
"""
Hi there!
This is the visualisation tool for Problem 2 from assignment 3 in BDA 2022.
You have the following options:
    [1] Visualise the raw data
    [2] Visualise the clusters (non-normalised)

Please enter the desired option:
"""
choice = input(greet)

if choice == "1":
    # get list of dataframes from the files in our stated directory
    files = glob.glob("../output/data4plots/raw_sample/*.csv")
    df = []
    for f in files:
        csv = pd.read_csv(f)
        df.append(csv)

    # now combine them into a single dataframe
    df = pd.concat(df, ignore_index=True)

    # plot them
    sns.set(style="darkgrid")

    fig = plt.figure()
    ax= fig.add_subplot(111, projection = '3d')

    x = df['inVolt']
    y = df['outVolt']
    z = df['tacho']

    ax.set_xlabel("Output Voltage")
    ax.set_ylabel("Input Voltage")
    ax.set_zlabel("Tachometer Reading")

    ax.scatter(x,y,z)

    plt.show()

elif choice == "2":
    files = glob.glob("../output/data4plots/clusters/*.csv")
    df = []
    for f in files:
        csv = pd.read_csv(f)
        df.append(csv)

    # now combine them into a single dataframe
    clusters_df = pd.concat(df, ignore_index=True)

    # plot them
    sns.set(style="darkgrid")

    fig = plt.figure()
    ax= Axes3D(fig)

    x = clusters_df['inVolt']
    y = clusters_df['outVolt']
    z = clusters_df['tacho']
    hue = clusters_df['prediction']

    # cmap = ListedColormap(sns.color_palette("husl", 256).as_hex())
    cmap = ListedColormap(sns.husl_palette())

    ax.set_xlabel("Output Voltage")
    ax.set_ylabel("Input Voltage")
    ax.set_zlabel("Tachometer Reading")

    ax.scatter(x,y,z, c=hue, cmap=cmap)

    plt.show()
else:
    raise Exception("Sorry, free choice is an illusion.")
