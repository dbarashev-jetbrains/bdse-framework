# This map-reduce is a stub for k-means clustering.
# Feel free to modify this code as necessary.

import argparse
import mincemeat
import mincemeat_inputs
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from collections import deque

# Reads files from the given directory, for each file skips the first line and converts the remaining lines to
# 2D points. The points are plotted on the canvas using different colors for different files.
def plot_data(dir):
    data_files = mincemeat_inputs.FileNameMapInput(dir)
    colors = deque([c for c in cm.rainbow(np.linspace(0, 1, data_files.size()))])
    while (data_files.has_next()):
        next_file = data_files.next()
        with open(f"{next_file[0]}/{next_file[1]}") as f:
            lines = f.read().splitlines()
            points = np.array([(float(point_str[0]), float(point_str[1])) for point_str in [s.split() for s in lines[1:]]])
            x, y = points.T
            plt.scatter(x,y,color=colors.popleft())

    plt.show()

# This map function just outputs the centroid number and coordinates.
# TODO: you need to write your code here
def mapfn(k, v):
    import math # otherwise it will not work on worker
    CLUSTER_COUNT=10

    def dist(point, centroid):
        return math.sqrt((point[0] - centroid[0])**2 + (point[1] - centroid[1])**2)

    for cnum in range(1, CLUSTER_COUNT):
        with open("centroid_%d" % cnum, "r") as centroid_file:
            cstr = centroid_file.read().splitlines()[0].split()
            c_x, c_y = float(cstr[0]), float(cstr[1])
            yield f"{cnum}", f"{c_x} {c_y}"

# In the reduce task we get a centroid point number and its coordinates repeated as many times as we have input shards
# We just write the first point from the list to both centroid_N and cluster_N files.
# This means that we will have a "cluster" consisting of just a single centroid point.
# TODO: you need to write your code here
def reducefn(k, vs):
    import codecs
    num = int(k)
    if len(vs) == 0:
        return

    c_x = float(vs[0].split()[0])
    c_y = float(vs[0].split()[1])

    print(f"Writing new centroid=({c_x}, {c_y}) to the file centroid_{num}")
    # Write the new centroid to its file
    with open("centroid_%d" % num, 'w') as centroid_file:
        centroid_file.write("%f %f\n" % (c_x, c_y))

    # Write points from the cluster to the output shard
    with open(f"kmeans/cluster_{num}", 'w') as cluster_file:
        cluster_file.write("%d\n" % num)
        cluster_file.write("%f %f\n" % (c_x, c_y))
    return k


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='K-means map-reduce',
        description='Runs an iteration of k-means clustering algorithm',
        epilog='See you!')
    # Switch on to plot the points before running map-reduce
    parser.add_argument('-b', '--plot-before', action='store_true')
    # Switch on to plot the points after running map-reduce
    parser.add_argument('-a', '--plot-after', action='store_true')
    args = parser.parse_args()


    s = mincemeat.Server()

    s.map_input = mincemeat_inputs.FileNameMapInput("kmeans")
    s.mapfn = mapfn
    s.reducefn = reducefn

    if args.plot_before:
        plot_data("kmeans")

    results = s.run_server()

    if args.plot_after:
        plot_data("kmeans")
