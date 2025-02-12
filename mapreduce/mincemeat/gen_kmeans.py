#!/usr/bin/env python
# This script will generate data for k-means clusterization

from random import uniform
from random import randint
from random import gauss
import numpy as np
from matplotlib import pyplot as plt

# How many clusters we need
CLUSTER_COUNT = 10
# How many points per cluster we will generate
POINTS_PER_CLUSTER = 500
# The points will be in a square [0, SQUARE_SIZE], [0, SQUARE_SIZE] (some points may go slightly beyond the bounds)
SQUARE_SIZE = 20
# Radius around the cluster centroid where most points will be placed into
CLUSTER_RADIUS = 0.4
# The size of a data chunk
# It is the same as the cluster size for convenience.
DATA_CHUNK_SIZE = POINTS_PER_CLUSTER
# Output file prefix
FILE_PREFIX = "kmeans/cluster_"

random_centroids = [(round(uniform(0, SQUARE_SIZE), 3), round(uniform(0, SQUARE_SIZE), 3)) for i in range(CLUSTER_COUNT)]
random_data = []


for i in range(CLUSTER_COUNT * POINTS_PER_CLUSTER):
    centroid = random_centroids[randint(0, CLUSTER_COUNT - 1)]
    random_point = (round(gauss(centroid[0], CLUSTER_RADIUS), 3), round(gauss(centroid[1], CLUSTER_RADIUS), 3))
    random_data.append(random_point)

file_num = 1
line_count = 0
centroid_num = 1
file = None
for p in random_data:
    if file is None:
        file = open(f"{FILE_PREFIX}{file_num}", "w")
        file.write(f"{file_num}\n")
    file.write(f"{p[0]} {p[1]}\n")
    line_count += 1

    if line_count % POINTS_PER_CLUSTER == 0:
        with open(f"centroid_{centroid_num}", "w") as centroid_file:
            centroid_file.write(f"{p[0]} {p[1]}\n")
        centroid_num += 1

    if line_count % DATA_CHUNK_SIZE == 0:
        file.close()
        file = None
        file_num += 1
data = np.array(random_data)
x, y = data.T
plt.scatter(x,y)
plt.show()