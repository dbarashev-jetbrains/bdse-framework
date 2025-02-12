## Summary

This directory contains a few map-reduce programs built with the help of a toy Map-Reduce framework [`mincemeat`](https://github.com/michaelfairley/mincemeatpy) which was slightly modified, mostly in what concerns to reading the input data. 

## Running map-reduce dispatcher and workers

Dispatcher is the process that knows the input data, map and reduce functions and can send the functions and the 
data to the worker process. The dispatcher is not doing any maps or reduces, however, it provides shuffling.

You can start a dispatcher process as follows:

```shell
python [mapreduce_file] [mapreduce_arguments]
```

The dispatcher starts and waits for the workers to join. You can start a worker process with 

```shell
python mincemeat.py [dispatcher address]   
```

If everything is fine, the dispatcher will start reading the input and calling the worker node to do maps and reduces. 
Once the whole input is processed, the dispatcher process will terminate.

## Examples
An example of the classic wordcount map-reduce is implemented in `wordcount.py`. 
Run the dispatcher with `python wordcount.py` and a worker process with `python mincemeat.py 127.0.0.1`

### Counting unigrams and bigrams

Files prefixed with `great` are map-reduces that counts all unigrams and bigrams in a word following the word 
"great" in a Google Books n-grams dataset.

File `great_bigrams.py` implements a few library functions for working with the dataset.

Files `great_bigrams_take1.py`, `great_bigrams_take2.py`,and `great_bigrams_take_3.py` implement the same map-reduce with
different inputs: 1) the map input record is a single line from a source file, 2) the map input record is the whole 
source file contents and 3) the map input is the source file name.

File `great_local.py` implements the same bi- and unigrams counting without the use of map-reduce

In `input` subdirectory you can find a small dataset from Google Books N-grams.

Run the dispatcher with `python great_bigrams_take3.py input` and a worker with `python mincemeat.py 127.0.0.1`

## K-means
You can generate input data for k-means using `python gen_kmeans.py`. \
By default it generates 5000 points that comprise 10 clusters in 10 files inside `kmeans` directory, and 10 files named 
`centroid_N` with the initial pseudo-random centroids. I recommend to leave the default parameters as-is.

A draft of the map-reduce is implemented in `mr_kmeans.py`. It is a fake implementation that just ovewrites the dataset
with the coordinates of the initial centroids. Feel free to modify that file.