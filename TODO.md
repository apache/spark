# Things to do for SparkR

## Functions to support in RDD

1. `head` which is called `take` in Spark.
2. `reduceByKey` and `groupByKey` -- Depends on implementing partitioner and PairRDD
3. Similar to `stats.py` in Python, add support for mean, median, stdev etc.

## Other features to support

1. Broadcast variables.
2. Allow R packages to be loaded into the run time. Also consider if we need to extend
this for any given R file to be sourced in the worker.
3. Use long-running R worker daemons to avoid forking a process each time.

## Longer term wishlist

1. RRDDs are distributed lists. Extend them to create a distributed data frame.
2. Integration with ML Lib to run ML algorithms from R.
3. Profile serialization overhead and see if there is anything better we can do.
