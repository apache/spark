# Things to do for SparkR

## Functions to support in RDD

2. Similar to `stats.py` in Python, add support for mean, median, stdev etc.

## Other features to support

1. Broadcast variables.
2. Allow R packages to be loaded into the run time. Also consider if we need to extend
this for any given R file to be sourced in the worker before functions are run.
3. Use long-running R worker daemons to avoid forking a process each time.
4. Memoizations of frequently queried vals in RDD, such as numPartitions, count etc.

## Longer term wishlist

1. RRDDs are distributed lists. Extend them to create a distributed data frame.
2. Integration with ML Lib to run ML algorithms from R.
3. Profile serialization overhead and see if there is anything better we can do.
4. Reduce code duplication between SparkR and PySpark
5. Add more examples (machine learning ?) and some performance benchmarks.

## Unit tests to add

### sparkR, context

1. textFile + collect -- use README.md, or some test file
   1. Check if minSplits works correctly

### Utils
1. utils.R - Check if dependencies are serialized correctly
2. convertJListToRList

## Documentation

1. Write Rd documentation for RRDD functions, context functions
