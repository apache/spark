## Things to do for SparkR, roughly in order of importance

## Unit tests still TODO

1. textFile + collect -- use README.md, or some test file (check if minSplits
works correctly)
2. utils.R - Check if dependencies are serialized correctly
3. convertJListToRList

## Functions to support
1. Similar to `stats.py` in Python, add support for mean, median, stdev etc.
2. Broadcast variables.
3. Consider if we need to extend `addPackage` so that any given R file can be sourced in the worker before functions are run.

## Performance improvements
1. Write hash functions in C and use .Call to call into them
2. Use long-running R worker daemons to avoid forking a process each time.
3. Memoizations of frequently queried vals in RDD, such as numPartitions, count etc.
4. Pipelined RRDD to execute multiple functions with one call.

## Feature wishlist

1. Integration with ML Lib to run ML algorithms from R.
2. RRDDs are distributed lists. Extend them to create a distributed data frame.
3. Profile serialization overhead and see if there is anything better we can do.
4. Reduce code duplication between SparkR and PySpark
5. Add more machine learning examples and some performance benchmarks.
