## Things to do for SparkR

## Unit tests
1. utils.R - Check if dependencies are serialized correctly

## Functions to support
1. Similar to `stats.py` in Python, add support for mean, median, stdev etc.
2. Extend `addPackage` so that any given R file can be sourced in the worker before functions are run.
3. Add a `lookup` method to get an element of a pair RDD object by key.
4. `hashCode` support for arbitrary R objects.
5. Support for other storage types like storing RDDs on disk.
6. Extend input formats to support `sequenceFile`.

## Performance improvements
1. Write hash functions in C and use .Call to call into them.
2. Memoizations of frequently queried vals in RDD, such as numPartitions, count etc.
3. Profile serialization overhead and see if there is anything better we can do.

## Feature wishlist

1. Integration with ML Lib to run ML algorithms from R.
2. RRDDs are distributed lists. Extend them to create a distributed data frame.
3. Support accumulators in R.
4. Reduce code duplication between SparkR and PySpark.
5. Add more machine learning examples and some performance benchmarks.
