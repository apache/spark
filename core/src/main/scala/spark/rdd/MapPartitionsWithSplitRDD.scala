package spark.rdd

import spark.{RDD, Split, TaskContext}


/**
 * A variant of the MapPartitionsRDD that passes the split index into the
 * closure. This can be used to generate or collect partition specific
 * information such as the number of tuples in a partition.
 */
private[spark]
class MapPartitionsWithSplitRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean
  ) extends RDD[U](prev) {

  override def getSplits = firstParent[T].splits

  override val partitioner = if (preservesPartitioning) prev.partitioner else None

  override def compute(split: Split, context: TaskContext) =
    f(split.index, firstParent[T].iterator(split, context))
}