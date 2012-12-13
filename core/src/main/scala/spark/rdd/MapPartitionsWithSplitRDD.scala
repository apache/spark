package spark.rdd

import spark.{OneToOneDependency, RDD, Split, TaskContext}

/**
 * A variant of the MapPartitionsRDD that passes the split index into the
 * closure. This can be used to generate or collect partition specific
 * information such as the number of tuples in a partition.
 */
private[spark]
class MapPartitionsWithSplitRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean)
  extends RDD[U](prev.context) {

  override val partitioner = if (preservesPartitioning) prev.partitioner else None
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split, taskContext: TaskContext) =
    f(split.index, prev.iterator(split, taskContext))
}