package spark.rdd

import spark.{RDD, Partition, TaskContext}


/**
 * A variant of the MapPartitionsRDD that passes the partition index into the
 * closure. This can be used to generate or collect partition specific
 * information such as the number of tuples in a partition.
 */
private[spark]
class MapPartitionsWithIndexRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean
  ) extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = if (preservesPartitioning) prev.partitioner else None

  override def compute(split: Partition, context: TaskContext) =
    f(split.index, firstParent[T].iterator(split, context))
}
