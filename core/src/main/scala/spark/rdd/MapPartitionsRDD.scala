package spark.rdd

import spark.{RDD, Partition, TaskContext}


private[spark]
class MapPartitionsRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: Iterator[T] => Iterator[U],
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner =
    if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) =
    f(firstParent[T].iterator(split, context))
}
