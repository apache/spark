package spark.rdd

import spark.{RDD, Partition, TaskContext}

private[spark]
class MappedRDD[U: ClassManifest, T: ClassManifest](prev: RDD[T], f: T => U)
  extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context).map(f)
}
