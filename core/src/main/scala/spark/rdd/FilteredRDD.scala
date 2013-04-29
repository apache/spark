package spark.rdd

import scala.reflect.ClassTag
import spark.{OneToOneDependency, RDD, Partition, TaskContext}

private[spark] class FilteredRDD[T: ClassTag](
    prev: RDD[T],
    f: T => Boolean)
  extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = prev.partitioner    // Since filter cannot change a partition's keys

  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context).filter(f)
}
