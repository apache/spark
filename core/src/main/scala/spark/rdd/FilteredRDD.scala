package spark.rdd

import spark.{OneToOneDependency, RDD, Split, TaskContext}

private[spark] class FilteredRDD[T: ClassManifest](
    prev: RDD[T],
    f: T => Boolean)
  extends RDD[T](prev) {

  override def getSplits: Array[Split] = firstParent[T].splits

  override val partitioner = prev.partitioner    // Since filter cannot change a partition's keys

  override def compute(split: Split, context: TaskContext) =
    firstParent[T].iterator(split, context).filter(f)
}
