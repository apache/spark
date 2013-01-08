package spark.rdd

import spark.{OneToOneDependency, RDD, Split, TaskContext}

private[spark] class FilteredRDD[T: ClassManifest](
    prev: RDD[T],
    f: T => Boolean)
  extends RDD[T](prev) {

  override def getSplits = firstParent[T].splits

  override def compute(split: Split, context: TaskContext) =
    firstParent[T].iterator(split, context).filter(f)
}
