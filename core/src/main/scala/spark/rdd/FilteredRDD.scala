package spark.rdd

import spark.{OneToOneDependency, RDD, Split, TaskContext}


private[spark]
class FilteredRDD[T: ClassManifest](prev: RDD[T], f: T => Boolean) extends RDD[T](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split, context: TaskContext) = prev.iterator(split, context).filter(f)
}