package spark.rdd

import spark.{OneToOneDependency, RDD, Split, TaskContext}


private[spark]
class GlommedRDD[T: ClassManifest](prev: RDD[T]) extends RDD[Array[T]](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split, context: TaskContext) =
    Array(prev.iterator(split, context).toArray).iterator
}