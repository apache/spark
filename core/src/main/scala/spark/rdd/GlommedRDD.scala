package spark.rdd

import spark.OneToOneDependency
import spark.RDD
import spark.Split

private[spark]
class GlommedRDD[T: ClassManifest](prev: RDD[T]) extends RDD[Array[T]](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = Array(prev.iterator(split).toArray).iterator
}