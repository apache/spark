package spark.rdd

import spark.RDD
import spark.Split

private[spark]
class GlommedRDD[T: ClassManifest](prev: RDD[T])
  extends RDD[Array[T]](prev) {
  override def getSplits = firstParent[T].splits
  override def compute(split: Split) = Array(firstParent[T].iterator(split).toArray).iterator
}