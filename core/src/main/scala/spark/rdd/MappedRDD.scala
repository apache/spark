package spark.rdd

import spark.RDD
import spark.Split

private[spark]
class MappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => U)
  extends RDD[U](prev) {

  override def getSplits = firstParent[T].splits
  override def compute(split: Split) = firstParent[T].iterator(split).map(f)
}