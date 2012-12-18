package spark.rdd

import spark.RDD
import spark.Split

private[spark]
class FilteredRDD[T: ClassManifest](
    prev: RDD[T],
    f: T => Boolean)
  extends RDD[T](prev) {

  override def getSplits = firstParent[T].splits
  override def compute(split: Split) = firstParent[T].iterator(split).filter(f)
}