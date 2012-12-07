package spark.rdd

import spark.RDD
import spark.Split

private[spark]
class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => TraversableOnce[U])
  extends RDD[U](prev) {
  
  override def splits = firstParent[T].splits
  override def compute(split: Split) = firstParent[T].iterator(split).flatMap(f)
}
