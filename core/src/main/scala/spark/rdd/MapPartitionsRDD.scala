package spark.rdd

import spark.RDD
import spark.Split

private[spark]
class MapPartitionsRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: Iterator[T] => Iterator[U],
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None
  
  override def getSplits = firstParent[T].splits
  override def compute(split: Split) = f(firstParent[T].iterator(split))
}