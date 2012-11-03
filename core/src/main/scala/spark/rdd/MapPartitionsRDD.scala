package spark.rdd

import spark.OneToOneDependency
import spark.RDD
import spark.Split
import java.lang.ref.WeakReference

private[spark]
class MapPartitionsRDD[U: ClassManifest, T: ClassManifest](
    prev: WeakReference[RDD[T]],
    f: Iterator[T] => Iterator[U],
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev.get) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None
  
  override def splits = firstParent[T].splits
  override def compute(split: Split) = f(firstParent[T].iterator(split))
}