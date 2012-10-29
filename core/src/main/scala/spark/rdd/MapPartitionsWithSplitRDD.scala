package spark.rdd

import spark.OneToOneDependency
import spark.RDD
import spark.Split
import java.lang.ref.WeakReference

/**
 * A variant of the MapPartitionsRDD that passes the split index into the
 * closure. This can be used to generate or collect partition specific
 * information such as the number of tuples in a partition.
 */
private[spark]
class MapPartitionsWithSplitRDD[U: ClassManifest, T: ClassManifest](
    @transient prev: WeakReference[RDD[T]],
    f: (Int, Iterator[T]) => Iterator[U])
  extends RDD[U](prev.get) {

  override def splits = firstParent[T].splits
  override def compute(split: Split) = f(split.index, firstParent[T].iterator(split))
}