package spark.rdd

import spark.OneToOneDependency
import spark.RDD
import spark.Split
import java.lang.ref.WeakReference

private[spark]
class MappedRDD[U: ClassManifest, T: ClassManifest](
    prev: WeakReference[RDD[T]],
    f: T => U)
  extends RDD[U](prev.get) {

  override def splits = firstParent[T].splits
  override def compute(split: Split) = firstParent[T].iterator(split).map(f)
}