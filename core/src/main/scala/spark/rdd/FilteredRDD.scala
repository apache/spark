package spark.rdd

import spark.OneToOneDependency
import spark.RDD
import spark.Split
import java.lang.ref.WeakReference

private[spark]
class FilteredRDD[T: ClassManifest](
    prev: WeakReference[RDD[T]],
    f: T => Boolean)
  extends RDD[T](prev.get) {

  override def splits = firstParent[T].splits
  override def compute(split: Split) = firstParent[T].iterator(split).filter(f)
}