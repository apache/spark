package spark.rdd

import spark.OneToOneDependency
import spark.RDD
import spark.Split
import java.lang.ref.WeakReference

private[spark]
class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
    @transient prev: WeakReference[RDD[T]],
    f: T => TraversableOnce[U])
  extends RDD[U](prev.get) {
  
  override def splits = firstParent[T].splits
  override def compute(split: Split) = firstParent[T].iterator(split).flatMap(f)
}
