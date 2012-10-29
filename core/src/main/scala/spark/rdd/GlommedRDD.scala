package spark.rdd

import spark.OneToOneDependency
import spark.RDD
import spark.Split
import java.lang.ref.WeakReference

private[spark]
class GlommedRDD[T: ClassManifest](@transient prev: WeakReference[RDD[T]])
  extends RDD[Array[T]](prev.get) {
  override def splits = firstParent[T].splits
  override def compute(split: Split) = Array(firstParent[T].iterator(split).toArray).iterator
}