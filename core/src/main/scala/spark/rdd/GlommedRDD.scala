package spark.rdd

import java.lang.ref.WeakReference

import spark.{RDD, Split, TaskContext}

private[spark]
class GlommedRDD[T: ClassManifest](prev: WeakReference[RDD[T]])
  extends RDD[Array[T]](prev.get) {
  override def splits = firstParent[T].splits
  override def compute(split: Split, context: TaskContext) =
    Array(firstParent[T].iterator(split, context).toArray).iterator
}
