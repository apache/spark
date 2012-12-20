package spark.rdd

import java.lang.ref.WeakReference

import spark.{RDD, Split, TaskContext}


private[spark]
class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
    prev: WeakReference[RDD[T]],
    f: T => TraversableOnce[U])
  extends RDD[U](prev.get) {

  override def splits = firstParent[T].splits
  override def compute(split: Split, context: TaskContext) =
    firstParent[T].iterator(split, context).flatMap(f)
}
