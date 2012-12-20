package spark.rdd

import java.lang.ref.WeakReference

import spark.{OneToOneDependency, RDD, Split, TaskContext}

private[spark]
class FilteredRDD[T: ClassManifest](prev: WeakReference[RDD[T]], f: T => Boolean)
  extends RDD[T](prev.get) {

  override def splits = firstParent[T].splits
  override def compute(split: Split, context: TaskContext) =
    firstParent[T].iterator(split, context).filter(f)
}