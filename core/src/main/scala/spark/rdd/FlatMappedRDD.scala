package spark.rdd

import spark.{OneToOneDependency, RDD, Split, TaskContext}

private[spark]
class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => TraversableOnce[U])
  extends RDD[U](prev.context) {

  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))

  override def compute(split: Split, context: TaskContext) =
    prev.iterator(split, context).flatMap(f)
}
