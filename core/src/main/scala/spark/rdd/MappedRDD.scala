package spark.rdd

import spark.{OneToOneDependency, RDD, Split, TaskContext}

private[spark]
class MappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => U)
  extends RDD[U](prev.context) {

  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split, taskContext: TaskContext) =
    prev.iterator(split, taskContext).map(f)
}