package spark.rdd

import spark.{OneToOneDependency, RDD, Split, TaskContext}


private[spark]
class MapPartitionsRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: Iterator[T] => Iterator[U],
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev.context) {

  override val partitioner = if (preservesPartitioning) prev.partitioner else None

  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split, context: TaskContext) = f(prev.iterator(split, context))
}