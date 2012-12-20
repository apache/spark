package spark.rdd

import java.lang.ref.WeakReference

import spark.{RDD, Split, TaskContext}


/**
 * A variant of the MapPartitionsRDD that passes the split index into the
 * closure. This can be used to generate or collect partition specific
 * information such as the number of tuples in a partition.
 */
private[spark]
class MapPartitionsWithSplitRDD[U: ClassManifest, T: ClassManifest](
    prev: WeakReference[RDD[T]],
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean)
  extends RDD[U](prev.get) {

  override val partitioner = if (preservesPartitioning) prev.get.partitioner else None
  override def splits = firstParent[T].splits
  override def compute(split: Split, context: TaskContext) =
    f(split.index, firstParent[T].iterator(split, context))
}