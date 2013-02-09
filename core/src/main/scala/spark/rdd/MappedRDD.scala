package spark.rdd

import spark.{RDD, Split, TaskContext}

private[spark]
class MappedRDD[U: ClassManifest, T: ClassManifest](prev: RDD[T], f: T => U)
  extends RDD[U](prev) {

  override def getSplits: Array[Split] = firstParent[T].splits

  override def compute(split: Split, context: TaskContext) =
    firstParent[T].iterator(split, context).map(f)
}
