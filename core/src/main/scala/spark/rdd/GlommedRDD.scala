package spark.rdd

import spark.{RDD, Split, TaskContext}

private[spark] class GlommedRDD[T: ClassManifest](prev: RDD[T])
  extends RDD[Array[T]](prev) {

  override def getSplits: Array[Split] = firstParent[T].splits

  override def compute(split: Split, context: TaskContext) =
    Array(firstParent[T].iterator(split, context).toArray).iterator
}
