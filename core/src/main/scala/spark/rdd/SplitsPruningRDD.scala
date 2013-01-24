package spark.rdd

import spark.{PruneDependency, RDD, SparkEnv, Split, TaskContext}

/**
 * A RDD used to prune RDD splits so we can avoid launching tasks on
 * all splits. An example use case: If we know the RDD is partitioned by range,
 * and the execution DAG has a filter on the key, we can avoid launching tasks
 * on splits that don't have the range covering the key.
 */
class SplitsPruningRDD[T: ClassManifest](
    prev: RDD[T],
    @transient splitsFilterFunc: Int => Boolean)
  extends RDD[T](prev.context, List(new PruneDependency(prev, splitsFilterFunc))) {

  @transient
  val _splits: Array[Split] = dependencies_.head.asInstanceOf[PruneDependency[T]].splits

  override def compute(split: Split, context: TaskContext) = firstParent[T].iterator(split, context)

  override protected def getSplits = _splits

  override val partitioner = prev.partitioner
}
