package spark.rdd

import spark.{PruneDependency, RDD, SparkEnv, Split, TaskContext}


/**
 * A RDD used to prune RDD partitions/splits so we can avoid launching tasks on
 * all partitions. An example use case: If we know the RDD is partitioned by range,
 * and the execution DAG has a filter on the key, we can avoid launching tasks
 * on partitions that don't have the range covering the key.
 *
 * TODO: This currently doesn't give partition IDs properly!
 */
class PartitionPruningRDD[T: ClassManifest](
    @transient prev: RDD[T],
    @transient partitionFilterFunc: Int => Boolean)
  extends RDD[T](prev.context, List(new PruneDependency(prev, partitionFilterFunc))) {

  override def compute(split: Split, context: TaskContext) = firstParent[T].iterator(
    split.asInstanceOf[PruneDependency.PartitionPruningRDDSplit].parentSplit, context)

  override protected def getSplits =
    getDependencies.head.asInstanceOf[PruneDependency[T]].partitions

  override val partitioner = firstParent[T].partitioner
}
