package spark.rdd

import spark.{NarrowDependency, RDD, SparkEnv, Split, TaskContext}


class PartitionPruningRDDSplit(idx: Int, val parentSplit: Split) extends Split {
  override val index = idx
}


/**
 * Represents a dependency between the PartitionPruningRDD and its parent. In this
 * case, the child RDD contains a subset of partitions of the parents'.
 */
class PruneDependency[T](rdd: RDD[T], @transient partitionFilterFunc: Int => Boolean)
  extends NarrowDependency[T](rdd) {

  @transient
  val partitions: Array[Split] = rdd.splits.filter(s => partitionFilterFunc(s.index))
    .zipWithIndex.map { case(split, idx) => new PartitionPruningRDDSplit(idx, split) : Split }

  override def getParents(partitionId: Int) = List(partitions(partitionId).index)
}


/**
 * A RDD used to prune RDD partitions/splits so we can avoid launching tasks on
 * all partitions. An example use case: If we know the RDD is partitioned by range,
 * and the execution DAG has a filter on the key, we can avoid launching tasks
 * on partitions that don't have the range covering the key.
 */
class PartitionPruningRDD[T: ClassManifest](
    @transient prev: RDD[T],
    @transient partitionFilterFunc: Int => Boolean)
  extends RDD[T](prev.context, List(new PruneDependency(prev, partitionFilterFunc))) {

  override def compute(split: Split, context: TaskContext) = firstParent[T].iterator(
    split.asInstanceOf[PartitionPruningRDDSplit].parentSplit, context)

  override protected def getSplits: Array[Split] =
    getDependencies.head.asInstanceOf[PruneDependency[T]].partitions
}
