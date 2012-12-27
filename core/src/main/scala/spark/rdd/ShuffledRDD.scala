package spark.rdd

import spark.{Partitioner, RDD, SparkEnv, ShuffleDependency, Split, TaskContext}

private[spark] class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

/**
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param prev the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 */
class ShuffledRDD[K, V](
    prev: RDD[(K, V)],
    part: Partitioner)
  extends RDD[(K, V)](prev.context, List(new ShuffleDependency(prev, part))) {

  override val partitioner = Some(part)

  @transient
  var splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def getSplits = splits_

  override def compute(split: Split, context: TaskContext): Iterator[(K, V)] = {
    val shuffledId = dependencies.head.asInstanceOf[ShuffleDependency[K, V]].shuffleId
    SparkEnv.get.shuffleFetcher.fetch[K, V](shuffledId, split.index)
  }

  override def clearDependencies() {
    splits_ = null
  }
}
