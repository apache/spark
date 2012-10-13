package spark.rdd

import spark.Partitioner
import spark.RDD
import spark.ShuffleDependency
import spark.SparkEnv
import spark.Split

private[spark] class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

/**
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param parent the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 */
class ShuffledRDD[K, V](
    @transient parent: RDD[(K, V)],
    part: Partitioner) extends RDD[(K, V)](parent.context) {

  override val partitioner = Some(part)

  @transient
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_

  override def preferredLocations(split: Split) = Nil

  val dep = new ShuffleDependency(context.newShuffleId, parent, part)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, V)] = {
    SparkEnv.get.shuffleFetcher.fetch[K, V](dep.shuffleId, split.index)
  }
}