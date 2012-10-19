package spark.rdd

import scala.collection.mutable.ArrayBuffer
import java.util.{HashMap => JHashMap}

import spark.Aggregator
import spark.Partitioner
import spark.RangePartitioner
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
 */
abstract class ShuffledRDD[K, V, C](
    @transient parent: RDD[(K, V)],
    aggregator: Option[Aggregator[K, V, C]],
    part: Partitioner)
  extends RDD[(K, C)](parent.context) {

  override val partitioner = Some(part)

  @transient
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_

  override def preferredLocations(split: Split) = Nil

  val dep = new ShuffleDependency(parent, aggregator, part)
  override val dependencies = List(dep)
}


/**
 * Repartition a key-value pair RDD.
 */
class RepartitionShuffledRDD[K, V](
    @transient parent: RDD[(K, V)],
    part: Partitioner)
  extends ShuffledRDD[K, V, V](
    parent,
    None,
    part) {

  override def compute(split: Split): Iterator[(K, V)] = {
    val buf = new ArrayBuffer[(K, V)]
    val fetcher = SparkEnv.get.shuffleFetcher
    def addTupleToBuffer(k: K, v: V) = { buf += Tuple(k, v) }
    fetcher.fetch[K, V](dep.shuffleId, split.index, addTupleToBuffer)
    buf.iterator
  }
}


/**
 * A sort-based shuffle (that doesn't apply aggregation). It does so by first
 * repartitioning the RDD by range, and then sort within each range.
 */
class ShuffledSortedRDD[K <% Ordered[K]: ClassManifest, V](
    @transient parent: RDD[(K, V)],
    ascending: Boolean,
    numSplits: Int)
  extends RepartitionShuffledRDD[K, V](
    parent,
    new RangePartitioner(numSplits, parent, ascending)) {

  override def compute(split: Split): Iterator[(K, V)] = {
    // By separating this from RepartitionShuffledRDD, we avoided a
    // buf.iterator.toArray call, thus avoiding building up the buffer twice.
    val buf = new ArrayBuffer[(K, V)]
    def addTupleToBuffer(k: K, v: V) { buf += ((k, v)) }
    SparkEnv.get.shuffleFetcher.fetch[K, V](dep.shuffleId, split.index, addTupleToBuffer)
    if (ascending) {
      buf.sortWith((x, y) => x._1 < y._1).iterator
    } else {
      buf.sortWith((x, y) => x._1 > y._1).iterator
    }
  }
}


/**
 * The resulting RDD from shuffle and running (hash-based) aggregation.
 */
class ShuffledAggregatedRDD[K, V, C](
    @transient parent: RDD[(K, V)],
    aggregator: Aggregator[K, V, C],
    part : Partitioner)
  extends ShuffledRDD[K, V, C](parent, Some(aggregator), part) {

  override def compute(split: Split): Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    val fetcher = SparkEnv.get.shuffleFetcher

    if (aggregator.mapSideCombine) {
      // Apply combiners on map partitions. In this case, post-shuffle we get a
      // list of outputs from the combiners and merge them using mergeCombiners.
      def mergePairWithMapSideCombiners(k: K, c: C) {
        val oldC = combiners.get(k)
        if (oldC == null) {
          combiners.put(k, c)
        } else {
          combiners.put(k, aggregator.mergeCombiners(oldC, c))
        }
      }
      fetcher.fetch[K, C](dep.shuffleId, split.index, mergePairWithMapSideCombiners)
    } else {
      // Do not apply combiners on map partitions (i.e. map side aggregation is
      // turned off). Post-shuffle we get a list of values and we use mergeValue
      // to merge them.
      def mergePairWithoutMapSideCombiners(k: K, v: V) {
        val oldC = combiners.get(k)
        if (oldC == null) {
          combiners.put(k, aggregator.createCombiner(v))
        } else {
          combiners.put(k, aggregator.mergeValue(oldC, v))
        }
      }
      fetcher.fetch[K, V](dep.shuffleId, split.index, mergePairWithoutMapSideCombiners)
    }

    return new Iterator[(K, C)] {
      var iter = combiners.entrySet().iterator()

      def hasNext: Boolean = iter.hasNext()

      def next(): (K, C) = {
        val entry = iter.next()
        (entry.getKey, entry.getValue)
      }
    }
  }
}
