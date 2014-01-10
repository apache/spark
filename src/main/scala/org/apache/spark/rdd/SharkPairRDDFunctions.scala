package org.apache.spark.rdd

import scala.reflect._
import org.apache.spark._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Aggregator
import org.apache.spark.SparkContext._

import scala.language.implicitConversions

/**
 * Extra functions for Shark available on RDDs of (key, value) pairs through an implicit conversion.
 * Import `org.apache.spark.SharkPairRDDFunctions._` at the top of your program to use these functions.
 */
class SharkPairRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)])
  extends Logging
  with Serializable {

  /**
   * Cogroup corresponding partitions of `this` and `other`. These two RDDs should have
   * the same number of partitions. Partitions of these two RDDs are cogrouped
   * according to the indexes of partitions. If we have two RDDs and
   * each of them has n partitions, we will cogroup the partition i from `this`
   * with the partition i from `other`.
   * This function will not introduce a shuffling operation.
   */
  def cogroupLocally[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    val cg = new CoGroupedLocallyRDD[K](Seq(self, other))
    val prfs = new PairRDDFunctions[K, Seq[Seq[_]]](cg)(classTag[K], ClassTags.seqSeqClassTag)
    prfs.mapValues { case Seq(vs, ws) =>
      (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]])
    }
  }

  /**
   * Group the values for each key within a partition of the RDD into a single sequence.
   * This function will not introduce a shuffling operation.
   */
  def groupByKeyLocally(): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    val aggregator = new Aggregator[K, V, ArrayBuffer[V]](createCombiner _, mergeValue _, null)
    val bufs = self.mapPartitionsWithContext((context, iter) => {
      new InterruptibleIterator(context, aggregator.combineValuesByKey(iter))
    }, preservesPartitioning = true)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
  }

  /**
   * Join corresponding partitions of `this` and `other`.
   * If we have two RDDs and each of them has n partitions,
   * we will join the partition i from `this` with the partition i from `other`.
   * This function will not introduce a shuffling operation.
   */
  def joinLocally[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    cogroupLocally(other).flatMapValues { case (vs, ws) =>
      for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
  }
}

object SharkPairRDDFunctions {
  implicit def rddToSharkPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) =
    new SharkPairRDDFunctions(rdd)
}


