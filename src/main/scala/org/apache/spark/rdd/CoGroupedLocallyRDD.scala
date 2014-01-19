package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.util.collection.AppendOnlyMap
import scala.collection.mutable.ArrayBuffer

/**
 * A RDD that cogroups its already co-partitioned parents. This RDD works the same as
 * a [[org.apache.spark.rdd.CoGroupedRDD]] except that its parents should have the
 * same number of partitions. Like a [[org.apache.spark.rdd.CoGroupedRDD]],
 * for each key k in parent RDDs, the resulting RDD contains a tuple with the list of
 * values for that key.
 *
 * @param rdds parent RDDs.
 */
class CoGroupedLocallyRDD[K](@transient var rdds: Seq[RDD[_ <: Product2[K, _]]])
  extends RDD[(K, Seq[Seq[_]])](rdds.head.context, Nil) {

  {
    // Check if all parents have the same number of partitions.
    // It is possible that a parent RDD does not preserve the partitioner,
    // so we do not check if all of parent RDDs have the same partitioner.
    if (!rdds.forall(rdd => rdd.partitions.size == rdds(0).partitions.size)) {
      throw new IllegalArgumentException(
        "All parent RDDs should have the same number of partitions.")
    }
  }

  // All dependencies of a CoGroupedLocallyRDD should be narrow dependencies.
  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_ <: Product2[K, _]] =>
      logDebug("Adding one-to-one dependency with " + rdd)
      new OneToOneDependency(rdd)
    }
  }

  override def getPartitions: Array[Partition] = {
    val numPartitions = firstParent[(K, _)].partitions.size
    val array = new Array[Partition](numPartitions)
    for (i <- 0 until array.size) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i))
      }.toArray)
    }
    array
  }

  // Take the first not None partitioner. It is possible that all parent partitioners
  // are None.
  override val partitioner = rdds.find(rdd => rdd.partitioner != None) match {
    case Some(rdd) => rdd.partitioner
    case None => None
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(K, Seq[Seq[_]])] = {
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = split.deps.size
    // e.g. for `(k, a) cogroup (k, b)`, K -> Seq(ArrayBuffer as, ArrayBuffer bs)
    val map = new AppendOnlyMap[K, Seq[ArrayBuffer[Any]]]

    val update: (Boolean, Seq[ArrayBuffer[Any]]) => Seq[ArrayBuffer[Any]] = (hadVal, oldVal) => {
      if (hadVal) oldVal else Array.fill(numRdds)(new ArrayBuffer[Any])
    }

    val getSeq = (k: K) => {
      map.changeValue(k, update)
    }

    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) => {
        // Read them from the parent
        rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, Any]]].foreach { kv =>
          getSeq(kv._1)(depNum) += kv._2
        }
      }
      case _ => {
        // We should not reach here. It is a sanity check.
        throw new RuntimeException("A dependency of this CoGroupedLocallyRDD is not " +
          "a narrow dependency.")
      }
    }
    new InterruptibleIterator(context, map.iterator)
  }
}