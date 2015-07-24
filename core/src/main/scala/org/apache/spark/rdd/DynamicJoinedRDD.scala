package org.apache.spark.rdd

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi

/**
 * Strategy for joining each partition in a [[DynamicJoinedRDD]]. Possible values are:
 * - BRING_TO_1: broadcast the partitions of RDD 2 and bring them to the nodes containing the
 *   partitions of RDD 1 to do a local broadcast join
 * - BRING_TO_2: broadcast the partitions of RDD 1 and bring them to the nodes containing the
 *   partitions of RDD 2 to do a local broadcast join
 * - SHUFFLE: shuffle the partitions of both RDDs to the same reduce task
 */
@DeveloperApi
object PartitionStrategy extends Enumeration {
  type PartitionStrategy = Value

  val BRING_TO_1, BRING_TO_2, SHUFFLE = Value
}

/**
 * :: DeveloperApi ::
 * An RDD that supports different join strategies by partition. It takes two ShuffleDependencies
 * that should have the same partitioner and will produce the input sides of the join, as well as
 * arrays describing what to do with each partition (which join strategy to use and, if shuffling
 * it, which reduce partition to shuffle data to). The resulting RDD will have one partition for
 * each shufflePartitonId used for shuffled partitions, plus one for each partition that data
 * is brought to (when we broadcast one RDD's data and bring it to all partitions of the other).
 *
 * @param dep1 shuffle dependency for first RDD
 * @param dep2 shuffle dependency for second RDD (must have same partitioner as dep1)
 * @param partitionStrategies [[PartitionStrategy]] to use for each partition; must be same size
 *   as the number of partitions of the dependencies' partitioners
 * @param shufflePartitionIds for partitions where the strategy is set to SHUFFLE, holds the
 *   destination partition ID to use (which is useful for coalescing data into fewer partitions);
 *   these IDs must be consecutive numbers from 0 to a maximum value, and the array of values must
 *   be non-decreasing (e.g. [0, 0, 0, 1, 1, 2, 2, 3])
 */
@DeveloperApi
class DynamicJoinedRDD[K, V, W](
    var dep1: ShuffleDependency[K, V, V],
    var dep2: ShuffleDependency[K, W, W],
    val partitionStrategies: Array[PartitionStrategy.PartitionStrategy],
    val shufflePartitionIds: Array[Int])
  extends RDD[(K, (V, W))](dep1.rdd.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = List(dep1, dep2)

  override protected def getPartitions: Array[Partition] = {
    val parentPartitions = dep1.partitioner.numPartitions
    var maxShufflePartitionId = -1
    var numBringTo1 = 0
    var numBringTo2 = 0
    for (i <- 0 until parentPartitions) {
      if (partitionStrategies(i) == PartitionStrategy.BRING_TO_1) {
        numBringTo1 += 1
      } else if (partitionStrategies(i) == PartitionStrategy.BRING_TO_2) {
        numBringTo2 += 1
      } else {
        maxShufflePartitionId = math.max(maxShufflePartitionId, shufflePartitionIds(i))
      }
    }

    val numShufflePartitions = maxShufflePartitionId + 1  // Works because this starts at -1
    val totalPartitions = numShufflePartitions + (numBringTo1 + numBringTo2) * parentPartitions

    val partitions = new Array[Partition](totalPartitions)

    // Build up the partitions in this order:
    // - 0 to maxShufflePartitionId: shuffled partitions
    // - next numBringTo1 * parentPartitions partitions: partitions brought to RDD 1
    // - next numBringTo2 * parentPartitions partitions: partitions brought to RDD 2
    var pos = 0
    for (shufflePart <- 0 until numShufflePartitions) {
      while (pos < parentPartitions && partitionStrategies(pos) != PartitionStrategy.SHUFFLE) {
        pos += 1
      }
      val startPos = pos
      while (pos < parentPartitions && (shufflePartitionIds(pos) == shufflePart ||
          partitionStrategies(pos) != PartitionStrategy.SHUFFLE)) {
        pos += 1
      }
      val endPos = pos
      partitions(shufflePart) = new DynamicJoinedRDDPartition(
        shufflePart, PartitionStrategy.SHUFFLE, startPos, endPos, startPos, endPos, true)
    }

    var outputPos = numShufflePartitions
    for (i <- 0 until parentPartitions) {
      if (partitionStrategies(i) == PartitionStrategy.BRING_TO_1) {
        for (j <- 0 until parentPartitions) {
          partitions(outputPos) = new DynamicJoinedRDDPartition(
            outputPos, PartitionStrategy.BRING_TO_1, j, j + 1, i, i + 1, false)
          outputPos += 1
        }
      } else if (partitionStrategies(i) == PartitionStrategy.BRING_TO_2) {
        for (j <- 0 until parentPartitions) {
          partitions(outputPos) = new DynamicJoinedRDDPartition(
            outputPos, PartitionStrategy.BRING_TO_2, i, i + 1, j, j + 1, true)
          outputPos += 1
        }
      }
    }

    assert(outputPos == totalPartitions)
    partitions
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[(K, (V, W))] = {
    val part = partition.asInstanceOf[DynamicJoinedRDDPartition]
    val iter1 = SparkEnv.get.shuffleManager.getReader(
      dep1.shuffleHandle, part.rdd1StartPartition, part.rdd1EndPartition, context)
      .read()
      .asInstanceOf[Iterator[(K, V)]]
    val iter2 = SparkEnv.get.shuffleManager.getReader(
      dep2.shuffleHandle, part.rdd2StartPartition, part.rdd2EndPartition, context)
      .read()
      .asInstanceOf[Iterator[(K, W)]]
    // This is a pretty slow way to join stuff, but it's just to show it
    if (part.hash1) {
      val map = new HashMap[K, ArrayBuffer[V]]
      for (pair <- iter1) {
        map.getOrElseUpdate(pair._1, new ArrayBuffer()) += pair._2
      }
      iter2.flatMap { pair =>
        val k = pair._1
        val w = pair._2
        val values = map.getOrElse(k, Nil)
        values.map(v => (k, (v, w)))
      }
    } else {
      val map = new HashMap[K, ArrayBuffer[W]]
      for (pair <- iter2) {
        map.getOrElseUpdate(pair._1, new ArrayBuffer()) += pair._2
      }
      iter1.flatMap { pair =>
        val k = pair._1
        val v = pair._2
        val values = map.getOrElse(k, Nil)
        values.map(w => (k, (v, w)))
      }
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val part = partition.asInstanceOf[DynamicJoinedRDDPartition]
    if (part.strategy == PartitionStrategy.BRING_TO_1) {
      // TODO: should support passing loc.executorId as a preferred location too
      val tracker = SparkEnv.get.mapOutputTracker
      tracker.getMapOutputLocation(dep1.shuffleId, part.rdd1StartPartition).map(_.host).toList
    } else if (part.strategy == PartitionStrategy.BRING_TO_2) {
      // TODO: should support passing loc.executorId as a preferred location too
      val tracker = SparkEnv.get.mapOutputTracker
      tracker.getMapOutputLocation(dep2.shuffleId, part.rdd2StartPartition).map(_.host).toList
    } else {
      Nil
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    dep1 = null
    dep2 = null
  }
}

/**
 * A partition in [[DynamicJoinedRDD]]; contains its index in the final RDD, the ranges of
 * partitions to request from rdd1 and rdd2, and whether to hash rdd1 or rdd2's tuples.
 */
class DynamicJoinedRDDPartition(
    val index: Int,
    val strategy: PartitionStrategy.PartitionStrategy,
    val rdd1StartPartition: Int,
    val rdd1EndPartition: Int,
    val rdd2StartPartition: Int,
    val rdd2EndPartition: Int,
    val hash1: Boolean)
  extends Partition {

  override def toString =
    s"DynamicJoinedPartition($index,$strategy,$rdd1StartPartition,$rdd1EndPartition,$rdd2StartPartition,$rdd2EndPartition,$hash1)"
}
