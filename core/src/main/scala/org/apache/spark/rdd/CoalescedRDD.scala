/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.util.Utils

/**
 * Class that captures a coalesced RDD by essentially keeping track of parent partitions
 * @param index of this coalesced partition
 * @param rdd which it belongs to
 * @param parentsIndices list of indices in the parent that have been coalesced into this partition
 * @param preferredLocation the preferred location for this partition
 */
private[spark] case class CoalescedRDDPartition(
    index: Int,
    @transient rdd: RDD[_],
    parentsIndices: Array[Int],
    @transient preferredLocation: Option[String] = None) extends Partition {
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent partition at the time of task serialization
    parents = parentsIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }

  /**
   * Computes the fraction of the parents' partitions containing preferredLocation within
   * their getPreferredLocs.
   * @return locality of this coalesced partition between 0 and 1
   */
  def localFraction: Double = {
    val loc = parents.count { p =>
      val parentPreferredLocations = rdd.context.getPreferredLocs(rdd, p.index).map(_.host)
      preferredLocation.exists(parentPreferredLocations.contains)
    }
    if (parents.isEmpty) 0.0 else loc.toDouble / parents.size.toDouble
  }
}

/**
 * Represents a coalesced RDD that has fewer partitions than its parent RDD
 * This class uses the PartitionCoalescer class to find a good partitioning of the parent RDD
 * so that each new partition has roughly the same number of parent partitions and that
 * the preferred location of each new partition overlaps with as many preferred locations of its
 * parent partitions
 * @param prev RDD to be coalesced
 * @param maxPartitions number of desired partitions in the coalesced RDD (must be positive)
 * @param partitionCoalescer [[PartitionCoalescer]] implementation to use for coalescing
 */
private[spark] class CoalescedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    maxPartitions: Int,
    partitionCoalescer: Option[PartitionCoalescer] = None)
  extends RDD[T](prev.context, Nil) {  // Nil since we implement getDependencies

  require(maxPartitions > 0 || maxPartitions == prev.partitions.length,
    s"Number of partitions ($maxPartitions) must be positive.")
  if (partitionCoalescer.isDefined) {
    require(partitionCoalescer.get.isInstanceOf[Serializable],
      "The partition coalescer passed in must be serializable.")
  }

  override def getPartitions: Array[Partition] = {
    val pc = partitionCoalescer.getOrElse(new DefaultPartitionCoalescer())

    pc.coalesce(maxPartitions, prev).zipWithIndex.map {
      case (pg, i) =>
        val ids = pg.partitions.map(_.index).toArray
        CoalescedRDDPartition(i, prev, ids, pg.prefLoc)
    }
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    partition.asInstanceOf[CoalescedRDDPartition].parents.iterator.flatMap { parentPartition =>
      firstParent[T].iterator(parentPartition, context)
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices
    })
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  /**
   * Returns the preferred machine for the partition. If split is of type CoalescedRDDPartition,
   * then the preferred machine will be one which most parent splits prefer too.
   * @param partition the partition for which to retrieve the preferred machine, if exists
   * @return the machine most preferred by split
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[CoalescedRDDPartition].preferredLocation.toSeq
  }
}

/**
 * Coalesce the partitions of a parent RDD (`prev`) into fewer partitions, so that each partition of
 * this RDD computes one or more of the parent ones. It will produce exactly `maxPartitions` if the
 * parent had more than maxPartitions, or fewer if the parent had fewer.
 *
 * This transformation is useful when an RDD with many partitions gets filtered into a smaller one,
 * or to avoid having a large number of small tasks when processing a directory with many files.
 *
 * If there is no locality information (no preferredLocations) in the parent, then the coalescing
 * is very simple: chunk parents that are close in the Array in chunks.
 * If there is locality information, it proceeds to pack them with the following four goals:
 *
 * (1) Balance the groups so they roughly have the same number of parent partitions
 * (2) Achieve locality per partition, i.e. find one machine which most parent partitions prefer
 * (3) Be efficient, i.e. O(n) algorithm for n parent partitions (problem is likely NP-hard)
 * (4) Balance preferred machines, i.e. avoid as much as possible picking the same preferred machine
 *
 * Furthermore, it is assumed that the parent RDD may have many partitions, e.g. 100 000.
 * We assume the final number of desired partitions is small, e.g. less than 1000.
 *
 * The algorithm tries to assign unique preferred machines to each partition. If the number of
 * desired partitions is greater than the number of preferred machines (can happen), it needs to
 * start picking duplicate preferred machines. This is determined using coupon collector estimation
 * (2n log(n)). The load balancing is done using power-of-two randomized bins-balls with one twist:
 * it tries to also achieve locality. This is done by allowing a slack (balanceSlack, where
 * 1.0 is all locality, 0 is all balance) between two bins. If two bins are within the slack
 * in terms of balance, the algorithm will assign partitions according to locality.
 * (contact alig for questions)
 */

private class DefaultPartitionCoalescer(val balanceSlack: Double = 0.10)
  extends PartitionCoalescer {

  implicit object partitionGroupOrdering extends Ordering[PartitionGroup] {
    override def compare(o1: PartitionGroup, o2: PartitionGroup): Int =
      java.lang.Integer.compare(o1.numPartitions, o2.numPartitions)
  }


  val rnd = new scala.util.Random(7919) // keep this class deterministic

  // each element of groupArr represents one coalesced partition
  val groupArr = ArrayBuffer[PartitionGroup]()

  // hash used to check whether some machine is already in groupArr
  val groupHash = mutable.Map[String, ArrayBuffer[PartitionGroup]]()

  // hash used for the first maxPartitions (to avoid duplicates)
  val initialHash = mutable.Set[Partition]()

  var noLocality = true  // if true if no preferredLocations exists for parent RDD

  // gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
  def currPrefLocs(part: Partition, prev: RDD[_]): Seq[String] = {
    prev.context.getPreferredLocs(prev, part.index).map(tl => tl.host)
  }

  private class PartitionLocations(prev: RDD[_]) {

    // contains all the partitions from the previous RDD that don't have preferred locations
    val partsWithoutLocs = ArrayBuffer[Partition]()
    // contains all the partitions from the previous RDD that have preferred locations
    val partsWithLocs = ArrayBuffer[(String, Partition)]()

    getAllPrefLocs(prev)

    // gets all the preferred locations of the previous RDD and splits them into partitions
    // with preferred locations and ones without
    def getAllPrefLocs(prev: RDD[_]): Unit = {
      val tmpPartsWithLocs = mutable.LinkedHashMap[Partition, Seq[String]]()
      // first get the locations for each partition, only do this once since it can be expensive
      prev.partitions.foreach(p => {
          val locs = currPrefLocs(p, prev)
          if (locs.nonEmpty) {
            tmpPartsWithLocs.put(p, locs)
          } else {
            partsWithoutLocs += p
          }
        }
      )
      // convert it into an array of host to partition
      for (x <- 0 to 2) {
        tmpPartsWithLocs.foreach { parts =>
          val p = parts._1
          val locs = parts._2
          if (locs.size > x) partsWithLocs += ((locs(x), p))
        }
      }
    }
  }

  /**
   * Gets the least element of the list associated with key in groupHash
   * The returned PartitionGroup is the least loaded of all groups that represent the machine "key"
   *
   * @param key string representing a partitioned group on preferred machine key
   * @return Option of [[PartitionGroup]] that has least elements for key
   */
  def getLeastGroupHash(key: String): Option[PartitionGroup] =
    groupHash.get(key).filter(_.nonEmpty).map(_.min)

  def addPartToPGroup(part: Partition, pgroup: PartitionGroup): Boolean = {
    if (!initialHash.contains(part)) {
      pgroup.partitions += part           // already assign this element
      initialHash += part // needed to avoid assigning partitions to multiple buckets
      true
    } else { false }
  }

  /**
   * Initializes targetLen partition groups. If there are preferred locations, each group
   * is assigned a preferredLocation. This uses coupon collector to estimate how many
   * preferredLocations it must rotate through until it has seen most of the preferred
   * locations (2 * n log(n))
   * @param targetLen The number of desired partition groups
   */
  def setupGroups(targetLen: Int, partitionLocs: PartitionLocations): Unit = {
    // deal with empty case, just create targetLen partition groups with no preferred location
    if (partitionLocs.partsWithLocs.isEmpty) {
      (1 to targetLen).foreach(_ => groupArr += new PartitionGroup())
      return
    }

    noLocality = false
    // number of iterations needed to be certain that we've seen most preferred locations
    val expectedCoupons2 = 2 * (math.log(targetLen)*targetLen + targetLen + 0.5).toInt
    var numCreated = 0
    var tries = 0

    // rotate through until either targetLen unique/distinct preferred locations have been created
    // OR (we have went through either all partitions OR we've rotated expectedCoupons2 - in
    // which case we have likely seen all preferred locations)
    val numPartsToLookAt = math.min(expectedCoupons2, partitionLocs.partsWithLocs.length)
    while (numCreated < targetLen && tries < numPartsToLookAt) {
      val (nxt_replica, nxt_part) = partitionLocs.partsWithLocs(tries)
      tries += 1
      if (!groupHash.contains(nxt_replica)) {
        val pgroup = new PartitionGroup(Some(nxt_replica))
        groupArr += pgroup
        addPartToPGroup(nxt_part, pgroup)
        groupHash.put(nxt_replica, ArrayBuffer(pgroup)) // list in case we have multiple
        numCreated += 1
      }
    }
    // if we don't have enough partition groups, create duplicates
    while (numCreated < targetLen) {
      // Copy the preferred location from a random input partition.
      // This helps in avoiding skew when the input partitions are clustered by preferred location.
      val (nxt_replica, nxt_part) = partitionLocs.partsWithLocs(
        rnd.nextInt(partitionLocs.partsWithLocs.length))
      val pgroup = new PartitionGroup(Some(nxt_replica))
      groupArr += pgroup
      groupHash.getOrElseUpdate(nxt_replica, ArrayBuffer()) += pgroup
      addPartToPGroup(nxt_part, pgroup)
      numCreated += 1
    }
  }

  /**
   * Takes a parent RDD partition and decides which of the partition groups to put it in
   * Takes locality into account, but also uses power of 2 choices to load balance
   * It strikes a balance between the two using the balanceSlack variable
   * @param p partition (ball to be thrown)
   * @param balanceSlack determines the trade-off between load-balancing the partitions sizes and
   *                     their locality. e.g., balanceSlack=0.10 means that it allows up to 10%
   *                     imbalance in favor of locality
   * @return partition group (bin to be put in)
   */
  def pickBin(
      p: Partition,
      prev: RDD[_],
      balanceSlack: Double,
      partitionLocs: PartitionLocations): PartitionGroup = {
    val slack = (balanceSlack * prev.partitions.length).toInt
    // least loaded pref locs
    val pref = currPrefLocs(p, prev).flatMap(getLeastGroupHash)
    val prefPart = if (pref.isEmpty) None else Some(pref.min)
    val r1 = rnd.nextInt(groupArr.size)
    val r2 = rnd.nextInt(groupArr.size)
    val minPowerOfTwo = {
      if (groupArr(r1).numPartitions < groupArr(r2).numPartitions) {
        groupArr(r1)
      }
      else {
        groupArr(r2)
      }
    }
    if (prefPart.isEmpty) {
      // if no preferred locations, just use basic power of two
      return minPowerOfTwo
    }

    val prefPartActual = prefPart.get

    // more imbalance than the slack allows
    if (minPowerOfTwo.numPartitions + slack <= prefPartActual.numPartitions) {
      minPowerOfTwo  // prefer balance over locality
    } else {
      prefPartActual // prefer locality over balance
    }
  }

  def throwBalls(
      maxPartitions: Int,
      prev: RDD[_],
      balanceSlack: Double, partitionLocs: PartitionLocations): Unit = {
    if (noLocality) {  // no preferredLocations in parent RDD, no randomization needed
      if (maxPartitions > groupArr.size) { // just return prev.partitions
        for ((p, i) <- prev.partitions.zipWithIndex) {
          groupArr(i).partitions += p
        }
      } else { // no locality available, then simply split partitions based on positions in array
        for (i <- 0 until maxPartitions) {
          val rangeStart = ((i.toLong * prev.partitions.length) / maxPartitions).toInt
          val rangeEnd = (((i.toLong + 1) * prev.partitions.length) / maxPartitions).toInt
          (rangeStart until rangeEnd).foreach{ j => groupArr(i).partitions += prev.partitions(j) }
        }
      }
    } else {
      // It is possible to have unionRDD where one rdd has preferred locations and another rdd
      // that doesn't. To make sure we end up with the requested number of partitions,
      // make sure to put a partition in every group.

      // if we don't have a partition assigned to every group first try to fill them
      // with the partitions with preferred locations
      val partIter = partitionLocs.partsWithLocs.iterator
      groupArr.filter(pg => pg.numPartitions == 0).foreach { pg =>
        while (partIter.hasNext && pg.numPartitions == 0) {
          val (_, nxt_part) = partIter.next()
          if (!initialHash.contains(nxt_part)) {
            pg.partitions += nxt_part
            initialHash += nxt_part
          }
        }
      }

      // if we didn't get one partitions per group from partitions with preferred locations
      // use partitions without preferred locations
      val partNoLocIter = partitionLocs.partsWithoutLocs.iterator
      groupArr.filter(pg => pg.numPartitions == 0).foreach { pg =>
        while (partNoLocIter.hasNext && pg.numPartitions == 0) {
          val nxt_part = partNoLocIter.next()
          if (!initialHash.contains(nxt_part)) {
            pg.partitions += nxt_part
            initialHash += nxt_part
          }
        }
      }

      // finally pick bin for the rest
      for (p <- prev.partitions if (!initialHash.contains(p))) { // throw every partition into group
        pickBin(p, prev, balanceSlack, partitionLocs).partitions += p
      }
    }
  }

  def getPartitions: Array[PartitionGroup] = groupArr.filter( pg => pg.numPartitions > 0).toArray

  /**
   * Runs the packing algorithm and returns an array of PartitionGroups that if possible are
   * load balanced and grouped by locality
    *
    * @return array of partition groups
   */
  def coalesce(maxPartitions: Int, prev: RDD[_]): Array[PartitionGroup] = {
    val partitionLocs = new PartitionLocations(prev)
    // setup the groups (bins)
    setupGroups(math.min(prev.partitions.length, maxPartitions), partitionLocs)
    // assign partitions (balls) to each group (bins)
    throwBalls(maxPartitions, prev, balanceSlack, partitionLocs)
    getPartitions
  }
}
