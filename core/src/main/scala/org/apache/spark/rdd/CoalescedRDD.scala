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

import org.apache.spark._
import java.io.{ObjectOutputStream, IOException}
import scala.collection.mutable
import scala.Some
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Class that captures a coalesced RDD by essentially keeping track of parent partitions
 * @param index of this coalesced partition
 * @param rdd which it belongs to
 * @param parentsIndices list of indices in the parent that have been coalesced into this partition
 * @param preferredLocation the preferred location for this partition
 */
case class CoalescedRDDPartition(
                                  index: Int,
                                  @transient rdd: RDD[_],
                                  parentsIndices: Array[Int],
                                  @transient preferredLocation: String = ""
                                  ) extends Partition {
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent partition at the time of task serialization
    parents = parentsIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }

  /**
   * Computes how many of the parents partitions have getPreferredLocation
   * as one of their preferredLocations
   * @return locality of this coalesced partition between 0 and 1
   */
  def localFraction: Double = {
    val loc = parents.count(p =>
      rdd.context.getPreferredLocs(rdd, p.index).map(tl => tl.host).contains(preferredLocation))

    if (parents.size == 0) 0.0 else (loc.toDouble / parents.size.toDouble)
  }
}

/**
 * Represents a coalesced RDD that has fewer partitions than its parent RDD
 * This class uses the PartitionCoalescer class to find a good partitioning of the parent RDD
 * so that each new partition has roughly the same number of parent partitions and that
 * the preferred location of each new partition overlaps with as many preferred locations of its
 * parent partitions
 * @param prev RDD to be coalesced
 * @param maxPartitions number of desired partitions in the coalesced RDD
 * @param balanceSlack used to trade-off balance and locality. 1.0 is all locality, 0 is all balance
 */
class CoalescedRDD[T: ClassTag](
                                      @transient var prev: RDD[T],
                                      maxPartitions: Int,
                                      balanceSlack: Double = 0.10)
  extends RDD[T](prev.context, Nil) {  // Nil since we implement getDependencies

  override def getPartitions: Array[Partition] = {
    val pc = new PartitionCoalescer(maxPartitions, prev, balanceSlack)

    pc.run().zipWithIndex.map {
      case (pg, i) =>
        val ids = pg.arr.map(_.index).toArray
        new CoalescedRDDPartition(i, prev, ids, pg.prefLoc)
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

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  /**
   * Returns the preferred machine for the partition. If split is of type CoalescedRDDPartition,
   * then the preferred machine will be one which most parent splits prefer too.
   * @param partition
   * @return the machine most preferred by split
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    List(partition.asInstanceOf[CoalescedRDDPartition].preferredLocation)
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
 * it tries to also achieve locality. This is done by allowing a slack (balanceSlack) between two
 * bins. If two bins are within the slack in terms of balance, the algorithm will assign partitions
 * according to locality. (contact alig for questions)
 *
 */

private[spark] class PartitionCoalescer(maxPartitions: Int, prev: RDD[_], balanceSlack: Double) {

  def compare(o1: PartitionGroup, o2: PartitionGroup): Boolean = o1.size < o2.size
  def compare(o1: Option[PartitionGroup], o2: Option[PartitionGroup]): Boolean =
    if (o1 == None) false else if (o2 == None) true else compare(o1.get, o2.get)

  val rnd = new scala.util.Random(7919) // keep this class deterministic

  // each element of groupArr represents one coalesced partition
  val groupArr = ArrayBuffer[PartitionGroup]()

  // hash used to check whether some machine is already in groupArr
  val groupHash = mutable.Map[String, ArrayBuffer[PartitionGroup]]()

  // hash used for the first maxPartitions (to avoid duplicates)
  val initialHash = mutable.Set[Partition]()

  // determines the tradeoff between load-balancing the partitions sizes and their locality
  // e.g. balanceSlack=0.10 means that it allows up to 10% imbalance in favor of locality
  val slack = (balanceSlack * prev.partitions.size).toInt

  var noLocality = true  // if true if no preferredLocations exists for parent RDD

  // gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
  def currPrefLocs(part: Partition): Seq[String] = {
    prev.context.getPreferredLocs(prev, part.index).map(tl => tl.host)
  }

  // this class just keeps iterating and rotating infinitely over the partitions of the RDD
  // next() returns the next preferred machine that a partition is replicated on
  // the rotator first goes through the first replica copy of each partition, then second, third
  // the iterators return type is a tuple: (replicaString, partition)
  class LocationIterator(prev: RDD[_]) extends Iterator[(String, Partition)] {

    var it: Iterator[(String, Partition)] = resetIterator()

    override val isEmpty = !it.hasNext

    // initializes/resets to start iterating from the beginning
    def resetIterator() = {
      val iterators = (0 to 2).map( x =>
        prev.partitions.iterator.flatMap(p => {
          if (currPrefLocs(p).size > x) Some((currPrefLocs(p)(x), p)) else None
        } )
      )
      iterators.reduceLeft((x, y) => x ++ y)
    }

    // hasNext() is false iff there are no preferredLocations for any of the partitions of the RDD
    def hasNext(): Boolean = { !isEmpty }

    // return the next preferredLocation of some partition of the RDD
    def next(): (String, Partition) = {
      if (it.hasNext)
        it.next()
      else {
        it = resetIterator() // ran out of preferred locations, reset and rotate to the beginning
        it.next()
      }
    }
  }

  /**
   * Sorts and gets the least element of the list associated with key in groupHash
   * The returned PartitionGroup is the least loaded of all groups that represent the machine "key"
   * @param key string representing a partitioned group on preferred machine key
   * @return Option of PartitionGroup that has least elements for key
   */
  def getLeastGroupHash(key: String): Option[PartitionGroup] = {
    groupHash.get(key).map(_.sortWith(compare).head)
  }

  def addPartToPGroup(part: Partition, pgroup: PartitionGroup): Boolean = {
    if (!initialHash.contains(part)) {
      pgroup.arr += part           // already assign this element
      initialHash += part // needed to avoid assigning partitions to multiple buckets
      true
    } else { false }
  }

  /**
   * Initializes targetLen partition groups and assigns a preferredLocation
   * This uses coupon collector to estimate how many preferredLocations it must rotate through
   * until it has seen most of the preferred locations (2 * n log(n))
   * @param targetLen
   */
  def setupGroups(targetLen: Int) {
    val rotIt = new LocationIterator(prev)

    // deal with empty case, just create targetLen partition groups with no preferred location
    if (!rotIt.hasNext()) {
      (1 to targetLen).foreach(x => groupArr += PartitionGroup())
      return
    }

    noLocality = false

    // number of iterations needed to be certain that we've seen most preferred locations
    val expectedCoupons2 = 2 * (math.log(targetLen)*targetLen + targetLen + 0.5).toInt
    var numCreated = 0
    var tries = 0

    // rotate through until either targetLen unique/distinct preferred locations have been created
    // OR we've rotated expectedCoupons2, in which case we have likely seen all preferred locations,
    // i.e. likely targetLen >> number of preferred locations (more buckets than there are machines)
    while (numCreated < targetLen && tries < expectedCoupons2) {
      tries += 1
      val (nxt_replica, nxt_part) = rotIt.next()
      if (!groupHash.contains(nxt_replica)) {
        val pgroup = PartitionGroup(nxt_replica)
        groupArr += pgroup
        addPartToPGroup(nxt_part, pgroup)
        groupHash += (nxt_replica -> (ArrayBuffer(pgroup))) // list in case we have multiple
        numCreated += 1
      }
    }

    while (numCreated < targetLen) {  // if we don't have enough partition groups, create duplicates
      var (nxt_replica, nxt_part) = rotIt.next()
      val pgroup = PartitionGroup(nxt_replica)
      groupArr += pgroup
      groupHash.get(nxt_replica).get += pgroup
      var tries = 0
      while (!addPartToPGroup(nxt_part, pgroup) && tries < targetLen) { // ensure at least one part
        nxt_part = rotIt.next()._2
        tries += 1
      }
      numCreated += 1
    }

  }

  /**
   * Takes a parent RDD partition and decides which of the partition groups to put it in
   * Takes locality into account, but also uses power of 2 choices to load balance
   * It strikes a balance between the two use the balanceSlack variable
   * @param p partition (ball to be thrown)
   * @return partition group (bin to be put in)
   */
  def pickBin(p: Partition): PartitionGroup = {
    val pref = currPrefLocs(p).map(getLeastGroupHash(_)).sortWith(compare) // least loaded pref locs
    val prefPart = if (pref == Nil) None else pref.head

    val r1 = rnd.nextInt(groupArr.size)
    val r2 = rnd.nextInt(groupArr.size)
    val minPowerOfTwo = if (groupArr(r1).size < groupArr(r2).size) groupArr(r1) else groupArr(r2)
    if (prefPart== None) // if no preferred locations, just use basic power of two
      return minPowerOfTwo

    val prefPartActual = prefPart.get

    if (minPowerOfTwo.size + slack <= prefPartActual.size)  // more imbalance than the slack allows
      return minPowerOfTwo  // prefer balance over locality
    else {
      return prefPartActual // prefer locality over balance
    }
  }

  def throwBalls() {
    if (noLocality) {  // no preferredLocations in parent RDD, no randomization needed
      if (maxPartitions > groupArr.size) { // just return prev.partitions
        for ((p,i) <- prev.partitions.zipWithIndex) {
          groupArr(i).arr += p
        }
      } else { // no locality available, then simply split partitions based on positions in array
        for(i <- 0 until maxPartitions) {
          val rangeStart = ((i.toLong * prev.partitions.length) / maxPartitions).toInt
          val rangeEnd = (((i.toLong + 1) * prev.partitions.length) / maxPartitions).toInt
          (rangeStart until rangeEnd).foreach{ j => groupArr(i).arr += prev.partitions(j) }
        }
      }
    } else {
      for (p <- prev.partitions if (!initialHash.contains(p))) { // throw every partition into group
        pickBin(p).arr += p
      }
    }
  }

  def getPartitions: Array[PartitionGroup] = groupArr.filter( pg => pg.size > 0).toArray

  /**
   * Runs the packing algorithm and returns an array of PartitionGroups that if possible are
   * load balanced and grouped by locality
   * @return array of partition groups
   */
  def run(): Array[PartitionGroup] = {
    setupGroups(math.min(prev.partitions.length, maxPartitions))   // setup the groups (bins)
    throwBalls()             // assign partitions (balls) to each group (bins)
    getPartitions
  }
}

private[spark] case class PartitionGroup(prefLoc: String = "") {
  var arr = mutable.ArrayBuffer[Partition]()

  def size = arr.size
}
