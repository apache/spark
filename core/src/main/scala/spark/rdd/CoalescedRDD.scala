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

package spark.rdd

import spark.{Dependency, OneToOneDependency, NarrowDependency, RDD, Partition, TaskContext}
import java.io.{ObjectOutputStream, IOException}
import scala.collection.mutable

private[spark] case class CoalescedRDDPartition(
    index: Int,
    @transient rdd: RDD[_],
    parentsIndices: Array[Int],
    prefLoc: String = ""
  ) extends Partition {
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    parents = parentsIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }

  /**
   * Gets the preferred location for this coalesced RDD partition. Most parent indices should prefer this machine.
   * @return preferred location
   */
  def getPreferredLocation = prefLoc

  /**
   * Computes how many of the parents partitions have getPreferredLocation as one of their preferredLocations
   * @return locality of this coalesced partition between 0 and 1
   */
  def localFraction :Double = {
    var loc: Int = 0
    parents.foreach(p => if (rdd.preferredLocations(p).contains(getPreferredLocation)) loc += 1)
    if (parents.size == 0) 0.0 else (loc.toDouble / parents.size.toDouble)
  }
}

/**
 * Coalesce the partitions of a parent RDD (`prev`) into fewer partitions, so that each partition of
 * this RDD computes one or more of the parent ones. Will produce exactly `maxPartitions` if the
 * parent had more than this many partitions, or fewer if the parent had fewer.
 *
 * This transformation is useful when an RDD with many partitions gets filtered into a smaller one,
 * or to avoid having a large number of small tasks when processing a directory with many files.
 */
class CoalescedRDD[T: ClassManifest](
    @transient var prev: RDD[T],
    maxPartitions: Int,
    balanceSlack: Double = 0.10 )
  extends RDD[T](prev.context, Nil) {  // Nil since we implement getDependencies

  override def getPartitions: Array[Partition] = {
    val res = mutable.ArrayBuffer[CoalescedRDDPartition]()
    val packer = new PartitionCoalescer(maxPartitions, prev, balanceSlack)

    for ((pg, i) <- packer.getPartitions.zipWithIndex) {
      val ids = pg.list.map(_.index).toArray
      res += new CoalescedRDDPartition(i, prev, ids, pg.prefLoc)
    }

    res.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[CoalescedRDDPartition].parents.iterator.flatMap { parentSplit =>
      firstParent[T].iterator(parentSplit, context)
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
   * Returns the preferred machine for the split. If split is of type CoalescedRDDPartition, then the preferred machine
   * will be one which most parent splits prefer too.
   * @param split
   * @return the machine most preferred by split
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (split.isInstanceOf[CoalescedRDDPartition])
      List(split.asInstanceOf[CoalescedRDDPartition].getPreferredLocation)
    else
      super.getPreferredLocations(split)
  }

}


class PartitionCoalescer(maxPartitions: Int, prev: RDD[_], balanceSlack: Double) {

  private def compare(o1: PartitionGroup, o2: PartitionGroup): Boolean = o1.size < o2.size
  private def compare(o1: Option[PartitionGroup], o2: Option[PartitionGroup]): Boolean =
    if (o1 == None) false else if (o2 == None) true else compare(o1.get, o2.get)

  private val rnd = new scala.util.Random(7919) // keep this class deterministic

  // each element of groupArr represents one coalesced partition
  private val groupArr = mutable.ArrayBuffer[PartitionGroup]()

  // hash used to check whether some machine is already in groupArr
  private val groupHash = mutable.Map[String, mutable.ListBuffer[PartitionGroup]]()

  // hash used for the first maxPartitions (to avoid duplicates)
  private val initialHash = mutable.Map[Partition, Boolean]()

  // determines the tradeoff between load-balancing the partitions sizes and their locality
  // e.g. balanceSlack=0.10 means that it allows up to 10% imbalance in favor of locality
  private val slack = (balanceSlack * prev.partitions.size).toInt

  private var noLocality = true  // if true if no preferredLocations exists for parent RDD

  this.setupGroups(math.min(prev.partitions.length, maxPartitions))   // setup the groups (bins) and preferred locations
  this.throwBalls()             // assign partitions (balls) to each group (bins)

  def getPartitions : Array[PartitionGroup] = groupArr.filter( pg => pg.size > 0).toArray

  // this class just keeps iterating and rotating infinitely over the partitions of the RDD
  // next() returns the next preferred machine that a partition is replicated on
  // the rotator first goes through the first replica copy of each partition, then second, then third
  // the iterators return type is a tuple: (replicaString, partition)
  private class RotateLocations(prev: RDD[_]) extends Iterator[(String, Partition)] {

    private var it: Iterator[(String, Partition)] = resetIterator()
    override val isEmpty = !it.hasNext

    // initializes/resets to start iterating from the beginning
    private def resetIterator() = {
      val i1 =  prev.partitions.view.map( (p: Partition) =>
      { if (prev.preferredLocations(p).length > 0) Some((prev.preferredLocations(p)(0),p)) else None } )
      val i2 =  prev.partitions.view.map( (p: Partition) =>
      { if (prev.preferredLocations(p).length > 1) Some((prev.preferredLocations(p)(1),p)) else None } )
      val i3 =  prev.partitions.view.map( (p: Partition) =>
      { if (prev.preferredLocations(p).length > 2) Some((prev.preferredLocations(p)(2),p)) else None } )
      val res = List(i1,i2,i3)
      res.view.flatMap(x => x).flatten.iterator // fuses the 3 iterators (1st replica, 2nd, 3rd) into one iterator
    }

    // hasNext() is false iff there are no preferredLocations for any of the partitions of the RDD
    def hasNext(): Boolean = !isEmpty

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

  case class PartitionGroup(prefLoc: String = "") {
    var list = mutable.ListBuffer[Partition]()

    def size = list.size
  }

  /**
   * Sorts and gets the least element of the list associated with key in groupHash
   * The returned PartitionGroup is the least loaded of all groups that represent the machine "key"
   * @param key string representing a partitioned group on preferred machine key
   * @return Option of PartitionGroup that has least elements for key
   */
  private def getLeastGroupHash(key: String): Option[PartitionGroup] = {
    groupHash.get(key).map(_.sortWith(compare).head)
  }

  def addPartToPGroup(part : Partition, pgroup : PartitionGroup) : Boolean = {
    if (!initialHash.contains(part)) {
      pgroup.list += part           // already preassign this element, ensures every bucket will have 1 element
      initialHash += (part -> true) // needed to avoid assigning partitions to multiple buckets/groups
      true
    } else false
  }

  /**
   * Initializes targetLen partition groups and assigns a preferredLocation
   * This uses coupon collector to estimate how many preferredLocations it must rotate through until it has seen
   * most of the preferred locations (2 * n log(n))
   * @param targetLen
   */
  private def setupGroups(targetLen: Int) {
    val rotIt = new RotateLocations(prev)

    // deal with empty rotator case, just create targetLen partition groups with no preferred location
    if (!rotIt.hasNext()) {
      (1 to targetLen).foreach(x => groupArr += PartitionGroup())
      return
    }

    noLocality = false

    // number of iterations needed to be certain that we've seen most preferred locations
    val expectedCoupons2 = 2 * (math.log(targetLen)*targetLen + targetLen + 0.5).toInt
    var numCreated = 0
    var tries = 0

    // rotate through until either targetLen unique/distinct preferred locations have been created OR
    // we've rotated expectedCoupons2, in which case we have likely seen all preferred locations, i.e.
    // likely targetLen >> number of preferred locations (more buckets than there are machines)
    while (numCreated < targetLen && tries < expectedCoupons2) {
      tries += 1
      val (nxt_replica, nxt_part) = rotIt.next()
      if (!groupHash.contains(nxt_replica)) {
        val pgroup = PartitionGroup(nxt_replica)
        groupArr += pgroup
        addPartToPGroup(nxt_part, pgroup)
        groupHash += (nxt_replica -> (mutable.ListBuffer(pgroup))) // list in case we have multiple groups per machine
        numCreated += 1
      }
    }

    while (numCreated < targetLen) {  // if we don't have enough partition groups, just create duplicates
    var (nxt_replica, nxt_part) = rotIt.next()
      val pgroup = PartitionGroup(nxt_replica)
      groupArr += pgroup
      groupHash.get(nxt_replica).get += pgroup
      var tries = 0
      while (!addPartToPGroup(nxt_part, pgroup) && tries < targetLen) { // ensure each group has at least one partition
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
  private def pickBin(p: Partition): PartitionGroup = {
    val pref = prev.preferredLocations(p).map(getLeastGroupHash(_)).sortWith(compare) // least loaded bin of replicas
    val prefPart = if (pref == Nil) None else pref.head

    val r1 = rnd.nextInt(groupArr.size)
    val r2 = rnd.nextInt(groupArr.size)
    val minPowerOfTwo = if (groupArr(r1).size < groupArr(r2).size) groupArr(r1) else groupArr(r2) // power of 2
    if (prefPart== None) // if no preferred locations, just use basic power of two
      return minPowerOfTwo

    val prefPartActual = prefPart.get

    if (minPowerOfTwo.size + slack <= prefPartActual.size)  // more imbalance than the slack allows
      return minPowerOfTwo  // prefer balance over locality
    else {
      return prefPartActual // prefer locality over balance
    }
  }

  private def throwBalls() {
    if (noLocality) {  // no preferredLocations in parent RDD, no randomization needed
      if (maxPartitions > groupArr.size) { // just return prev.partitions
        for ((p,i) <- prev.partitions.zipWithIndex) {
          groupArr(i).list += p
        }
      } else { // old code, just splits them grouping partitions that are next to each other in the array
        (0 until maxPartitions).foreach { i =>
          val rangeStart = ((i.toLong * prev.partitions.length) / maxPartitions).toInt
          val rangeEnd = (((i.toLong + 1) * prev.partitions.length) / maxPartitions).toInt
          (rangeStart until rangeEnd).foreach{ j => groupArr(i).list += prev.partitions(j) }
        }
      }
    } else {
      for (p <- prev.partitions if (!initialHash.contains(p))) { // throw every partition into a partition group
        pickBin(p).list += p
      }
    }
  }

}
