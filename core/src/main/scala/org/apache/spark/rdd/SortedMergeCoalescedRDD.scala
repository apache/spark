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
import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.util.Utils

/**
 * An RDD that coalesces partitions while preserving ordering through k-way merge.
 *
 * Unlike CoalescedRDD which simply concatenates partitions, this RDD performs a sorted
 * merge of multiple input partitions to maintain ordering. This is useful when input
 * partitions are locally sorted and we want to preserve that ordering after coalescing.
 *
 * The merge is performed using a priority queue (min-heap) which provides O(n log k)
 * time complexity, where n is the total number of elements and k is the number of
 * partitions being merged.
 *
 * @param prev The parent RDD
 * @param numPartitions The number of output partitions after coalescing
 * @param ordering The ordering to maintain during merge
 * @param partitionCoalescer The coalescer defining how to group input partitions
 * @tparam T The element type
 */
private[spark] class SortedMergeCoalescedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    numPartitions: Int,
    partitionCoalescer: PartitionCoalescer,
    ordering: Ordering[T])
  extends RDD[T](prev.context, Nil) {

  override def getPartitions: Array[Partition] = {
    partitionCoalescer.coalesce(numPartitions, prev).zipWithIndex.map {
      case (pg, i) =>
        val parentIndices = pg.partitions.map(_.index).toSeq
        new SortedMergePartition(i, prev, parentIndices, pg.prefLoc)
    }
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    val mergePartition = partition.asInstanceOf[SortedMergePartition]
    val parentPartitions = mergePartition.parents

    if (parentPartitions.isEmpty) {
      Iterator.empty
    } else if (parentPartitions.length == 1) {
      // No merge needed for single partition
      firstParent[T].iterator(parentPartitions.head, context)
    } else {
      // Perform k-way merge
      new SortedMergeIterator[T](
        parentPartitions.map(p => firstParent[T].iterator(p, context)),
        ordering
      )
    }
  }

  override def getDependencies: Seq[org.apache.spark.Dependency[_]] = {
    Seq(new org.apache.spark.NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[SortedMergePartition].parentsIndices
    })
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[SortedMergePartition].prefLoc.toSeq
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}

/**
 * Partition for SortedMergeCoalescedRDD that tracks which parent partitions to merge.
 * @param index of this coalesced partition
 * @param rdd which it belongs to
 * @param parentsIndices list of indices in the parent that have been coalesced into this partition
 * @param prefLoc the preferred location for this partition
 */
private[spark] class SortedMergePartition(
    idx: Int,
    @transient private val rdd: RDD[_],
    val parentsIndices: Seq[Int],
    val prefLoc: Option[String] = None) extends Partition {
  override val index: Int = idx
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent partition at the time of task serialization
    parents = parentsIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }
}

/**
 * Iterator that performs k-way merge of sorted iterators.
 *
 * Uses a priority queue (min-heap) to efficiently find the next smallest element
 * across all input iterators according to the specified ordering. This provides
 * O(n log k) time complexity where n is the total number of elements and k is
 * the number of iterators being merged.
 *
 * @param iterators The sequence of sorted iterators to merge
 * @param ordering The ordering to use for comparison
 * @tparam T The element type
 */
private[spark] class SortedMergeIterator[T](
    iterators: Seq[Iterator[T]],
    ordering: Ordering[T]) extends Iterator[T] {

  // Priority queue entry: element paired with the iterator it came from
  private case class QueueEntry(element: T, iter: Iterator[T])

  // Min-heap ordered by element according to the provided ordering
  private implicit val queueOrdering: Ordering[QueueEntry] = new Ordering[QueueEntry] {
    override def compare(x: QueueEntry, y: QueueEntry): Int = {
      // Reverse for min-heap (PriorityQueue is max-heap by default)
      ordering.compare(y.element, x.element)
    }
  }

  private val queue = mutable.PriorityQueue.empty[QueueEntry]

  // The iterator whose underlying reader must be advanced before the next queue operation.
  // null means no advance is pending.
  private var pendingIter: Iterator[T] = null

  // Initialize queue with first element from each non-empty iterator
  iterators.foreach { iter =>
    if (iter.hasNext) {
      queue.enqueue(QueueEntry(iter.next(), iter))
    }
  }

  override def hasNext: Boolean = queue.nonEmpty || (pendingIter != null && pendingIter.hasNext)

  override def next(): T = {
    // Advance the pending iterator now that the caller is done with the previously returned
    // element. Deferred from the last next() call so the returned row's buffer (which may be reused
    // in-place by the underlying reader) remained valid until this point.
    if (pendingIter != null) {
      if (pendingIter.hasNext) {
        queue.enqueue(QueueEntry(pendingIter.next(), pendingIter))
      }
      pendingIter = null
    }
    if (queue.isEmpty) {
      throw new NoSuchElementException("next on empty iterator")
    }

    val entry = queue.dequeue()
    // Defer advancing this iterator until the next next() call so the returned element's buffer
    // remains valid across any subsequent hasNext() calls.
    pendingIter = entry.iter
    entry.element
  }
}
