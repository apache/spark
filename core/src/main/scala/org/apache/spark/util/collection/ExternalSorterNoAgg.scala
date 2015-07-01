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

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import com.google.common.annotations.VisibleForTesting
import org.apache.spark._
import org.apache.spark.serializer._
import org.apache.spark.shuffle.sort.{SortShuffleWriter, SortShuffleFileWriter}
import org.apache.spark.storage.BlockId

/**
 * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
 * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
 * optionally sorts keys within each partition using a custom Comparator. Can output a single
 * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
 *
 * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
 *
 * Note: Although ExternalSorter is a fairly generic sorter, some of its configuration is tied
 * to its use in sort-based shuffle (for example, its block compression is controlled by
 * `spark.shuffle.compress`).  We may need to revisit this if ExternalSorter is used in other
 * non-shuffle contexts where we might want to use different configuration settings.
 *
 * @param partitioner optional Partitioner; if given, sort by partition ID and then key
 * @param ordering optional Ordering to sort keys within each partition; should be a total ordering
 * @param serializer serializer to use when spilling to disk
 *
 * Note that if an Ordering is given, we'll always sort using it, so only provide it if you really
 * want the output keys to be sorted. In a map task without map-side combine for example, you
 * probably want to pass None as the ordering to avoid extra sorting. On the other hand, if you do
 * want to do combining, having an Ordering is more efficient than not having it.
 *
 * Users interact with this class in the following way:
 *
 * 1. Instantiate an ExternalSorter.
 *
 * 2. Call insertAll() with a set of records.
 *
 * 3. Request an iterator() back to traverse sorted/aggregated records.
 *     - or -
 *    Invoke writePartitionedFile() to create a file containing sorted/aggregated outputs
 *    that can be used in Spark's sort shuffle.
 *
 * At a high level, this class works internally as follows:
 *
 * - We repeatedly fill up buffers of in-memory data, using either a PartitionedAppendOnlyMap if
 *   we want to combine by key, or a PartitionedSerializedPairBuffer or PartitionedPairBuffer if we
 *   don't. Inside these buffers, we sort elements by partition ID and then possibly also by key.
 *   To avoid calling the partitioner multiple times with each key, we store the partition ID
 *   alongside each record.
 *
 * - When each buffer reaches our memory limit, we spill it to a file. This file is sorted first
 *   by partition ID and possibly second by key or by hash code of the key, if we want to do
 *   aggregation. For each file, we track how many objects were in each partition in memory, so we
 *   don't have to write out the partition ID for every element.
 *
 * - When the user requests an iterator or file output, the spilled files are merged, along with
 *   any remaining in-memory data, using the same sort order defined above (unless both sorting
 *   and aggregation are disabled). If we need to aggregate by key, we either use a total ordering
 *   from the ordering parameter, or read the keys with the same hash code and compare them with
 *   each other for equality to merge values.
 *
 * - Users are expected to call stop() at the end to delete all the intermediate files.
 */
private[spark] class ExternalSorterNoAgg[K, V, C](
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Option[Serializer] = None)
  extends ExternalSorter[K, V, C](partitioner, ordering, serializer)
  with Spillable[WritablePartitionedPairCollection[K, C]]
  with SortShuffleFileWriter[K, V] {

  // Since SPARK-7855, bypassMergeSort optimization is no longer performed as part of this class.
  // As a sanity check, make sure that we're not handling a shuffle which should use that path.
  if (SortShuffleWriter.shouldBypassMergeSort(conf, numPartitions, None, ordering)) {
    throw new IllegalArgumentException("ExternalSorter should not be used to handle "
      + " a sort that the BypassMergeSortShuffleWriter should handle")
  }

  override def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  override def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // Stick values into our buffer
    while (records.hasNext) {
      addElementsRead()
      val kv = records.next()
      buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
      maybeSpillCollection(usingMap = false)
    }
  }

  /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   *
   * Returns an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   */
  override def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)]):
      Iterator[(Int, Iterator[Product2[K, C]])] = {
    val readers = spills.map(new SpillReader(_))
    val inMemBuffered = inMemory.buffered
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      if (ordering.isDefined) {
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * Return an iterator over all the data written to this object, grouped by partition.
   * For each partition we then have an iterator over its contents, and these are expected to be
   * accessed in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   */
  @VisibleForTesting
  override def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    partitionedIterator(buffer)
  }

  /**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter.
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @param context a TaskContext for a running Spark task, for us to update shuffle metrics.
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
  override def writePartitionedFile(
      blockId: BlockId,
      context: TaskContext,
      outputFile: File): Array[Long] = {
    writePartitionedFile(blockId, context, outputFile, buffer)
  }
}
