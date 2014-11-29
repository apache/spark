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

import scala.collection.BufferedIterator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.collection.ExternalAppendOnlyMap.HashComparator
import org.apache.spark.executor.ShuffleWriteMetrics

/**
 * :: DeveloperApi ::
 * An append-only map that spills sorted content to disk when there is insufficient space for it
 * to grow.
 *
 * This map takes two passes over the data:
 *
 *   (1) Values are merged into combiners, which are sorted and spilled to disk as necessary
 *   (2) Combiners are read from disk and merged together
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 *
 * Two parameters control the memory threshold:
 *
 *   `spark.shuffle.memoryFraction` specifies the collective amount of memory used for storing
 *   these maps as a fraction of the executor's total memory. Since each concurrently running
 *   task maintains one map, the actual threshold for each map is this quantity divided by the
 *   number of running tasks.
 *
 *   `spark.shuffle.safetyFraction` specifies an additional margin of safety as a fraction of
 *   this threshold, in case map size estimation is not sufficiently accurate.
 */
@DeveloperApi
class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager)
  extends Iterable[(K, C)]
  with Serializable
  with Logging
  with Spillable[SizeTracker] {

  private var currentMap = new SizeTrackingAppendOnlyMap[K, C]
  private val spilledMaps = new ArrayBuffer[DiskMapIterator]
  private val sparkConf = SparkEnv.get.conf
  private val diskBlockManager = blockManager.diskBlockManager

  /**
   * Size of object batches when reading/writing from serializers.
   *
   * Objects are written in batches, with each batch using its own serialization stream. This
   * cuts down on the size of reference-tracking maps constructed when deserializing a stream.
   *
   * NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
   * grow internal data structures by growing + copying every time the number of objects doubles.
   */
  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Number of bytes spilled in total
  private var _diskBytesSpilled = 0L

  private val fileBufferSize = sparkConf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  // Write metrics for current spill
  private var curWriteMetrics: ShuffleWriteMetrics = _

  private val keyComparator = new HashComparator[K]
  private val ser = serializer.newInstance()

  /**
   * Insert the given key and value into the map.
   */
  def insert(key: K, value: V): Unit = {
    insertAll(Iterator((key, value)))
  }

  /**
   * Insert the given iterator of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    // An update function for the map that we reuse across entries to avoid allocating
    // a new closure each time
    var curEntry: Product2[K, V] = null
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
    }

    while (entries.hasNext) {
      curEntry = entries.next()
      if (maybeSpill(currentMap, currentMap.estimateSize())) {
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      currentMap.changeValue(curEntry._1, update)
      addElementsRead()
    }
  }

  /**
   * Insert the given iterable of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insertAll(entries: Iterable[Product2[K, V]]): Unit = {
    insertAll(entries.iterator)
  }

  /**
   * Sort the existing contents of the in-memory map and spill them to a temporary file on disk.
   */
  override protected[this] def spill(collection: SizeTracker): Unit = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    curWriteMetrics = new ShuffleWriteMetrics()
    var writer = blockManager.getDiskWriter(blockId, file, serializer, fileBufferSize,
      curWriteMetrics)
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    def flush() = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten
      batchSizes.append(curWriteMetrics.shuffleBytesWritten)
      objectsWritten = 0
    }

    var success = false
    try {
      val it = currentMap.destructiveSortedIterator(keyComparator)
      while (it.hasNext) {
        val kv = it.next()
        writer.write(kv)
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
          curWriteMetrics = new ShuffleWriteMetrics()
          writer = blockManager.getDiskWriter(blockId, file, serializer, fileBufferSize,
            curWriteMetrics)
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else if (writer != null) {
        val w = writer
        writer = null
        w.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        if (writer != null) {
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {
          file.delete()
        }
      }
    }

    spilledMaps.append(new DiskMapIterator(file, blockId, batchSizes))
  }

  def diskBytesSpilled: Long = _diskBytesSpilled

  /**
   * Return an iterator that merges the in-memory map with the spilled maps.
   * If no spill has occurred, simply return the in-memory map's iterator.
   */
  override def iterator: Iterator[(K, C)] = {
    if (spilledMaps.isEmpty) {
      currentMap.iterator
    } else {
      new ExternalIterator()
    }
  }

  /**
   * An iterator that sort-merges (K, C) pairs from the in-memory map and the spilled maps
   */
  private class ExternalIterator extends Iterator[(K, C)] {

    // A queue that maintains a buffer for each stream we are currently merging
    // This queue maintains the invariant that it only contains non-empty buffers
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    private val sortedMap = currentMap.destructiveSortedIterator(keyComparator)
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)

    inputStreams.foreach { it =>
      val kcPairs = new ArrayBuffer[(K, C)]
      readNextHashCode(it, kcPairs)
      if (kcPairs.length > 0) {
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }

    /**
     * Fill a buffer with the next set of keys with the same hash code from a given iterator. We
     * read streams one hash code at a time to ensure we don't miss elements when they are merged.
     *
     * Assumes the given iterator is in sorted order of hash code.
     *
     * @param it iterator to read from
     * @param buf buffer to write the results into
     */
    private def readNextHashCode(it: BufferedIterator[(K, C)], buf: ArrayBuffer[(K, C)]): Unit = {
      if (it.hasNext) {
        var kc = it.next()
        buf += kc
        val minHash = hashKey(kc)
        while (it.hasNext && it.head._1.hashCode() == minHash) {
          kc = it.next()
          buf += kc
        }
      }
    }

    /**
     * If the given buffer contains a value for the given key, merge that value into
     * baseCombiner and remove the corresponding (K, C) pair from the buffer.
     */
    private def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C = {
      var i = 0
      while (i < buffer.pairs.length) {
        val pair = buffer.pairs(i)
        if (pair._1 == key) {
          // Note that there's at most one pair in the buffer with a given key, since we always
          // merge stuff in a map before spilling, so it's safe to return after the first we find
          removeFromBuffer(buffer.pairs, i)
          return mergeCombiners(baseCombiner, pair._2)
        }
        i += 1
      }
      baseCombiner
    }

    /**
     * Remove the index'th element from an ArrayBuffer in constant time, swapping another element
     * into its place. This is more efficient than the ArrayBuffer.remove method because it does
     * not have to shift all the elements in the array over. It works for our array buffers because
     * we don't care about the order of elements inside, we just want to search them for a key.
     */
    private def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T = {
      val elem = buffer(index)
      buffer(index) = buffer(buffer.size - 1)  // This also works if index == buffer.size - 1
      buffer.reduceToSize(buffer.size - 1)
      elem
    }

    /**
     * Return true if there exists an input stream that still has unvisited pairs.
     */
    override def hasNext: Boolean = mergeHeap.length > 0

    /**
     * Select a key with the minimum hash, then combine all values with the same key from all
     * input streams.
     */
    override def next(): (K, C) = {
      if (mergeHeap.length == 0) {
        throw new NoSuchElementException
      }
      // Select a key from the StreamBuffer that holds the lowest key hash
      val minBuffer = mergeHeap.dequeue()
      val minPairs = minBuffer.pairs
      val minHash = minBuffer.minKeyHash
      val minPair = removeFromBuffer(minPairs, 0)
      val minKey = minPair._1
      var minCombiner = minPair._2
      assert(hashKey(minPair) == minHash)

      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (mergeHeap.length > 0 && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        mergedBuffers += newBuffer
      }

      // Repopulate each visited stream buffer and add it back to the queue if it is non-empty
      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {
          readNextHashCode(buffer.iterator, buffer.pairs)
        }
        if (!buffer.isEmpty) {
          mergeHeap.enqueue(buffer)
        }
      }

      (minKey, minCombiner)
    }

    /**
     * A buffer for streaming from a map iterator (in-memory or on-disk) sorted by key hash.
     * Each buffer maintains all of the key-value pairs with what is currently the lowest hash
     * code among keys in the stream. There may be multiple keys if there are hash collisions.
     * Note that because when we spill data out, we only spill one value for each key, there is
     * at most one element for each key.
     *
     * StreamBuffers are ordered by the minimum key hash currently available in their stream so
     * that we can put them into a heap and sort that.
     */
    private class StreamBuffer(
        val iterator: BufferedIterator[(K, C)],
        val pairs: ArrayBuffer[(K, C)])
      extends Comparable[StreamBuffer] {

      def isEmpty = pairs.length == 0

      // Invalid if there are no more pairs in this stream
      def minKeyHash: Int = {
        assert(pairs.length > 0)
        hashKey(pairs.head)
      }

      override def compareTo(other: StreamBuffer): Int = {
        // descending order because mutable.PriorityQueue dequeues the max, not the min
        if (other.minKeyHash < minKeyHash) -1 else if (other.minKeyHash == minKeyHash) 0 else 1
      }
    }
  }

  /**
   * An iterator that returns (K, C) pairs in sorted order from an on-disk map
   */
  private class DiskMapIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[(K, C)]
  {
    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
    assert(file.length() == batchOffsets.last,
      "File length is not equal to the last batch offset:\n" +
      s"    file length = ${file.length}\n" +
      s"    last batch offset = ${batchOffsets.last}\n" +
      s"    all batch offsets = ${batchOffsets.mkString(",")}"
    )

    private var batchIndex = 0  // Which batch we're in
    private var fileStream: FileInputStream = null

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    private var deserializeStream = nextBatchStream()
    private var nextItem: (K, C) = null
    private var objectsRead = 0

    /**
     * Construct a stream that reads only from the next batch.
     */
    private def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchIndex < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchIndex)
        fileStream = new FileInputStream(file)
        fileStream.getChannel.position(start)
        batchIndex += 1

        val end = batchOffsets(batchIndex)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(blockId, bufferedStream)
        ser.deserializeStream(compressedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): (K, C) = {
      try {
        val item = deserializeStream.readObject().asInstanceOf[(K, C)]
        objectsRead += 1
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        item
      } catch {
        case e: EOFException =>
          cleanup()
          null
      }
    }

    override def hasNext: Boolean = {
      if (nextItem == null) {
        if (deserializeStream == null) {
          return false
        }
        nextItem = readNextItem()
      }
      nextItem != null
    }

    override def next(): (K, C) = {
      val item = if (nextItem == null) readNextItem() else nextItem
      if (item == null) {
        throw new NoSuchElementException
      }
      nextItem = null
      item
    }

    // TODO: Ensure this gets called even if the iterator isn't drained.
    private def cleanup() {
      batchIndex = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      ds.close()
      file.delete()
    }
  }

  /** Convenience function to hash the given (K, C) pair by the key. */
  private def hashKey(kc: (K, C)): Int = ExternalAppendOnlyMap.hash(kc._1)
}

private[spark] object ExternalAppendOnlyMap {

  /**
   * Return the hash code of the given object. If the object is null, return a special hash code.
   */
  private def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }

  /**
   * A comparator which sorts arbitrary keys based on their hash codes.
   */
  private class HashComparator[K] extends Comparator[K] {
    def compare(key1: K, key2: K): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }
}
