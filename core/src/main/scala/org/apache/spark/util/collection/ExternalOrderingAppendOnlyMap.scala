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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.storage.{BlockId, BlockManager}
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
class ExternalOrderingAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    createExternalCombiner: () => C, // To avoid keeping 2 `CompactBuffer`s in the memory
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager)(implicit ord: Ordering[K])
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
      val it = currentMap.destructiveSortedIterator(ord)
      while (it.hasNext) {
        val kv = it.next()
        writer.write(kv._1)
        writer.write(kv._2)
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
    private val sortedMap = currentMap.destructiveSortedIterator(ord).
      map(p => (p._1, new MemoryLazyValue(p._2)))
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps)

    inputStreams.foreach { it =>
      val buffer = new StreamBuffer(it)
      if (buffer.moveToNext()) {
        mergeHeap.enqueue(buffer)
      }
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
      var minCombiner = createExternalCombiner()

      // Select a key from the StreamBuffer that holds the lowest key hash
      val minBuffer = mergeHeap.dequeue()
      val minKey = minBuffer.currentKey
      minCombiner = mergeCombiners(minCombiner, minBuffer.currentValue)

      // Release the memory used by `minBuffer.currentValue`
      if (minBuffer.moveToNext()) {
        mergeHeap.enqueue(minBuffer)
      }

      while (mergeHeap.length > 0 && mergeHeap.head.currentKey == minKey) {
        val newBuffer = mergeHeap.dequeue()
        minCombiner = mergeCombiners(minCombiner, newBuffer.currentValue)
        if (newBuffer.moveToNext()) {
          mergeHeap.enqueue(newBuffer)
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
        val iterator: Iterator[(K, LazyValue[C])])
      extends Comparable[StreamBuffer] {

      private var pair: (K, LazyValue[C]) = null

      def currentKey = pair._1

      def currentValue = pair._2.value

      override def compareTo(other: StreamBuffer): Int = {
        // descending order because mutable.PriorityQueue dequeues the max, not the min
        ord.compare(other.pair._1, pair._1)
      }

      def moveToNext(): Boolean = {
        if (iterator.hasNext) {
          pair = iterator.next()
          true
        }
        else {
          // Note: It's important to release the reference to free the memory
          pair = null
          false
        }
      }
    }
  }

  /**
   * An iterator that returns (K, LazyValue[C]) pairs in sorted order from an on-disk map.
   */
  private class DiskMapIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[(K, LazyValue[C])]
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
    private var nextItem: (K, StreamLazyValue[C]) = null
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
    private def readNextItem(): (K, StreamLazyValue[C]) = {
      try {
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        if (deserializeStream == null) {
          return null
        }
        val item = (deserializeStream.readObject().asInstanceOf[K],
          new StreamLazyValue[C](deserializeStream))
        objectsRead += 1
        item
      } catch {
        case e: EOFException =>
          cleanup()
          null
      }
    }

    override def hasNext: Boolean = {
      if (nextItem == null) {
        nextItem = readNextItem()
      }
      nextItem != null
    }

    override def next(): (K, LazyValue[C]) = {
      if (hasNext) {
        val item = nextItem
        nextItem = null
        item
      }
      else {
        throw new NoSuchElementException
      }
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

}

private[spark] trait LazyValue[T]{
  def value: T
}

/**
 * A wrapper to a value memory. It's used to make the interface consistent.
 *
 * @param value
 * @tparam T
 */
private[spark] class MemoryLazyValue[T](override val value: T) extends LazyValue[T]

/**
 * A wrapper to read the value lazily. This is used to prefetch a key without reading the value into
 * memory.
 *
 * Note: `stream` must not be read in other place until `value` is materialized.
 *
 * @param stream
 * @tparam T
 */
private[spark] class StreamLazyValue[T](stream: DeserializationStream) extends LazyValue[T] {
  override lazy val value: T = stream.readObject().asInstanceOf[T]
}
