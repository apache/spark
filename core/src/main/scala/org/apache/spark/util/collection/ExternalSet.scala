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
import org.apache.spark.util.collection.ExternalSet.HashComparator
import org.apache.spark.executor.ShuffleWriteMetrics

@DeveloperApi
class ExternalSet[K](
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager)
  extends Iterable[K]
  with Serializable
  with Logging
  with Spillable[SizeTracker] {

  private var currentSet = new SizeTrackingSet[Any]
  private val spilledMaps = new ArrayBuffer[DiskSetIterator]
  private val sparkConf = SparkEnv.get.conf
  private val diskBlockManager = blockManager.diskBlockManager
  
  // Number of keys added since last spill
  protected[this] var elementsRead = 0L
  
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
  
  private val keyComparator = new HashComparator[Any]
  private val ser = serializer.newInstance()
  
  /**
   * Add the given key into the set.
   */
  def add(key: K): Unit = {
    addAll(Iterator(key))
  }

  /**
   * Add the given iterator of keys into the set.
   * When the underlying set needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the set;
   * otherwise, spill the in-memory set to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def addAll(entries: Iterator[K]): Unit = {
    while (entries.hasNext) {
      if (maybeSpill(currentSet, currentSet.estimateSize())) {
        currentSet = new SizeTrackingSet[Any]
      }
      currentSet.add(entries.next())
      elementsRead += 1
    }
  }

  /**
    * Add the given iterable of keys into the set.
    * When the underlying set needs to grow, check if the global pool of shuffle memory has 
    * enough room for this to happen. If so, allocate the memory required to grow the set;
    * otherwise, spill the in-memory set to disk.
    *
    * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
    */
  def addAll(entries: Iterable[K]): Unit = {
    addAll(entries.iterator)
  }

  /**
   * Sort the existing contents of the in-memory set 
   * by hashCode and spill them to a temporary file on disk.
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
      val it = currentSet.destructiveSortedIterator(keyComparator)
      while (it.hasNext) {
        val k = it.next()
        writer.write(k)
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
    
    spilledMaps.append(new DiskSetIterator(file, blockId, batchSizes))

    elementsRead = 0
  }
  
  /**
   * Return an iterator that merges the in-memory set with the spilled sets.
   * If no spill has occurred, simply return the in-memory set's iterator.
   */
  override def iterator: Iterator[K] = {
    if (spilledMaps.isEmpty) {
      currentSet.iterator.asInstanceOf[Iterator[K]]
    } else {
      new ExternalIterator()
    }
  }
  
  /**
   * An iterator that sort-merges keys from the in-memory set and the spilled sets
   */
  private class ExternalIterator extends Iterator[K] {
    
    // A queue that maintains a buffer for each stream we are currently merging
    // This queue maintains the invariant that it only contains non-empty buffers
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]
    
    // Input streams are derived both from the in-memory set and spilled sets on disk
    // The in-memory set is sorted in place, while the spilled sets are already in sorted order
    private val sortedMap = currentSet.destructiveSortedIterator(keyComparator)
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)
    
    private var keysBuffer: OpenHashSet[Any] = null
    private var keysIterator: Iterator[Any] = null
    
    inputStreams.foreach { it =>
      val keys = new ArrayBuffer[K]
      readNextHashCode(it.asInstanceOf[BufferedIterator[K]], keys)
      if (keys.length > 0) {
        mergeHeap.enqueue(new StreamBuffer(it.asInstanceOf[BufferedIterator[K]], keys))
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
    private def readNextHashCode(it: BufferedIterator[K], buf: ArrayBuffer[K]): Unit = {
      if (it.hasNext) {
        var k = it.next()
        buf += k
        val minHash = hashKey(k)
        while (it.hasNext && it.head.hashCode() == minHash) {
          k = it.next()
          buf += k
        }
      }
    }
    
    /**
     * Return true if there exists an input stream that still has unvisited keys.
     */
    override def hasNext: Boolean = (keysIterator != null && keysIterator.hasNext) ||
                                    mergeHeap.length > 0
    
    /**
     * Select a key with the minimum hash, then combine all values with the same key from all
     * input streams.
     */
    override def next(): K = {
      if (keysIterator != null && keysIterator.hasNext) {
        return keysIterator.next.asInstanceOf[K]
      }
      if (mergeHeap.length == 0) {
        throw new NoSuchElementException
      }
      // Select a key from the StreamBuffer that holds the lowest key hash
      val minBuffer = mergeHeap.dequeue()
      keysBuffer = new OpenHashSet[Any]()
      val it = minBuffer.keys.iterator
      while (it.hasNext) {
        keysBuffer.add(it.next)
      }
      minBuffer.keys.clear
      
      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      val minHash = minBuffer.minKeyHash
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (mergeHeap.length > 0 && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        val it = newBuffer.keys.iterator
        while (it.hasNext) {
          keysBuffer.add(it.next)
        }
        newBuffer.keys.clear
        mergedBuffers += newBuffer
      }
      
      // Repopulate each visited stream buffer and add it back to the queue if it is non-empty
      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {
          readNextHashCode(buffer.iterator, buffer.keys)
        }
        if (!buffer.isEmpty) {
          mergeHeap.enqueue(buffer)
        }
      }
      
      keysIterator = keysBuffer.iterator
      if (keysIterator != null && keysIterator.hasNext) {
        return keysIterator.next.asInstanceOf[K]
      } else {
        throw new NoSuchElementException("Should not reached here")
      }
    }
    
    /**
     * A buffer for streaming from a set iterator (in-memory or on-disk) sorted by key hash.
     * Each buffer maintains all of the keys with what is currently the lowest hash
     * code among keys in the stream. There may be multiple keys if there are hash collisions.
     * Note that because when we spill data out, we only spill one value for each key, there is
     * at most one element for each key.
     *
     * StreamBuffers are ordered by the minimum key hash currently available in their stream so
     * that we can put them into a heap and sort that.
     */
    private class StreamBuffer(
        val iterator: BufferedIterator[K],
        val keys: ArrayBuffer[K])
      extends Comparable[StreamBuffer] {

      def isEmpty = keys.length == 0

      // Invalid if there are no more pairs in this stream
      def minKeyHash: Int = {
        assert(keys.length > 0)
        hashKey(keys.head)
      }

      override def compareTo(other: StreamBuffer): Int = {
        // descending order because mutable.PriorityQueue dequeues the max, not the min
        if (other.minKeyHash < minKeyHash) -1 else if (other.minKeyHash == minKeyHash) 0 else 1
      }
    }
  }
  
  /**
   * An iterator that returns keys in sorted order from an on-disk set
   */
  private class DiskSetIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[K]
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
    private var nextItem: K = null.asInstanceOf[K]
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
     * Return the next key from the deserialization stream.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): K = {
      try {
        val item = deserializeStream.readObject().asInstanceOf[K]
        objectsRead += 1
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        item
      } catch {
        case e: EOFException =>
          cleanup()
          null.asInstanceOf[K]
      }
    }
    
    override def hasNext: Boolean = {
      if (nextItem == null) {
        if (deserializeStream == null) {
          return false
        }
        nextItem = readNextItem()
      }
      nextItem != null && !nextItem.equals(null)
    }

    override def next(): K = {
      val item = if (nextItem == null || nextItem.equals(null)) readNextItem() else nextItem
      if (item == null || item.equals(null)) {
        throw new NoSuchElementException
      }
      nextItem = null.asInstanceOf[K]
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
  
  /** Convenience function to hash the given key. */
  private def hashKey(k: K): Int = ExternalSet.hash(k)
}

private[spark] object ExternalSet {

  /**
   * Return the hash code of the given object. If the object is null, return a special hash code.
   */
  private def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }

  /**
   * A comparator which sorts arbitrary keys based on their hash codes.
   */
  private class HashComparator[T : scala.reflect.ClassTag] extends Comparator[T] {
    def compare(key1: T, key2: T): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }
}
