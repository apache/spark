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

import it.unimi.dsi.fastutil.io.FastBufferedInputStream

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}

import org.apache.spark.{SparkConf, Logging, SparkEnv}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{DiskBlockManager, DiskBlockObjectWriter}

/**
 * An append-only map that spills sorted content to disk when the memory threshold is exceeded.
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
 * A few parameters control the memory threshold:
 *
 *   `spark.shuffle.memoryFraction` specifies the collective amount of memory used for storing
 *   these maps as a fraction of the executor's total memory. Since each concurrently running
 *   task maintains one map, the actual threshold for each map is this quantity divided by the
 *   number of running tasks.
 *
 *   `spark.shuffle.safetyFraction` specifies an additional margin of safety as a fraction of
 *   this threshold, in case map size estimation is not sufficiently accurate.
 *
 *   `spark.shuffle.updateThresholdInterval` controls how frequently each thread checks on
 *   shared executor state to update its local memory threshold.
 */

private[spark] class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializerManager.default,
    diskBlockManager: DiskBlockManager = SparkEnv.get.blockManager.diskBlockManager)
  extends Iterable[(K, C)] with Serializable with Logging {

  import ExternalAppendOnlyMap._

  private var currentMap = new SizeTrackingAppendOnlyMap[K, C]
  private val spilledMaps = new ArrayBuffer[DiskMapIterator]
  private val sparkConf = new SparkConf()

  // Collective memory threshold shared across all running tasks
  private val maxMemoryThreshold = {
    val memoryFraction = sparkConf.getDouble("spark.shuffle.memoryFraction", 0.75)
    val safetyFraction = sparkConf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

  // Maximum size for this map before a spill is triggered
  private var spillThreshold = maxMemoryThreshold

  // How often to update spillThreshold
  private val updateThresholdInterval =
    sparkConf.getInt("spark.shuffle.updateThresholdInterval", 100)

  private val fileBufferSize = sparkConf.getInt("spark.shuffle.file.buffer.kb", 100) * 1024
  private val syncWrites = sparkConf.get("spark.shuffle.sync", "false").toBoolean
  private val comparator = new KCComparator[K, C]
  private val ser = serializer.newInstance()
  private var insertCount = 0
  private var spillCount = 0

  def insert(key: K, value: V) {
    insertCount += 1
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, value) else createCombiner(value)
    }
    currentMap.changeValue(key, update)
    if (insertCount % updateThresholdInterval == 1) {
      updateSpillThreshold()
    }
    if (currentMap.estimateSize() > spillThreshold) {
      spill()
    }
  }

  // TODO: differentiate ShuffleMapTask's from ResultTask's
  private def updateSpillThreshold() {
    val numRunningTasks = math.max(SparkEnv.get.numRunningTasks, 1)
    spillThreshold = maxMemoryThreshold / numRunningTasks
  }

  private def spill() {
    spillCount += 1
    logWarning("In-memory map exceeded %s MB! Spilling to disk (%d time%s so far)"
      .format(spillThreshold / (1024 * 1024), spillCount, if (spillCount > 1) "s" else ""))
    val (blockId, file) = diskBlockManager.createTempBlock()
    val writer =
      new DiskBlockObjectWriter(blockId, file, serializer, fileBufferSize, identity, syncWrites)
    try {
      val it = currentMap.destructiveSortedIterator(comparator)
      while (it.hasNext) {
        val kv = it.next()
        writer.write(kv)
      }
      writer.commit()
    } finally {
      // Partial failures cannot be tolerated; do not revert partial writes
      writer.close()
    }
    currentMap = new SizeTrackingAppendOnlyMap[K, C]
    spilledMaps.append(new DiskMapIterator(file))
  }

  override def iterator: Iterator[(K, C)] = {
    if (spilledMaps.isEmpty) {
      currentMap.iterator
    } else {
      new ExternalIterator()
    }
  }

  /** An iterator that sort-merges (K, C) pairs from the in-memory and on-disk maps */
  private class ExternalIterator extends Iterator[(K, C)] {

    // A fixed-size queue that maintains a buffer for each stream we are currently merging
    val mergeHeap = new PriorityQueue[StreamBuffer]

    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    val inputStreams = Seq(currentMap.destructiveSortedIterator(comparator)) ++ spilledMaps

    inputStreams.foreach{ it =>
      val kcPairs = getMorePairs(it)
      mergeHeap.enqueue(StreamBuffer(it, kcPairs))
    }

    /**
     * Fetch from the given iterator until a key of different hash is retrieved. In the
     * event of key hash collisions, this ensures no pairs are hidden from being merged.
     */
    def getMorePairs(it: Iterator[(K, C)]): ArrayBuffer[(K, C)] = {
      val kcPairs = new ArrayBuffer[(K, C)]
      if (it.hasNext) {
        var kc = it.next()
        kcPairs += kc
        val minHash = kc._1.hashCode()
        while (it.hasNext && kc._1.hashCode() == minHash) {
          kc = it.next()
          kcPairs += kc
        }
      }
      kcPairs
    }

    /**
     * If the given buffer contains a value for the given key, merge that value into
     * baseCombiner and remove the corresponding (K, C) pair from the buffer
     */
    def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C = {
      var i = 0
      while (i < buffer.pairs.size) {
        val (k, c) = buffer.pairs(i)
        if (k == key) {
          buffer.pairs.remove(i)
          return mergeCombiners(baseCombiner, c)
        }
        i += 1
      }
      baseCombiner
    }

    override def hasNext: Boolean = {
      mergeHeap.foreach{ buffer =>
        if (!buffer.pairs.isEmpty) {
          return true
        }
      }
      false
    }

    override def next(): (K, C) = {
      // Select a return key from the StreamBuffer that holds the lowest key hash
      val minBuffer = mergeHeap.dequeue()
      val (minPairs, minHash) = (minBuffer.pairs, minBuffer.minKeyHash)
      if (minPairs.length == 0) {
        // Should only happen when no other stream buffers have any pairs left
        throw new NoSuchElementException
      }
      var (minKey, minCombiner) = minPairs.remove(0)
      assert(minKey.hashCode() == minHash)

      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (!mergeHeap.isEmpty && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        mergedBuffers += newBuffer
      }

      // Repopulate each visited stream buffer and add it back to the merge heap
      mergedBuffers.foreach { buffer =>
        if (buffer.pairs.length == 0) {
          buffer.pairs ++= getMorePairs(buffer.iterator)
        }
        mergeHeap.enqueue(buffer)
      }

      (minKey, minCombiner)
    }

    /**
     * A buffer for streaming from a map iterator (in-memory or on-disk) sorted by key hash.
     * Each buffer maintains the lowest-ordered keys in the corresponding iterator. Due to
     * hash collisions, it is possible for multiple keys to be "tied" for being the lowest.
     *
     * StreamBuffers are ordered by the minimum key hash found across all of their own pairs.
     */
    case class StreamBuffer(iterator: Iterator[(K, C)], pairs: ArrayBuffer[(K, C)])
      extends Comparable[StreamBuffer] {

      def minKeyHash: Int = {
        if (pairs.length > 0){
          // pairs are already sorted by key hash
          pairs(0)._1.hashCode()
        } else {
          Int.MaxValue
        }
      }

      override def compareTo(other: StreamBuffer): Int = {
        // minus sign because mutable.PriorityQueue dequeues the max, not the min
        -minKeyHash.compareTo(other.minKeyHash)
      }
    }
  }

  // Iterate through (K, C) pairs in sorted order from an on-disk map
  private class DiskMapIterator(file: File) extends Iterator[(K, C)] {
    val fileStream = new FileInputStream(file)
    val bufferedStream = new FastBufferedInputStream(fileStream)
    val deserializeStream = ser.deserializeStream(bufferedStream)
    var nextItem: Option[(K, C)] = None
    var eof = false

    def readNextItem(): Option[(K, C)] = {
      if (!eof) {
        try {
          return Some(deserializeStream.readObject().asInstanceOf[(K, C)])
        } catch {
          case e: EOFException =>
            eof = true
            cleanup()
        }
      }
      None
    }

    override def hasNext: Boolean = {
      nextItem match {
        case Some(item) => true
        case None =>
          nextItem = readNextItem()
          nextItem.isDefined
      }
    }

    override def next(): (K, C) = {
      nextItem match {
        case Some(item) =>
          nextItem = None
          item
        case None =>
          val item = readNextItem()
          item.getOrElse(throw new NoSuchElementException)
      }
    }

    // TODO: Ensure this gets called even if the iterator isn't drained.
    def cleanup() {
      deserializeStream.close()
      file.delete()
    }
  }
}

private[spark] object ExternalAppendOnlyMap {
  private class KCComparator[K, C] extends Comparator[(K, C)] {
    def compare(kc1: (K, C), kc2: (K, C)): Int = {
      kc1._1.hashCode().compareTo(kc2._1.hashCode())
    }
  }
}
