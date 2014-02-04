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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.io.FastBufferedInputStream

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.io.LZFCompressionCodec
import org.apache.spark.serializer.{KryoDeserializationStream, Serializer}
import org.apache.spark.storage.{BlockId, BlockManager, DiskBlockObjectWriter}

/**
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

private[spark] class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializerManager.default,
    blockManager: BlockManager = SparkEnv.get.blockManager)
  extends Iterable[(K, C)] with Serializable with Logging {

  import ExternalAppendOnlyMap._

  private var currentMap = new SizeTrackingAppendOnlyMap[K, C]
  private val spilledMaps = new ArrayBuffer[DiskMapIterator]
  private val sparkConf = SparkEnv.get.conf
  private val diskBlockManager = blockManager.diskBlockManager

  // Collective memory threshold shared across all running tasks
  private val maxMemoryThreshold = {
    val memoryFraction = sparkConf.getDouble("spark.shuffle.memoryFraction", 0.3)
    val safetyFraction = sparkConf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

  // Number of pairs in the in-memory map
  private var numPairsInMemory = 0L

  // Number of in-memory pairs inserted before tracking the map's shuffle memory usage
  private val trackMemoryThreshold = 1000

  // Size of object batches when reading/writing from serializers. Objects are written in
  // batches, with each batch using its own serialization stream. This cuts down on the size
  // of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excess copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)

  // How many times we have spilled so far
  private var spillCount = 0

  // Number of bytes spilled in total
  private var _memoryBytesSpilled = 0L
  private var _diskBytesSpilled = 0L

  private val fileBufferSize = sparkConf.getInt("spark.shuffle.file.buffer.kb", 100) * 1024
  private val syncWrites = sparkConf.getBoolean("spark.shuffle.sync", false)
  private val comparator = new KCComparator[K, C]
  private val ser = serializer.newInstance()

  /**
   * Insert the given key and value into the map.
   *
   * If the underlying map is about to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insert(key: K, value: V) {
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, value) else createCombiner(value)
    }
    if (numPairsInMemory > trackMemoryThreshold && currentMap.atGrowThreshold) {
      val mapSize = currentMap.estimateSize()
      var shouldSpill = false
      val shuffleMemoryMap = SparkEnv.get.shuffleMemoryMap

      // Atomically check whether there is sufficient memory in the global pool for
      // this map to grow and, if possible, allocate the required amount
      shuffleMemoryMap.synchronized {
        val threadId = Thread.currentThread().getId
        val previouslyOccupiedMemory = shuffleMemoryMap.get(threadId)
        val availableMemory = maxMemoryThreshold -
          (shuffleMemoryMap.values.sum - previouslyOccupiedMemory.getOrElse(0L))

        // Assume map growth factor is 2x
        shouldSpill = availableMemory < mapSize * 2
        if (!shouldSpill) {
          shuffleMemoryMap(threadId) = mapSize * 2
        }
      }
      // Do not synchronize spills
      if (shouldSpill) {
        spill(mapSize)
      }
    }
    currentMap.changeValue(key, update)
    numPairsInMemory += 1
  }

  /**
   * Sort the existing contents of the in-memory map and spill them to a temporary file on disk
   */
  private def spill(mapSize: Long) {
    spillCount += 1
    logWarning("Spilling in-memory map of %d MB to disk (%d time%s so far)"
      .format(mapSize / (1024 * 1024), spillCount, if (spillCount > 1) "s" else ""))
    val (blockId, file) = diskBlockManager.createTempBlock()

    /* IMPORTANT NOTE: To avoid having to keep large object graphs in memory, this approach
    *  closes and re-opens serialization and compression streams within each file. This makes some
     * assumptions about the way that serialization and compression streams work, specifically:
     *
     * 1) The serializer input streams do not pre-fetch data from the underlying stream.
     *
     * 2) Several compression streams can be opened, written to, and flushed on the write path
     *    while only one compression input stream is created on the read path
     *
     * In practice (1) is only true for Java, so we add a special fix below to make it work for
     * Kryo. (2) is only true for LZF and not Snappy, so we coerce this to use LZF.
     *
     * To avoid making these assumptions we should create an intermediate stream that batches
     * objects and sends an EOF to the higher layer streams to make sure they never prefetch data.
     * This is a bit tricky because, within each segment, you'd need to track the total number
     * of bytes written and then re-wind and write it at the beginning of the segment. This will
     * most likely require using the file channel API.
     */

    val shouldCompress = blockManager.shouldCompress(blockId)
    val compressionCodec = new LZFCompressionCodec(sparkConf)
    def wrapForCompression(outputStream: OutputStream) = {
      if (shouldCompress) compressionCodec.compressedOutputStream(outputStream) else outputStream
    }

    def getNewWriter = new DiskBlockObjectWriter(blockId, file, serializer, fileBufferSize,
      wrapForCompression, syncWrites)

    var writer = getNewWriter
    var objectsWritten = 0
    try {
      val it = currentMap.destructiveSortedIterator(comparator)
      while (it.hasNext) {
        val kv = it.next()
        writer.write(kv)
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          writer.commit()
          writer.close()
          _diskBytesSpilled += writer.bytesWritten
          writer = getNewWriter
          objectsWritten = 0
        }
      }

      if (objectsWritten > 0) writer.commit()
    } finally {
      // Partial failures cannot be tolerated; do not revert partial writes
      writer.close()
      _diskBytesSpilled += writer.bytesWritten
    }
    currentMap = new SizeTrackingAppendOnlyMap[K, C]
    spilledMaps.append(new DiskMapIterator(file, blockId))

    // Reset the amount of shuffle memory used by this map in the global pool
    val shuffleMemoryMap = SparkEnv.get.shuffleMemoryMap
    shuffleMemoryMap.synchronized {
      shuffleMemoryMap(Thread.currentThread().getId) = 0
    }
    numPairsInMemory = 0
    _memoryBytesSpilled += mapSize
  }

  def memoryBytesSpilled: Long = _memoryBytesSpilled
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

    // A fixed-size queue that maintains a buffer for each stream we are currently merging
    val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    val sortedMap = currentMap.destructiveSortedIterator(comparator)
    val inputStreams = Seq(sortedMap) ++ spilledMaps

    inputStreams.foreach { it =>
      val kcPairs = getMorePairs(it)
      mergeHeap.enqueue(StreamBuffer(it, kcPairs))
    }

    /**
     * Fetch from the given iterator until a key of different hash is retrieved. In the
     * event of key hash collisions, this ensures no pairs are hidden from being merged.
     * Assume the given iterator is in sorted order.
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

    /**
     * Return true if there exists an input stream that still has unvisited pairs
     */
    override def hasNext: Boolean = mergeHeap.exists(!_.pairs.isEmpty)

    /**
     * Select a key with the minimum hash, then combine all values with the same key from all input streams.
     */
    override def next(): (K, C) = {
      // Select a key from the StreamBuffer that holds the lowest key hash
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

  /**
   * An iterator that returns (K, C) pairs in sorted order from an on-disk map
   */
  private class DiskMapIterator(file: File, blockId: BlockId) extends Iterator[(K, C)] {
    val fileStream = new FileInputStream(file)
    val bufferedStream = new FastBufferedInputStream(fileStream, fileBufferSize)

    val shouldCompress = blockManager.shouldCompress(blockId)
    val compressionCodec = new LZFCompressionCodec(sparkConf)
    val compressedStream =
      if (shouldCompress) {
        compressionCodec.compressedInputStream(bufferedStream)
      } else {
        bufferedStream
      }
    var deserializeStream = ser.deserializeStream(compressedStream)
    var objectsRead = 0

    var nextItem: (K, C) = null
    var eof = false

    def readNextItem(): (K, C) = {
      if (!eof) {
        try {
          if (objectsRead == serializerBatchSize) {
            val newInputStream = deserializeStream match {
              case stream: KryoDeserializationStream =>
                // Kryo's serializer stores an internal buffer that pre-fetches from the underlying
                // stream. We need to capture this buffer and feed it to the new serialization
                // stream so that the bytes are not lost.
                val kryoInput = stream.input
                val remainingBytes = kryoInput.limit() - kryoInput.position()
                val extraBuf = kryoInput.readBytes(remainingBytes)
                new SequenceInputStream(new ByteArrayInputStream(extraBuf), compressedStream)
              case _ => compressedStream
            }
            deserializeStream = ser.deserializeStream(newInputStream)
            objectsRead = 0
          }
          objectsRead += 1
          return deserializeStream.readObject().asInstanceOf[(K, C)]
        } catch {
          case e: EOFException =>
            eof = true
            cleanup()
        }
      }
      null
    }

    override def hasNext: Boolean = {
      if (nextItem == null) {
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
