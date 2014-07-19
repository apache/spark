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

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{Aggregator, SparkEnv, Logging, Partitioner}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.BlockId

/**
 * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
 * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
 * optionally sorts keys within each partition using a custom Comparator. Can output a single
 * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
 *
 * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
 *
 * @param aggregator optional Aggregator with combine functions to use for merging data
 * @param partitioner optional partitioner; if given, sort by partition ID and then key
 * @param ordering optional ordering to sort keys within each partition
 * @param serializer serializer to use
 */
private[spark] class ExternalSorter[K, V, C](
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Option[Serializer] = None) extends Logging {

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1

  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val ser = Serializer.getSerializer(serializer.getOrElse(null))
  private val serInstance = ser.newInstance()

  private val conf = SparkEnv.get.conf
  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 100) * 1024
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  // TODO: Would prefer to have an ArrayBuffer[Any] that we sort pairs of adjacent elements in.
  var map = new SizeTrackingAppendOnlyMap[(Int, K), C]
  var buffer = new SizeTrackingBuffer[((Int, K), C)]

  // Track how many elements we've read before we try to estimate memory. Ideally we'd use
  // map.size or buffer.size for this, but because users' Aggregators can potentially increase
  // the size of a merged element when we add values with the same key, it's safer to track
  // elements read from the input iterator.
  private var elementsRead = 0L
  private val trackMemoryThreshold = 1000

  // Spilling statistics
  private var spillCount = 0
  private var _memoryBytesSpilled = 0L
  private var _diskBytesSpilled = 0L

  // Collective memory threshold shared across all running tasks
  private val maxMemoryThreshold = {
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.3)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

  // For now, just compare them by partition; later we can compare by key as well
  private val comparator = new Comparator[((Int, K), C)] {
    override def compare(a: ((Int, K), C), b: ((Int, K), C)): Int = {
      a._1._1 - b._1._1
    }
  }

  // Information about a spilled file. Includes sizes in bytes of "batches" written by the
  // serializer as we periodically reset its stream, as well as number of elements in each
  // partition, used to efficiently keep track of partitions when merging.
  private case class SpilledFile(
    file: File,
    blockId: BlockId,
    serializerBatchSizes: ArrayBuffer[Long],
    elementsPerPartition: Array[Long])
  private val spills = new ArrayBuffer[SpilledFile]

  def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        elementsRead += 1
        kv = records.next()
        map.changeValue((getPartition(kv._1), kv._1), update)
        maybeSpill(usingMap = true)
      }
    } else {
      // Stick values into our buffer
      while (records.hasNext) {
        elementsRead += 1
        val kv = records.next()
        buffer += (((getPartition(kv._1), kv._1), kv._2.asInstanceOf[C]))
        maybeSpill(usingMap = false)
      }
    }
  }

  private def maybeSpill(usingMap: Boolean): Unit = {
    val collection: SizeTrackingCollection[((Int, K), C)] = if (usingMap) map else buffer

    if (elementsRead > trackMemoryThreshold && collection.atGrowThreshold) {
      // TODO: This is code from ExternalAppendOnlyMap that doesn't work if there are two external
      // collections being used in the same task. However we'll just copy it for now.

      val currentSize = collection.estimateSize()
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
        shouldSpill = availableMemory < currentSize * 2
        if (!shouldSpill) {
          shuffleMemoryMap(threadId) = currentSize * 2
        }
      }
      // Do not synchronize spills
      if (shouldSpill) {
        spill(currentSize, usingMap)
      }
    }
  }

  /**
   * Spill the current in-memory collection to disk, adding a new file to spills, and clear it.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  def spill(memorySize: Long, usingMap: Boolean): Unit = {
    val collection: SizeTrackingCollection[((Int, K), C)] = if (usingMap) map else buffer
    val memorySize = collection.estimateSize()

    spillCount += 1
    logWarning("Spilling in-memory batch of %d MB to disk (%d spill%s so far)"
      .format(memorySize / (1024 * 1024), spillCount, if (spillCount > 1) "s" else ""))
    val (blockId, file) = diskBlockManager.createTempBlock()
    var writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize)
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    // TODO: this should become a sparser data structure
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables
    def flush() = {
      writer.commit()
      val bytesWritten = writer.bytesWritten
      batchSizes.append(bytesWritten)
      _diskBytesSpilled += bytesWritten
      objectsWritten = 0
    }

    try {
      val it = collection.destructiveSortedIterator(comparator)
      while (it.hasNext) {
        val elem = it.next()
        val partitionId = elem._1._1
        val key = elem._1._2
        val value = elem._2
        writer.write(key)
        writer.write(value)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
          writer.close()
          writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize)
        }
      }
      if (objectsWritten > 0) {
        flush()
      }
    } finally {
      // Partial failures cannot be tolerated; do not revert partial writes
      writer.close()
    }

    if (usingMap) {
      map = new SizeTrackingAppendOnlyMap[(Int, K), C]
    } else {
      buffer = new SizeTrackingBuffer[((Int, K), C)]
    }

    spills.append(SpilledFile(file, blockId, batchSizes, elementsPerPartition))
    _memoryBytesSpilled += memorySize
  }

  /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   */
  def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {
    // TODO: merge intermediate results if they are sorted by the comparator
    val readers = spills.map(new SpillReader(_))
    val inMemBuffered = inMemory.buffered
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new Iterator[(K, C)] {
        override def hasNext: Boolean = {
          inMemBuffered.hasNext && inMemBuffered.head._1._1 == p
        }
        override def next(): (K, C) = {
          val elem = inMemBuffered.next()
          (elem._1._2, elem._2)
        }
      }
      (p, readers.iterator.flatMap(_.readNextPartition()) ++ inMemIterator)
    }
  }

  /**
   * An internal class for reading a spilled file partition by partition. Expects all the
   * partitions to be requested in order.
   */
  private class SpillReader(spill: SpilledFile) {
    val fileStream = new FileInputStream(spill.file)
    val bufferedStream = new BufferedInputStream(fileStream, fileBufferSize)

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    var batchStream = nextBatchStream()
    var compressedStream = blockManager.wrapForCompression(spill.blockId, batchStream)
    var deserStream = serInstance.deserializeStream(compressedStream)
    var nextItem: (K, C) = null
    var finished = false

    // Track which partition and which batch stream we're in
    var partitionId = 0
    var indexInPartition = -1L  // Just to make sure we start at index 0
    var batchStreamsRead = 0
    var indexInBatch = -1

    /** Construct a stream that only reads from the next batch */
    def nextBatchStream(): InputStream = {
      if (batchStreamsRead < spill.serializerBatchSizes.length) {
        batchStreamsRead += 1
        ByteStreams.limit(bufferedStream, spill.serializerBatchSizes(batchStreamsRead - 1))
      } else {
        // No more batches left
        bufferedStream
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream and update partitionId,
     * indexInPartition, indexInBatch and such to match its location.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): (K, C) = {
      try {
        if (finished) {
          return null
        }
        // Start reading the next batch if we're done with this one
        indexInBatch += 1
        if (indexInBatch == serializerBatchSize) {
          batchStream = nextBatchStream()
          compressedStream = blockManager.wrapForCompression(spill.blockId, batchStream)
          deserStream = serInstance.deserializeStream(compressedStream)
          indexInBatch = 0
        }
        // Update the partition location of the element we're reading
        indexInPartition += 1
        while (indexInPartition == spill.elementsPerPartition(partitionId)) {
          partitionId += 1
          indexInPartition = 0
        }
        val k = deserStream.readObject().asInstanceOf[K]
        val c = deserStream.readObject().asInstanceOf[C]
        if (partitionId == numPartitions - 1 &&
            indexInPartition == spill.elementsPerPartition(partitionId) - 1) {
          finished = true
          deserStream.close()
        }
        (k, c)
      } catch {
        case e: EOFException =>
          finished = true
          deserStream.close()
          null
      }
    }

    var nextPartitionToRead = 0

    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        // Check that we're still in the right partition; will be numPartitions at EOF
        partitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }
  }

  /**
   * Return an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   */
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: SizeTrackingCollection[((Int, K), C)] = if (usingMap) map else buffer
    merge(spills, collection.destructiveSortedIterator(comparator))
  }

  /**
   * Return an iterator over all the data written to this object.
   */
  def iterator: Iterator[Product2[K, C]] = partitionedIterator.flatMap(pair => pair._2)

  def stop(): Unit = ???

  def memoryBytesSpilled: Long = _memoryBytesSpilled

  def diskBytesSpilled: Long = _diskBytesSpilled
}
