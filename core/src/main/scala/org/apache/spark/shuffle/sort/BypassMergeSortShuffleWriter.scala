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

package org.apache.spark.shuffle.sort

import java.io.{File, FileInputStream, FileOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, BlockManager, BlockObjectWriter}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection._
import org.apache.spark.{Logging, Partitioner, SparkConf, TaskContext}


/**
 * This class handles the `bypassMergeSort` sort-shuffle write path. This path used to be part of
 * [[ExternalSorter]]. Here is its original documentation:
 *
 * As a special case, if no Ordering and no Aggregator is given, and the number of partitions is
 * less than spark.shuffle.sort.bypassMergeThreshold, we bypass the merge-sort and just write to
 * separate files for each partition each time we spill, similar to the HashShuffleWriter. We can
 * then concatenate these files to produce a single sorted file, without having to serialize and
 * de-serialize each item twice (as is needed during the merge). This speeds up the map side of
 * groupBy, sort, etc operations since they do no partial aggregation.
 */
private[spark] class BypassMergeSortShuffleWriter[K, V](
    conf: SparkConf,
    blockManager: BlockManager,
    partitioner: Partitioner,
    serializer: Option[Serializer] = None)
  extends Logging
  with Spillable[WritablePartitionedPairCollection[K, V]]
  with SortShuffleSorter[K, V] {

  private[this] val numPartitions = partitioner.numPartitions
  private[this] val shouldPartition = numPartitions > 1
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.getPartition(key) else 0
  }

  private val spillingEnabled = conf.getBoolean("spark.shuffle.spill", true)
  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024
  private val transferToEnabled = conf.getBoolean("spark.file.transferTo", true)

  private val ser = Serializer.getSerializer(serializer)
  private val serInstance = ser.newInstance()

  /**
   * Allocates a new buffer. Called in the constructor and after every spill.
   */
  private def newBuffer: () => WritablePartitionedPairCollection[K, V] with SizeTracker = {
    val useSerializedPairBuffer =
      conf.getBoolean("spark.shuffle.sort.serializeMapOutputs", true) &&
      ser.supportsRelocationOfSerializedObjects
    if (useSerializedPairBuffer) {
      val kvChunkSize = conf.getInt("spark.shuffle.sort.kvChunkSize", 1 << 22) // 4 MB
      () => new PartitionedSerializedPairBuffer(metaInitialRecords = 256, kvChunkSize, serInstance)
    } else {
      () => new PartitionedPairBuffer[K, V]
    }
  }
  private var buffer = newBuffer()

  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  /**
   * Information about a spilled file.
   *
   * @param file the file
   * @param blockId the block id
   * @param serializerBatchSizes sizes, in bytes, of "batches" written by the serializer as we
   *                             periodically reset its stream
   * @param elementsPerPartition the number of elements in each partition, used to efficiently
   *                             kepe track of partitions when merging.
   */
  private[this] case class SpilledFile(
    file: File,
    blockId: BlockId,
    serializerBatchSizes: Array[Long],
    elementsPerPartition: Array[Long])
  private val spills = new ArrayBuffer[SpilledFile]

  /** Array of file writers for each partition, used if we've spilled */
  private var partitionWriters: Array[BlockObjectWriter] = _

  /**
   * Write metrics for spill. This is initialized when partitionWriters is created */
  private var spillWriteMetrics: ShuffleWriteMetrics = _

  def insertAll(records: Iterator[_ <: Product2[K, V]]): Unit = {
    // SPARK-4479: Also bypass buffering if merge sort is bypassed to avoid defensive copies
    if (records.hasNext) {
      spill(
        WritablePartitionedIterator.fromIterator(records.map { kv =>
          ((getPartition(kv._1), kv._1), kv._2.asInstanceOf[V])
        })
      )
    }
  }

  /**
   * Spill the current in-memory collection to disk if needed.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    if (spillingEnabled && maybeSpill(buffer, buffer.estimateSize())) {
      buffer = newBuffer()
    }
  }

  /**
   * Spill our in-memory collection to separate files, one for each partition, then clears the
   * collection.
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, V]): Unit = {
    spill(collection.writablePartitionedIterator())
  }

  private def spill(iterator: WritablePartitionedIterator): Unit = {
    // Create our file writers if we haven't done so yet
    if (partitionWriters == null) {
      spillWriteMetrics = new ShuffleWriteMetrics()
      val openStartTime = System.nanoTime
      partitionWriters = Array.fill(numPartitions) {
        // Because these files may be read during shuffle, their compression must be controlled by
        // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
        // createTempShuffleBlock here; see SPARK-3426 for more context.
        val (blockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()
        val writer = blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize,
          spillWriteMetrics)
        writer.open()
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, and can take a long time in aggregate when we open many files, so should be
      // included in the shuffle write time.
      spillWriteMetrics.incShuffleWriteTime(System.nanoTime - openStartTime)
    }

    // No need to sort stuff, just write each element out
    while (iterator.hasNext) {
      val partitionId = iterator.nextPartition()
      iterator.writeNext(partitionWriters(partitionId))
    }
  }

  /**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter and can go through an efficient path of just concatenating
   * binary files if we decided to avoid merge-sorting.
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @param context a TaskContext for a running Spark task, for us to update shuffle metrics.
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
  def writePartitionedFile(
    blockId: BlockId,
    context: TaskContext,
    outputFile: File): Array[Long] = {

    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)

    if (spills.isEmpty) {
      // Case where we only have in-memory data
      assert (partitionWriters == null)
      assert (spillWriteMetrics == null)

      val it = buffer.writablePartitionedIterator()
      while (it.hasNext) {
        val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
          context.taskMetrics.shuffleWriteMetrics.get)
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)
        }
        writer.commitAndClose()
        val segment = writer.fileSegment()
        lengths(partitionId) = segment.length
      }
    } else {
      // Case where we have both in-memory and spilled data.
      assert (partitionWriters != null)
      assert (spillWriteMetrics != null)
      // For simplicity, spill out the current in-memory collection so that everything is in files.
      spill(buffer)
      partitionWriters.foreach(_.commitAndClose())
      val out = new FileOutputStream(outputFile, true)
      val writeStartTime = System.nanoTime
      Utils.tryWithSafeFinally {
        for (i <- 0 until numPartitions) {
          val in = new FileInputStream(partitionWriters(i).fileSegment().file)
          Utils.tryWithSafeFinally {
            lengths(i) = Utils.copyStream(in, out, closeStreams = false, transferToEnabled)
          } {
            in.close()
          }
        }
      } {
        out.close()
        context.taskMetrics.shuffleWriteMetrics.foreach { m =>
          m.incShuffleWriteTime(System.nanoTime - writeStartTime)
        }
      }
    }
    context.taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics.incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics.shuffleWriteMetrics.foreach { m =>
      if (spillWriteMetrics != null) {
        m.incShuffleBytesWritten(spillWriteMetrics.shuffleBytesWritten)
        m.incShuffleWriteTime(spillWriteMetrics.shuffleWriteTime)
        m.incShuffleRecordsWritten(spillWriteMetrics.shuffleRecordsWritten)
      }
    }

    lengths
  }

  def stop(): Unit = {
    spills.foreach(s => s.file.delete())
    spills.clear()
    if (partitionWriters != null) {
      partitionWriters.foreach { w =>
        w.revertPartialWritesAndClose()
        blockManager.diskBlockManager.getFile(w.blockId).delete()
      }
      partitionWriters = null
    }
  }
}
