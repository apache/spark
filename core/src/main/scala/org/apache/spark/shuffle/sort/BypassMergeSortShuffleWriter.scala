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
    writeMetrics: ShuffleWriteMetrics,
    serializer: Option[Serializer] = None)
  extends Logging with SortShuffleSorter[K, V] {

  private[this] val numPartitions = partitioner.numPartitions
  private[this] val shouldPartition = numPartitions > 1
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.getPartition(key) else 0
  }

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024
  private val transferToEnabled = conf.getBoolean("spark.file.transferTo", true)

  private val ser = Serializer.getSerializer(serializer)
  private val serInstance = ser.newInstance()

  /** Array of file writers for each partition */
  private var partitionWriters: Array[BlockObjectWriter] = _

  def insertAll(records: Iterator[_ <: Product2[K, V]]): Unit = {
    assert (partitionWriters == null)
    if (records.hasNext) {
      val openStartTime = System.nanoTime
      partitionWriters = Array.fill(numPartitions) {
        val (blockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()
        val writer =
          blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics)
        writer.open()
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, and can take a long time in aggregate when we open many files, so should be
      // included in the shuffle write time.
      writeMetrics.incShuffleWriteTime(System.nanoTime - openStartTime)

      while (records.hasNext) {
        val record = records.next()
        val key: K = record._1
        partitionWriters(getPartition(key)).write(key, record._2)
      }
    }
  }

  /**
   * Write all the data added into this writer into a single file in the disk store. This is
   * called by the SortShuffleWriter and can go through an efficient path of just concatenating
   * the per-partition binary files.
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

    // TODO: handle case where partition writers is null (e.g. we haven't written any data).

    partitionWriters.foreach(_.commitAndClose())
    // Concatenate the per-partition files.
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
        if (blockManager.diskBlockManager.getFile(partitionWriters(i).blockId).delete()) {
          logError("Unable to delete file for partition i. ")
        }
      }
    } {
      out.close()
      context.taskMetrics.shuffleWriteMetrics.foreach { m =>
        m.incShuffleWriteTime(System.nanoTime - writeStartTime)
      }
    }

    lengths
  }

  def stop(): Unit = {
    if (partitionWriters != null) {
      partitionWriters.foreach { w =>
        w.revertPartialWritesAndClose()
        blockManager.diskBlockManager.getFile(w.blockId).delete()
      }
      partitionWriters = null
    }
  }
}
