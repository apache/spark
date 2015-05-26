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

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, BlockManager, BlockObjectWriter}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection._

/**
 * This class handles sort-based shuffle's `bypassMergeSort` write path, which is used for shuffles
 * for which no Ordering and no Aggregator is given and the number of partitions is
 * less than `spark.shuffle.sort.bypassMergeThreshold`.
 *
 * This path used to be part of [[ExternalSorter]] but was refactored into its own class in order to
 * reduce code complexity; see SPARK-7855 for more details.
 *
 * There have been proposals to completely remove this code path; see SPARK-6026 for details.
 */
private[spark] class BypassMergeSortShuffleWriter[K, V](
    conf: SparkConf,
    blockManager: BlockManager,
    partitioner: Partitioner,
    writeMetrics: ShuffleWriteMetrics,
    serializer: Serializer)
  extends Logging with SortShuffleFileWriter[K, V] {

  private[this] val numPartitions = partitioner.numPartitions

  /** Array of file writers for each partition */
  private[this] var partitionWriters: Array[BlockObjectWriter] = _

  def insertAll(records: Iterator[_ <: Product2[K, V]]): Unit = {
    assert (partitionWriters == null)
    if (records.hasNext) {
      val serInstance = serializer.newInstance()
      // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
      val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024
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
        partitionWriters(partitioner.getPartition(key)).write(key, record._2)
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
  def writePartitionedFile(blockId: BlockId, context: TaskContext, file: File): Array[Long] = {
    if (partitionWriters == null) {
      // We were passed an empty iterator
      Array.fill(numPartitions)(0L)
    } else {
      partitionWriters.foreach(_.commitAndClose())

      // Track location of each range in the output file
      val lengths = new Array[Long](numPartitions)

      val transferToEnabled = conf.getBoolean("spark.file.transferTo", true)
      val out = new FileOutputStream(file, true)
      val writeStartTime = System.nanoTime
      Utils.tryWithSafeFinally {
        for (i <- 0 until numPartitions) {
          val in = new FileInputStream(partitionWriters(i).fileSegment().file)
          Utils.tryWithSafeFinally {
            lengths(i) = Utils.copyStream(in, out, closeStreams = false, transferToEnabled)
          } {
            in.close()
          }
          if (!blockManager.diskBlockManager.getFile(partitionWriters(i).blockId).delete()) {
            logError("Unable to delete file for partition i. ")
          }
        }
      } {
        out.close()
        context.taskMetrics().shuffleWriteMetrics.foreach { m =>
          m.incShuffleWriteTime(System.nanoTime - writeStartTime)
        }
      }

      lengths
    }
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
