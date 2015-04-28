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
package org.apache.spark.streaming.rdd

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration

import org.apache.spark._
import org.apache.spark.rdd.BlockRDD
import org.apache.spark.storage.{StreamBlockId, BlockId, StorageLevel}
import org.apache.spark.streaming.util.{HdfsUtils, WriteAheadLogFileSegment, WriteAheadLogRandomReader}

/**
 * Partition class for [[org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD]].
 * It contains information about the id of the blocks having this partition's data and
 * the segment of the write ahead log that backs the partition.
 * @param index index of the partition
 * @param blockId id of the block having the partition data
 * @param segment segment of the write ahead log having the partition data
 */
private[streaming]
class WriteAheadLogBackedBlockRDDPartition(
    val index: Int,
    val blockId: BlockId,
    val isBlockIdValid: Boolean,
    val segment: WriteAheadLogFileSegment)
  extends Partition


/**
 * This class represents a special case of the BlockRDD where the data blocks in
 * the block manager are also backed by segments in write ahead logs. For reading
 * the data, this RDD first looks up the blocks by their ids in the block manager.
 * If it does not find them, it looks up the corresponding file segment. The finding
 * of the blocks by their ids can be skipped by setting the corresponding element in
 * isBlockIdValid to false. This is a performance optimization which does not affect
 * correctness, and it can be used in situations where it is known that the block
 * does not exist in the Spark executors (e.g. after a failed driver is restarted).
 *
 *
 * @param sc SparkContext
 * @param blockIds Ids of the blocks that contains this RDD's data
 * @param segments Segments in write ahead logs that contain this RDD's data
 * @param isBlockIdValid Whether the block Ids are valid (i.e., the blocks are present in the Spark
 *                         executors). If not, then block lookups by the block ids will be skipped.
 *                         By default, this is an empty array signifying true for all the blocks.
 * @param storeInBlockManager Whether to store in the block manager after reading from the segment
 * @param storageLevel storage level to store when storing in block manager
 *                     (applicable when storeInBlockManager = true)
 */
private[streaming]
class WriteAheadLogBackedBlockRDD[T: ClassTag](
    @transient sc: SparkContext,
    @transient blockIds: Array[BlockId],
    @transient segments: Array[WriteAheadLogFileSegment],
    @transient isBlockIdValid: Array[Boolean] = Array.empty,
    storeInBlockManager: Boolean = false,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER)
  extends BlockRDD[T](sc, blockIds) {

  require(
    blockIds.length == segments.length,
    s"Number of block Ids (${blockIds.length}) must be " +
      s" same as number of segments (${segments.length}})")

  require(
    isBlockIdValid.isEmpty || isBlockIdValid.length == blockIds.length,
    s"Number of elements in isBlockIdValid (${isBlockIdValid.length}) must be " +
      s" same as number of block Ids (${blockIds.length})")

  // Hadoop configuration is not serializable, so broadcast it as a serializable.
  @transient private val hadoopConfig = sc.hadoopConfiguration
  private val broadcastedHadoopConf = new SerializableWritable(hadoopConfig)

  setInvalidIfBlocksRemoved(false)

  override def getPartitions: Array[Partition] = {
    assertValid()
    Array.tabulate(blockIds.length) { i =>
      val isValid = if (isBlockIdValid.length == 0) true else isBlockIdValid(i)
      new WriteAheadLogBackedBlockRDDPartition(i, blockIds(i), isValid, segments(i))
    }
  }

  /**
   * Gets the partition data by getting the corresponding block from the block manager.
   * If the block does not exist, then the data is read from the corresponding segment
   * in write ahead log files.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    assertValid()
    val hadoopConf = broadcastedHadoopConf.value
    val blockManager = SparkEnv.get.blockManager
    val partition = split.asInstanceOf[WriteAheadLogBackedBlockRDDPartition]
    val blockId = partition.blockId
    val segment = partition.segment

    def getBlockFromBlockManager(): Option[Iterator[T]] = {
      blockManager.get(blockId).map(_.data.asInstanceOf[Iterator[T]])
    }

    def getBlockFromWriteAheadLog(): Iterator[T] = {
      val reader = new WriteAheadLogRandomReader(segment.path, hadoopConf)
      val dataRead = reader.read(segment)
      reader.close()
      logDebug(s"Read partition data of $this from write ahead log, segment ${partition.segment}")
      if (storeInBlockManager) {
        blockManager.putBytes(blockId, dataRead, storageLevel)
        logDebug(s"Stored partition data of $this into block manager with level $storageLevel")
        dataRead.rewind()
      }
      blockManager.dataDeserialize(blockId, dataRead).asInstanceOf[Iterator[T]]
    }

    if (partition.isBlockIdValid) {
      getBlockFromBlockManager().getOrElse { getBlockFromWriteAheadLog() }
    } else {
      getBlockFromWriteAheadLog()
    }
  }

  /**
   * Get the preferred location of the partition. This returns the locations of the block
   * if it is present in the block manager, else it returns the location of the
   * corresponding segment in HDFS.
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val partition = split.asInstanceOf[WriteAheadLogBackedBlockRDDPartition]
    val blockLocations = if (partition.isBlockIdValid) {
      getBlockIdLocations().get(partition.blockId)
    } else {
      None
    }

    blockLocations.getOrElse(
      HdfsUtils.getFileSegmentLocations(
        partition.segment.path, partition.segment.offset, partition.segment.length, hadoopConfig))
  }
}
