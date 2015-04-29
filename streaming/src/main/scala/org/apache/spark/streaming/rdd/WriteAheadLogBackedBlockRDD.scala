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

import java.nio.ByteBuffer

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.commons.io.FileUtils

import org.apache.spark._
import org.apache.spark.rdd.BlockRDD
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.util._

/**
 * Partition class for [[org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD]].
 * It contains information about the id of the blocks having this partition's data and
 * the segment of the write ahead log that backs the partition.
 * @param index index of the partition
 * @param blockId id of the block having the partition data
 * @param walRecordHandle Handle of the record in a write ahead log having the partition data
 */
private[streaming]
class WriteAheadLogBackedBlockRDDPartition(
    val index: Int,
    val blockId: BlockId,
    val walRecordHandle: WriteAheadLogRecordHandle)
  extends Partition


/**
 * This class represents a special case of the BlockRDD where the data blocks in
 * the block manager are also backed by data in write ahead logs. For reading
 * the data, this RDD first looks up the blocks by their ids in the block manager.
 * If it does not find them, it looks up the corresponding data in the write ahead log.
 *
 * @param sc SparkContext
 * @param blockIds Ids of the blocks that contains this RDD's data
 * @param walRecordHandles Record handles in write ahead logs that contain this RDD's data
 * @param storeInBlockManager Whether to store in the block manager after reading
 *                            from the WAL record
 * @param storageLevel storage level to store when storing in block manager
 *                     (applicable when storeInBlockManager = true)
 */
private[streaming]
class WriteAheadLogBackedBlockRDD[T: ClassTag](
    @transient sc: SparkContext,
    @transient blockIds: Array[BlockId],
    @transient walRecordHandles: Array[WriteAheadLogRecordHandle],
    storeInBlockManager: Boolean,
    storageLevel: StorageLevel)
  extends BlockRDD[T](sc, blockIds) {

  require(
    blockIds.length == walRecordHandles.length,
    s"Number of block ids (${blockIds.length}) must be " +
      s"the same as number of WAL record handles (${walRecordHandles.length}})!")

  // Hadoop configuration is not serializable, so broadcast it as a serializable.
  @transient private val hadoopConfig = sc.hadoopConfiguration
  private val broadcastedHadoopConf = new SerializableWritable(hadoopConfig)

  override def getPartitions: Array[Partition] = {
    assertValid()
    Array.tabulate(blockIds.size) { i =>
      new WriteAheadLogBackedBlockRDDPartition(i, blockIds(i), walRecordHandles(i))
    }
  }

  /**
   * Gets the partition data by getting the corresponding block from the block manager.
   * If the block does not exist, then the data is read from the corresponding record
   * in write ahead log files.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    assertValid()
    val hadoopConf = broadcastedHadoopConf.value
    val blockManager = SparkEnv.get.blockManager
    val partition = split.asInstanceOf[WriteAheadLogBackedBlockRDDPartition]
    val blockId = partition.blockId
    blockManager.get(blockId) match {
      case Some(block) => // Data is in Block Manager
        val iterator = block.data.asInstanceOf[Iterator[T]]
        logDebug(s"Read partition data of $this from block manager, block $blockId")
        iterator
      case None => // Data not found in Block Manager, grab it from write ahead log file
        var dataRead: ByteBuffer = null
        var writeAheadLog: WriteAheadLog = null
        try {
          // The WriteAheadLogUtils.createLog*** method needs a directory to create a
          // WriteAheadLog object as the default FileBasedWriteAheadLog needs a directory for
          // writing log data. However, the directory is not needed if data needs to be read, hence
          // a dummy path is provided to satisfy the method parameter requirements.
          // FileBasedWriteAheadLog will not create any file or directory at that path.
          val dummyDirectory = FileUtils.getTempDirectoryPath()
          writeAheadLog = WriteAheadLogUtils.createLogForReceiver(
            SparkEnv.get.conf, dummyDirectory, hadoopConf)
          dataRead = writeAheadLog.read(partition.walRecordHandle)
        } catch {
          case NonFatal(e) =>
            throw new SparkException(
              s"Could not read data from write ahead log record ${partition.walRecordHandle}", e)
        } finally {
          if (writeAheadLog != null) {
            writeAheadLog.close()
            writeAheadLog = null
          }
        }
        if (dataRead == null) {
          throw new SparkException(
            s"Could not read data from write ahead log record ${partition.walRecordHandle}, " +
              s"read returned null")
        }
        logInfo(s"Read partition data of $this from write ahead log, record handle " +
          partition.walRecordHandle)
        if (storeInBlockManager) {
          blockManager.putBytes(blockId, dataRead, storageLevel)
          logDebug(s"Stored partition data of $this into block manager with level $storageLevel")
          dataRead.rewind()
        }
        blockManager.dataDeserialize(blockId, dataRead).asInstanceOf[Iterator[T]]
    }
  }

  /**
   * Get the preferred location of the partition. This returns the locations of the block
   * if it is present in the block manager, else if FileBasedWriteAheadLogSegment is used,
   * it returns the location of the corresponding file segment in HDFS .
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val partition = split.asInstanceOf[WriteAheadLogBackedBlockRDDPartition]
    val blockLocations = getBlockIdLocations().get(partition.blockId)
    blockLocations.getOrElse {
      partition.walRecordHandle match {
        case fileSegment: FileBasedWriteAheadLogSegment =>
          HdfsUtils.getFileSegmentLocations(
            fileSegment.path, fileSegment.offset, fileSegment.length, hadoopConfig)
        case _ =>
          Seq.empty
      }
    }
  }
}
