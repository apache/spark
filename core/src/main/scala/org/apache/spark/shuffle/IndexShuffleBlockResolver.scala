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

package org.apache.spark.shuffle

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExecutorDiskUtils
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    // var for testing
    var _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging with MigratableResolver {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")


  def getDataFile(shuffleId: Int, mapId: Long): File = getDataFile(shuffleId, mapId, None)

  /**
   * Get the shuffle files that are stored locally. Used for block migrations.
   */
  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = {
    val allBlocks = blockManager.diskBlockManager.getAllBlocks()
    allBlocks.flatMap {
      case ShuffleIndexBlockId(shuffleId, mapId, _) =>
        Some(ShuffleBlockInfo(shuffleId, mapId))
      case _ =>
        None
    }
  }

  /**
   * Get the shuffle data file.
   *
   * When the dirs parameter is None then use the disk manager's local directories. Otherwise,
   * read from the specified directories.
   */
   def getDataFile(shuffleId: Int, mapId: Long, dirs: Option[Array[String]]): File = {
    val blockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    dirs
      .map(ExecutorDiskUtils.getFile(_, blockManager.subDirsPerLocalDir, blockId.name))
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }

  /**
   * Get the shuffle index file.
   *
   * When the dirs parameter is None then use the disk manager's local directories. Otherwise,
   * read from the specified directories.
   */
  def getIndexFile(
      shuffleId: Int,
      mapId: Long,
      dirs: Option[Array[String]] = None): File = {
    val blockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    dirs
      .map(ExecutorDiskUtils.getFile(_, blockManager.subDirsPerLocalDir, blockId.name))
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // the index file should have `block + 1` longs as offset.
    if (index.length() != (blocks + 1) * 8L) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in = try {
      new DataInputStream(new NioBufferedFileInputStream(index))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file
    if (data.length() == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  /**
   * Write a provided shuffle block as a stream. Used for block migrations.
   * ShuffleBlockBatchIds must contain the full range represented in the ShuffleIndexBlock.
   * Requires the caller to delete any shuffle index blocks where the shuffle block fails to
   * put.
   */
  override def putShuffleBlockAsStream(blockId: BlockId, serializerManager: SerializerManager):
      StreamCallbackWithID = {
    val file = blockId match {
      case ShuffleIndexBlockId(shuffleId, mapId, _) =>
        getIndexFile(shuffleId, mapId)
      case ShuffleDataBlockId(shuffleId, mapId, _) =>
        getDataFile(shuffleId, mapId)
      case _ =>
        throw new IllegalStateException(s"Unexpected shuffle block transfer ${blockId} as " +
          s"${blockId.getClass().getSimpleName()}")
    }
    val fileTmp = Utils.tempFileWith(file)
    val channel = Channels.newChannel(
      serializerManager.wrapStream(blockId,
        new FileOutputStream(fileTmp)))

    new StreamCallbackWithID {

      override def getID: String = blockId.name

      override def onData(streamId: String, buf: ByteBuffer): Unit = {
        while (buf.hasRemaining) {
          channel.write(buf)
        }
      }

      override def onComplete(streamId: String): Unit = {
        logTrace(s"Done receiving shuffle block $blockId, now storing on local disk.")
        channel.close()
        val diskSize = fileTmp.length()
        this.synchronized {
          if (file.exists()) {
            file.delete()
          }
          if (!fileTmp.renameTo(file)) {
            throw new IOException(s"fail to rename file ${fileTmp} to ${file}")
          }
        }
        blockManager.reportBlockStatus(blockId, BlockStatus(StorageLevel.DISK_ONLY, 0, diskSize))
      }

      override def onFailure(streamId: String, cause: Throwable): Unit = {
        // the framework handles the connection itself, we just need to do local cleanup
        logWarning(s"Error while uploading $blockId", cause)
        channel.close()
        fileTmp.delete()
      }
    }
  }

  /**
   * Get the index & data block for migration.
   */
  def getMigrationBlocks(shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    try {
      val shuffleId = shuffleBlockInfo.shuffleId
      val mapId = shuffleBlockInfo.mapId
      // Load the index block
      val indexFile = getIndexFile(shuffleId, mapId)
      val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
      val indexFileSize = indexFile.length()
      val indexBlockData = new FileSegmentManagedBuffer(
        transportConf, indexFile, 0, indexFileSize)

      // Load the data block
      val dataFile = getDataFile(shuffleId, mapId)
      val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
      val dataBlockData = new FileSegmentManagedBuffer(
        transportConf, dataFile, 0, dataFile.length())

      // Make sure the index exist.
      if (!indexFile.exists()) {
        throw new FileNotFoundException("Index file is deleted already.")
      }
      if (dataFile.exists()) {
        List((dataBlockId, dataBlockData), (indexBlockId, indexBlockData))
      } else {
        List((indexBlockId, indexBlockData))
      }
    } catch {
      case _: Exception => // If we can't load the blocks ignore them.
        logWarning(s"Failed to resolve shuffle block ${shuffleBlockInfo}. " +
          "This is expected to occur if a block is removed after decommissioning has started.")
        List.empty[(BlockId, ManagedBuffer)]
    }
  }


  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Long,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = Utils.tempFileWith(indexFile)
    try {
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      this.synchronized {
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
          Utils.tryWithSafeFinally {
            // We take in lengths of each block, need to convert it to offsets.
            var offset = 0L
            out.writeLong(offset)
            for (length <- lengths) {
              offset += length
              out.writeLong(offset)
            }
          } {
            out.close()
          }

          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }
          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      logDebug(s"Shuffle index for mapId $mapId: ${lengths.mkString("[", ",", "]")}")
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
    }
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(shuffleId, mapId, dirs)

    // SPARK-22982: if this FileInputStream's position is seeked forward by another piece of code
    // which is incorrectly using our file descriptor then this code will fetch the wrong offsets
    // (which may cause a reducer to be sent a different reducer's data). The explicit position
    // checks added here were a useful debugging aid during SPARK-22982 and may help prevent this
    // class of issue from re-occurring in the future which is why they are left here even though
    // SPARK-22982 is fixed.
    val channel = Files.newByteChannel(indexFile.toPath)
    channel.position(startReduceId * 8L)
    val in = new DataInputStream(Channels.newInputStream(channel))
    try {
      val startOffset = in.readLong()
      channel.position(endReduceId * 8L)
      val endOffset = in.readLong()
      val actualPosition = channel.position()
      val expectedPosition = endReduceId * 8L + 8
      if (actualPosition != expectedPosition) {
        throw new Exception(s"SPARK-22982: Incorrect channel position after index file reads: " +
          s"expected $expectedPosition but actual position was $actualPosition.")
      }
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(shuffleId, mapId, dirs),
        startOffset,
        endOffset - startOffset)
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}
