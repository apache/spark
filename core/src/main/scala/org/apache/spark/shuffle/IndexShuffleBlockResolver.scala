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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.{ExecutorDiskUtils, MergedBlockMeta}
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
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

  private val remoteShuffleMaxDisk: Option[Long] =
    conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_MAX_DISK_SIZE)

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

  private def getShuffleBytesStored(): Long = {
    val shuffleFiles: Seq[File] = getStoredShuffles().map {
      si => getDataFile(si.shuffleId, si.mapId)
    }
    shuffleFiles.map(_.length()).sum
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
      .map(d =>
        new File(ExecutorDiskUtils.getFilePath(d, blockManager.subDirsPerLocalDir, blockId.name)))
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
      .map(d =>
        new File(ExecutorDiskUtils.getFilePath(d, blockManager.subDirsPerLocalDir, blockId.name)))
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }

  private def getMergedBlockDataFile(
      appId: String,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      dirs: Option[Array[String]] = None): File = {
    blockManager.diskBlockManager.getMergedShuffleFile(
      ShuffleMergedDataBlockId(appId, shuffleId, shuffleMergeId, reduceId), dirs)
  }

  private def getMergedBlockIndexFile(
      appId: String,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      dirs: Option[Array[String]] = None): File = {
    blockManager.diskBlockManager.getMergedShuffleFile(
      ShuffleMergedIndexBlockId(appId, shuffleId, shuffleMergeId, reduceId), dirs)
  }

  private def getMergedBlockMetaFile(
      appId: String,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      dirs: Option[Array[String]] = None): File = {
    blockManager.diskBlockManager.getMergedShuffleFile(
      ShuffleMergedMetaBlockId(appId, shuffleId, shuffleMergeId, reduceId), dirs)
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists() && !file.delete()) {
      logWarning(s"Error deleting data ${file.getPath()}")
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists() && !file.delete()) {
      logWarning(s"Error deleting index ${file.getPath()}")
    }

    file = getChecksumFile(shuffleId, mapId, conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM))
    if (file.exists() && !file.delete()) {
      logWarning(s"Error deleting checksum ${file.getPath()}")
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
    // Throw an exception if we have exceeded maximum shuffle files stored
    remoteShuffleMaxDisk.foreach { maxBytes =>
      val bytesUsed = getShuffleBytesStored()
      if (maxBytes < bytesUsed) {
        throw new SparkException(s"Not storing remote shuffles $bytesUsed exceeds $maxBytes")
      }
    }
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
   * Commit the data and metadata files as an atomic operation, use the existing ones, or
   * replace them with new ones. Note that the metadata parameters (`lengths`, `checksums`)
   * will be updated to match the existing ones if use the existing ones.
   *
   * There're two kinds of metadata files:
   *
   * - index file
   * An index file contains the offsets of each block, plus a final offset at the end
   * for the end of the output file. It will be used by [[getBlockData]] to figure out
   * where each block begins and ends.
   *
   * - checksum file (optional)
   * An checksum file contains the checksum of each block. It will be used to diagnose
   * the cause when a block is corrupted. Note that empty `checksums` indicate that
   * checksum is disabled.
   */
  def writeMetadataFileAndCommit(
      shuffleId: Int,
      mapId: Long,
      lengths: Array[Long],
      checksums: Array[Long],
      dataTmp: File): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = Utils.tempFileWith(indexFile)

    val checksumEnabled = checksums.nonEmpty
    val (checksumFileOpt, checksumTmpOpt) = if (checksumEnabled) {
      assert(lengths.length == checksums.length,
        "The size of partition lengths and checksums should be equal")
      val checksumFile =
        getChecksumFile(shuffleId, mapId, conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM))
      (Some(checksumFile), Some(Utils.tempFileWith(checksumFile)))
    } else {
      (None, None)
    }

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
          if (checksumEnabled) {
            val existingChecksums = getChecksums(checksumFileOpt.get, checksums.length)
            if (existingChecksums != null) {
              System.arraycopy(existingChecksums, 0, checksums, 0, lengths.length)
            } else {
              // It's possible that the previous task attempt succeeded writing the
              // index file and data file but failed to write the checksum file. In
              // this case, the current task attempt could write the missing checksum
              // file by itself.
              writeMetadataFile(checksums, checksumTmpOpt.get, checksumFileOpt.get, false)
            }
          }
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.

          val offsets = lengths.scanLeft(0L)(_ + _)
          writeMetadataFile(offsets, indexTmp, indexFile, true)

          if (dataFile.exists()) {
            dataFile.delete()
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }

          // write the checksum file
          checksumTmpOpt.zip(checksumFileOpt).foreach { case (checksumTmp, checksumFile) =>
            try {
              writeMetadataFile(checksums, checksumTmp, checksumFile, false)
            } catch {
              case e: Exception =>
                // It's not worthwhile to fail here after index file and data file are
                // already successfully stored since checksum is only a best-effort for
                // the corner error case.
                logError("Failed to write checksum file", e)
            }
          }
        }
      }
    } finally {
      logDebug(s"Shuffle index for mapId $mapId: ${lengths.mkString("[", ",", "]")}")
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
      checksumTmpOpt.foreach { checksumTmp =>
        if (checksumTmp.exists()) {
          try {
            if (!checksumTmp.delete()) {
              logError(s"Failed to delete temporary checksum file " +
                s"at ${checksumTmp.getAbsolutePath}")
            }
          } catch {
            case e: Exception =>
              // Unlike index deletion, we won't propagate the error for the checksum file since
              // checksum is only a best-effort.
              logError(s"Failed to delete temporary checksum file " +
                s"at ${checksumTmp.getAbsolutePath}", e)
          }
        }
      }
    }
  }

  /**
   * Write the metadata file (index or checksum). Metadata values will be firstly write into
   * the tmp file and the tmp file will be renamed to the target file at the end to avoid dirty
   * writes.
   * @param metaValues The metadata values
   * @param tmpFile The temp file
   * @param targetFile The target file
   * @param propagateError Whether to propagate the error for file operation. Unlike index file,
   *                       checksum is only a best-effort so we won't fail the whole task due to
   *                       the error from checksum.
   */
  private def writeMetadataFile(
      metaValues: Array[Long],
      tmpFile: File,
      targetFile: File,
      propagateError: Boolean): Unit = {
    val out = new DataOutputStream(
      new BufferedOutputStream(
        new FileOutputStream(tmpFile)
      )
    )
    Utils.tryWithSafeFinally {
      metaValues.foreach(out.writeLong)
    } {
      out.close()
    }

    if (targetFile.exists()) {
      targetFile.delete()
    }

    if (!tmpFile.renameTo(targetFile)) {
      val errorMsg = s"fail to rename file $tmpFile to $targetFile"
      if (propagateError) {
        throw new IOException(errorMsg)
      } else {
        logWarning(errorMsg)
      }
    }
  }

  /**
   * This is only used for reading local merged block data. In such cases, all chunks in the
   * merged shuffle file need to be identified at once, so the ShuffleBlockFetcherIterator
   * knows how to consume local merged shuffle file as multiple chunks.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    val indexFile =
      getMergedBlockIndexFile(conf.getAppId, blockId.shuffleId, blockId.shuffleMergeId,
        blockId.reduceId, dirs)
    val dataFile = getMergedBlockDataFile(conf.getAppId, blockId.shuffleId,
      blockId.shuffleMergeId, blockId.reduceId, dirs)
    // Load all the indexes in order to identify all chunks in the specified merged shuffle file.
    val size = indexFile.length.toInt
    val offsets = Utils.tryWithResource {
      new DataInputStream(Files.newInputStream(indexFile.toPath))
    } { dis =>
      val buffer = ByteBuffer.allocate(size)
      dis.readFully(buffer.array)
      buffer.asLongBuffer
    }
    // Number of chunks is number of indexes - 1
    val numChunks = size / 8 - 1
    for (index <- 0 until numChunks) yield {
      new FileSegmentManagedBuffer(transportConf, dataFile,
        offsets.get(index),
        offsets.get(index + 1) - offsets.get(index))
    }
  }

  /**
   * This is only used for reading local merged block meta data.
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    val indexFile =
      getMergedBlockIndexFile(conf.getAppId, blockId.shuffleId,
        blockId.shuffleMergeId, blockId.reduceId, dirs)
    val size = indexFile.length.toInt
    val numChunks = (size / 8) - 1
    val metaFile = getMergedBlockMetaFile(conf.getAppId, blockId.shuffleId,
      blockId.shuffleMergeId, blockId.reduceId, dirs)
    val chunkBitMaps = new FileSegmentManagedBuffer(transportConf, metaFile, 0L, metaFile.length)
    new MergedBlockMeta(numChunks, chunkBitMaps)
  }

  private[shuffle] def getChecksums(checksumFile: File, blockNum: Int): Array[Long] = {
    if (!checksumFile.exists()) return null
    val checksums = new ArrayBuffer[Long]
    // Read the checksums of blocks
    var in: DataInputStream = null
    try {
      in = new DataInputStream(new NioBufferedFileInputStream(checksumFile))
      while (checksums.size < blockNum) {
        checksums += in.readLong()
      }
    } catch {
      case _: IOException | _: EOFException =>
        return null
    } finally {
      in.close()
    }

    checksums.toArray
  }

  /**
   * Get the shuffle checksum file.
   *
   * When the dirs parameter is None then use the disk manager's local directories. Otherwise,
   * read from the specified directories.
   */
  def getChecksumFile(
      shuffleId: Int,
      mapId: Long,
      algorithm: String,
      dirs: Option[Array[String]] = None): File = {
    val blockId = ShuffleChecksumBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    val fileName = ShuffleChecksumHelper.getChecksumFileName(blockId.name, algorithm)
    dirs
      .map(d =>
        new File(ExecutorDiskUtils.getFilePath(d, blockManager.subDirsPerLocalDir, fileName)))
      .getOrElse(blockManager.diskBlockManager.getFile(fileName))
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
