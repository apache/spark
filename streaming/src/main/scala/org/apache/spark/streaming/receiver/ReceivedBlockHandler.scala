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

package org.apache.spark.streaming.receiver

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{EFFECTIVE_STORAGE_LEVEL, STORAGE_LEVEL, STORAGE_LEVEL_DESERIALIZED, STORAGE_LEVEL_REPLICATION}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage._
import org.apache.spark.streaming.receiver.WriteAheadLogBasedBlockHandler._
import org.apache.spark.streaming.util.{WriteAheadLogRecordHandle, WriteAheadLogUtils}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}
import org.apache.spark.util.io.ChunkedByteBuffer

/** Trait that represents the metadata related to storage of blocks */
private[streaming] trait ReceivedBlockStoreResult {
  // Any implementation of this trait will store a block id
  def blockId: StreamBlockId
  // Any implementation of this trait will have to return the number of records
  def numRecords: Option[Long]
}

/** Trait that represents a class that handles the storage of blocks received by receiver */
private[streaming] trait ReceivedBlockHandler {

  /** Store a received block with the given block id and return related metadata */
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): ReceivedBlockStoreResult

  /** Cleanup old blocks older than the given threshold time */
  def cleanupOldBlocks(threshTime: Long): Unit
}


/**
 * Implementation of [[org.apache.spark.streaming.receiver.ReceivedBlockStoreResult]]
 * that stores the metadata related to storage of blocks using
 * [[org.apache.spark.streaming.receiver.BlockManagerBasedBlockHandler]]
 */
private[streaming] case class BlockManagerBasedStoreResult(
      blockId: StreamBlockId, numRecords: Option[Long])
  extends ReceivedBlockStoreResult


/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks into a block manager with the specified storage level.
 */
private[streaming] class BlockManagerBasedBlockHandler(
    blockManager: BlockManager, storageLevel: StorageLevel)
  extends ReceivedBlockHandler with Logging {

  def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {

    var numRecords: Option[Long] = None

    val putSucceeded: Boolean = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        numRecords = Some(arrayBuffer.size.toLong)
        blockManager.putIterator(blockId, arrayBuffer.iterator, storageLevel,
          tellMaster = true)
      case IteratorBlock(iterator) =>
        val countIterator = new CountingIterator(iterator)
        val putResult = blockManager.putIterator(blockId, countIterator, storageLevel,
          tellMaster = true)
        numRecords = countIterator.count()
        putResult
      case ByteBufferBlock(byteBuffer) =>
        blockManager.putBytes(
          blockId, new ChunkedByteBuffer(byteBuffer.duplicate()), storageLevel, tellMaster = true)
      case o =>
        throw new SparkException(
          s"Could not store $blockId to block manager, unexpected block type ${o.getClass.getName}")
    }
    if (!putSucceeded) {
      throw new SparkException(
        s"Could not store $blockId to block manager with storage level $storageLevel")
    }
    BlockManagerBasedStoreResult(blockId, numRecords)
  }

  def cleanupOldBlocks(threshTime: Long): Unit = {
    // this is not used as blocks inserted into the BlockManager are cleared by DStream's clearing
    // of BlockRDDs.
  }
}


/**
 * Implementation of [[org.apache.spark.streaming.receiver.ReceivedBlockStoreResult]]
 * that stores the metadata related to storage of blocks using
 * [[org.apache.spark.streaming.receiver.WriteAheadLogBasedBlockHandler]]
 */
private[streaming] case class WriteAheadLogBasedStoreResult(
    blockId: StreamBlockId,
    numRecords: Option[Long],
    walRecordHandle: WriteAheadLogRecordHandle
  ) extends ReceivedBlockStoreResult


/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks in both, a write ahead log and a block manager.
 */
private[streaming] class WriteAheadLogBasedBlockHandler(
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    streamId: Int,
    storageLevel: StorageLevel,
    conf: SparkConf,
    hadoopConf: Configuration,
    checkpointDir: String,
    clock: Clock = new SystemClock
  ) extends ReceivedBlockHandler with Logging {

  private val blockStoreTimeout = conf.getInt(
    "spark.streaming.receiver.blockStoreTimeout", 30).seconds

  private val effectiveStorageLevel = {
    if (storageLevel.deserialized) {
      logWarning(log"Storage level serialization " +
        log"${MDC(STORAGE_LEVEL_DESERIALIZED, storageLevel.deserialized)} is not " +
        log"supported when write ahead log is enabled, change to serialization false")
    }
    if (storageLevel.replication > 1) {
      logWarning(log"Storage level replication " +
        log"${MDC(STORAGE_LEVEL_REPLICATION, storageLevel.replication)} is unnecessary when " +
        log"write ahead log is enabled, change to replication 1")
    }

    StorageLevel(storageLevel.useDisk, storageLevel.useMemory, storageLevel.useOffHeap, false, 1)
  }

  if (storageLevel != effectiveStorageLevel) {
    logWarning(log"User defined storage level ${MDC(STORAGE_LEVEL, storageLevel)} is changed to " +
      log"effective storage level ${MDC(EFFECTIVE_STORAGE_LEVEL, effectiveStorageLevel)} when " +
      log"write ahead log is enabled")
  }

  // Write ahead log manages
  private val writeAheadLog = WriteAheadLogUtils.createLogForReceiver(
    conf, checkpointDirToLogDir(checkpointDir, streamId), hadoopConf)

  // For processing futures used in parallel block storing into block manager and write ahead log
  // # threads = 2, so that both writing to BM and WAL can proceed in parallel
  implicit private val executionContext: ExecutionContextExecutorService = ExecutionContext
    .fromExecutorService(ThreadUtils.newDaemonFixedThreadPool(2, this.getClass.getSimpleName))

  /**
   * This implementation stores the block into the block manager as well as a write ahead log.
   * It does this in parallel, using Scala Futures, and returns only after the block has
   * been stored in both places.
   */
  def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {

    var numRecords = Option.empty[Long]
    // Serialize the block so that it can be inserted into both
    val serializedBlock = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        numRecords = Some(arrayBuffer.size.toLong)
        serializerManager.dataSerialize(blockId, arrayBuffer.iterator)
      case IteratorBlock(iterator) =>
        val countIterator = new CountingIterator(iterator)
        val serializedBlock = serializerManager.dataSerialize(blockId, countIterator)
        numRecords = countIterator.count()
        serializedBlock
      case ByteBufferBlock(byteBuffer) =>
        new ChunkedByteBuffer(byteBuffer.duplicate())
      case _ =>
        throw new Exception(s"Could not push $blockId to block manager, unexpected block type")
    }

    // Store the block in block manager
    val storeInBlockManagerFuture = Future {
      val putSucceeded = blockManager.putBytes(
        blockId,
        serializedBlock,
        effectiveStorageLevel,
        tellMaster = true)
      if (!putSucceeded) {
        throw new SparkException(
          s"Could not store $blockId to block manager with storage level $storageLevel")
      }
    }

    // Store the block in write ahead log
    val storeInWriteAheadLogFuture = Future {
      writeAheadLog.write(serializedBlock.toByteBuffer, clock.getTimeMillis())
    }

    // Combine the futures, wait for both to complete, and return the write ahead log record handle
    val combinedFuture = storeInBlockManagerFuture.zip(storeInWriteAheadLogFuture).map(_._2)
    val walRecordHandle = ThreadUtils.awaitResult(combinedFuture, blockStoreTimeout)
    WriteAheadLogBasedStoreResult(blockId, numRecords, walRecordHandle)
  }

  def cleanupOldBlocks(threshTime: Long): Unit = {
    writeAheadLog.clean(threshTime, false)
  }

  def stop(): Unit = {
    writeAheadLog.close()
    executionContext.shutdown()
  }
}

private[streaming] object WriteAheadLogBasedBlockHandler {
  def checkpointDirToLogDir(checkpointDir: String, streamId: Int): String = {
    new Path(checkpointDir, new Path("receivedData", streamId.toString)).toString
  }
}

/**
 * A utility that will wrap the Iterator to get the count
 */
private[streaming] class CountingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
   private var _count = 0

   private def isFullyConsumed: Boolean = !iterator.hasNext

   def hasNext: Boolean = iterator.hasNext

   def count(): Option[Long] = {
     if (isFullyConsumed) Some(_count) else None
   }

   def next(): T = {
    _count += 1
    iterator.next()
   }
}
