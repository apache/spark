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

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.{existentials, postfixOps}

import WriteAheadLogBasedBlockHandler._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.storage._
import org.apache.spark.streaming.util.{Clock, SystemClock, WriteAheadLogFileSegment, WriteAheadLogManager}
import org.apache.spark.util.Utils

/** Trait that represents the metadata related to storage of blocks */
private[streaming] trait ReceivedBlockStoreResult {
  def blockId: StreamBlockId  // Any implementation of this trait will store a block id
}

/** Trait that represents a class that handles the storage of blocks received by receiver */
private[streaming] trait ReceivedBlockHandler {

  /** Store a received block with the given block id and return related metadata */
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): ReceivedBlockStoreResult

  /** Cleanup old blocks older than the given threshold time */
  def cleanupOldBlock(threshTime: Long)
}


/**
 * Implementation of [[org.apache.spark.streaming.receiver.ReceivedBlockStoreResult]]
 * that stores the metadata related to storage of blocks using
 * [[org.apache.spark.streaming.receiver.BlockManagerBasedBlockHandler]]
 */
private[streaming] case class BlockManagerBasedStoreResult(blockId: StreamBlockId)
  extends ReceivedBlockStoreResult


/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks into a block manager with the specified storage level.
 */
private[streaming] class BlockManagerBasedBlockHandler(
    blockManager: BlockManager, storageLevel: StorageLevel)
  extends ReceivedBlockHandler with Logging {
  
  def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {
    val putResult: Seq[(BlockId, BlockStatus)] = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        blockManager.putIterator(blockId, arrayBuffer.iterator, storageLevel, tellMaster = true)
      case IteratorBlock(iterator) =>
        blockManager.putIterator(blockId, iterator, storageLevel, tellMaster = true)
      case ByteBufferBlock(byteBuffer) =>
        blockManager.putBytes(blockId, byteBuffer, storageLevel, tellMaster = true)
      case o =>
        throw new SparkException(
          s"Could not store $blockId to block manager, unexpected block type ${o.getClass.getName}")
    }
    if (!putResult.map { _._1 }.contains(blockId)) {
      throw new SparkException(
        s"Could not store $blockId to block manager with storage level $storageLevel")
    }
    BlockManagerBasedStoreResult(blockId)
  }

  def cleanupOldBlock(threshTime: Long) {
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
    segment: WriteAheadLogFileSegment
  ) extends ReceivedBlockStoreResult


/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks in both, a write ahead log and a block manager.
 */
private[streaming] class WriteAheadLogBasedBlockHandler(
    blockManager: BlockManager,
    streamId: Int,
    storageLevel: StorageLevel,
    conf: SparkConf,
    hadoopConf: Configuration,
    checkpointDir: String,
    clock: Clock = new SystemClock
  ) extends ReceivedBlockHandler with Logging {

  private val blockStoreTimeout = conf.getInt(
    "spark.streaming.receiver.blockStoreTimeout", 30).seconds
  private val rollingInterval = conf.getInt(
    "spark.streaming.receiver.writeAheadLog.rollingInterval", 60)
  private val maxFailures = conf.getInt(
    "spark.streaming.receiver.writeAheadLog.maxFailures", 3)

  // Manages rolling log files
  private val logManager = new WriteAheadLogManager(
    checkpointDirToLogDir(checkpointDir, streamId),
    hadoopConf, rollingInterval, maxFailures,
    callerName = this.getClass.getSimpleName,
    clock = clock
  )

  // For processing futures used in parallel block storing into block manager and write ahead log
  // # threads = 2, so that both writing to BM and WAL can proceed in parallel
  implicit private val executionContext = ExecutionContext.fromExecutorService(
    Utils.newDaemonFixedThreadPool(2, this.getClass.getSimpleName))

  /**
   * This implementation stores the block into the block manager as well as a write ahead log.
   * It does this in parallel, using Scala Futures, and returns only after the block has
   * been stored in both places.
   */
  def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {

    // Serialize the block so that it can be inserted into both
    val serializedBlock = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        blockManager.dataSerialize(blockId, arrayBuffer.iterator)
      case IteratorBlock(iterator) =>
        blockManager.dataSerialize(blockId, iterator)
      case ByteBufferBlock(byteBuffer) =>
        byteBuffer
      case _ =>
        throw new Exception(s"Could not push $blockId to block manager, unexpected block type")
    }

    // Store the block in block manager
    val storeInBlockManagerFuture = Future {
      val putResult =
        blockManager.putBytes(blockId, serializedBlock, storageLevel, tellMaster = true)
      if (!putResult.map { _._1 }.contains(blockId)) {
        throw new SparkException(
          s"Could not store $blockId to block manager with storage level $storageLevel")
      }
    }

    // Store the block in write ahead log
    val storeInWriteAheadLogFuture = Future {
      logManager.writeToLog(serializedBlock)
    }

    // Combine the futures, wait for both to complete, and return the write ahead log segment
    val combinedFuture = for {
      _ <- storeInBlockManagerFuture
      fileSegment <- storeInWriteAheadLogFuture
    } yield fileSegment
    val segment = Await.result(combinedFuture, blockStoreTimeout)
    WriteAheadLogBasedStoreResult(blockId, segment)
  }

  def cleanupOldBlock(threshTime: Long) {
    logManager.cleanupOldLogs(threshTime)
  }

  def stop() {
    logManager.stop()
  }
}

private[streaming] object WriteAheadLogBasedBlockHandler {
  def checkpointDirToLogDir(checkpointDir: String, streamId: Int): String = {
    new Path(checkpointDir, new Path("receivedData", streamId.toString)).toString
  }
}
