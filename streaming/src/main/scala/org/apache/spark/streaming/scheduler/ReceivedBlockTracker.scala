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

package org.apache.spark.streaming.scheduler

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.util.{WriteAheadLog, WriteAheadLogUtils}
import org.apache.spark.util.{Clock, Utils}

/** Trait representing any event in the ReceivedBlockTracker that updates its state. */
private[streaming] sealed trait ReceivedBlockTrackerLogEvent

private[streaming] case class BlockAdditionEvent(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchAllocationEvent(time: Time, allocatedBlocks: AllocatedBlocks)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchCleanupEvent(times: Seq[Time])
  extends ReceivedBlockTrackerLogEvent

/** Class representing the blocks of all the streams allocated to a batch */
private[streaming]
case class AllocatedBlocks(streamIdToAllocatedBlocks: Map[Int, Seq[ReceivedBlockInfo]]) {
  def getBlocksOfStream(streamId: Int): Seq[ReceivedBlockInfo] = {
    streamIdToAllocatedBlocks.getOrElse(streamId, Seq.empty)
  }
}

/**
 * Class that keep track of all the received blocks, and allocate them to batches
 * when required. All actions taken by this class can be saved to a write ahead log
 * (if a checkpoint directory has been provided), so that the state of the tracker
 * (received blocks and block-to-batch allocations) can be recovered after driver failure.
 *
 * Note that when any instance of this class is created with a checkpoint directory,
 * it will try reading events from logs in the directory.
 */
private[streaming] class ReceivedBlockTracker(
    conf: SparkConf,
    hadoopConf: Configuration,
    streamIds: Seq[Int],
    clock: Clock,
    recoverFromWriteAheadLog: Boolean,
    checkpointDirOption: Option[String])
  extends Logging {

  private type ReceivedBlockQueue = mutable.Queue[ReceivedBlockInfo]

  private val streamIdToUnallocatedBlockQueues = new mutable.HashMap[Int, ReceivedBlockQueue]
  private val timeToAllocatedBlocks = new mutable.HashMap[Time, AllocatedBlocks]
  private val writeAheadLogOption = createWriteAheadLog()

  private var lastAllocatedBatchTime: Time = null

  // Recover block information from write ahead logs
  if (recoverFromWriteAheadLog) {
    recoverPastEvents()
  }

  /** Add received block. This event will get written to the write ahead log (if enabled). */
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    try {
      val writeResult = writeToLog(BlockAdditionEvent(receivedBlockInfo))
      if (writeResult) {
        synchronized {
          getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
        }
        logDebug(s"Stream ${receivedBlockInfo.streamId} received " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId}")
      } else {
        logDebug(s"Failed to acknowledge stream ${receivedBlockInfo.streamId} receiving " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId} in the Write Ahead Log.")
      }
      writeResult
    } catch {
      case NonFatal(e) =>
        logError(s"Error adding block $receivedBlockInfo", e)
        false
    }
  }

  /**
   * Allocate all unallocated blocks to the given batch.
   * This event will get written to the write ahead log (if enabled).
   */
  def allocateBlocksToBatch(batchTime: Time): Unit = synchronized {
    if (lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {
      // We explicitly create an ArrayBuffer here because at least as of Scala 2.11 and 2.12
      // a mutable.Queue fails serialization with a StackOverflow error if it has more than
      // a few thousand elements.  So we explicitly allocate a collection for serialization which
      // we know doesn't have this issue.  (See SPARK-26734).
      val streamIdToBlocks = streamIds.map { streamId =>
        val blocks = mutable.ArrayBuffer[ReceivedBlockInfo]()
        blocks ++= getReceivedBlockQueue(streamId).clone()
        (streamId, blocks.toSeq)
      }.toMap
      val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)
      if (writeToLog(BatchAllocationEvent(batchTime, allocatedBlocks))) {
        streamIds.foreach(getReceivedBlockQueue(_).clear())
        timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
        lastAllocatedBatchTime = batchTime
      } else {
        logInfo(s"Possibly processed batch $batchTime needs to be processed again in WAL recovery")
      }
    } else {
      // This situation occurs when:
      // 1. WAL is ended with BatchAllocationEvent, but without BatchCleanupEvent,
      // possibly processed batch job or half-processed batch job need to be processed again,
      // so the batchTime will be equal to lastAllocatedBatchTime.
      // 2. Slow checkpointing makes recovered batch time older than WAL recovered
      // lastAllocatedBatchTime.
      // This situation will only occurs in recovery time.
      logInfo(s"Possibly processed batch $batchTime needs to be processed again in WAL recovery")
    }
  }

  /** Get the blocks allocated to the given batch. */
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = synchronized {
    timeToAllocatedBlocks.get(batchTime).map { _.streamIdToAllocatedBlocks }.getOrElse(Map.empty)
  }

  /** Get the blocks allocated to the given batch and stream. */
  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {
    synchronized {
      timeToAllocatedBlocks.get(batchTime).map {
        _.getBlocksOfStream(streamId)
      }.getOrElse(Seq.empty)
    }
  }

  /** Check if any blocks are left to be allocated to batches. */
  def hasUnallocatedReceivedBlocks: Boolean = synchronized {
    !streamIdToUnallocatedBlockQueues.values.forall(_.isEmpty)
  }

  /**
   * Get blocks that have been added but not yet allocated to any batch. This method
   * is primarily used for testing.
   */
  def getUnallocatedBlocks(streamId: Int): Seq[ReceivedBlockInfo] = synchronized {
    getReceivedBlockQueue(streamId).toSeq
  }

  /**
   * Clean up block information of old batches. If waitForCompletion is true, this method
   * returns only after the files are cleaned up.
   */
  def cleanupOldBatches(cleanupThreshTime: Time, waitForCompletion: Boolean): Unit = synchronized {
    require(cleanupThreshTime.milliseconds < clock.getTimeMillis())
    val timesToCleanup = timeToAllocatedBlocks.keys.filter { _ < cleanupThreshTime }.toSeq
    logInfo(s"Deleting batches: ${timesToCleanup.mkString(" ")}")
    if (writeToLog(BatchCleanupEvent(timesToCleanup))) {
      timeToAllocatedBlocks --= timesToCleanup
      writeAheadLogOption.foreach(_.clean(cleanupThreshTime.milliseconds, waitForCompletion))
    } else {
      logWarning("Failed to acknowledge batch clean up in the Write Ahead Log.")
    }
  }

  /** Stop the block tracker. */
  def stop(): Unit = {
    writeAheadLogOption.foreach { _.close() }
  }

  /**
   * Recover all the tracker actions from the write ahead logs to recover the state (unallocated
   * and allocated block info) prior to failure.
   */
  private def recoverPastEvents(): Unit = synchronized {
    // Insert the recovered block information
    def insertAddedBlock(receivedBlockInfo: ReceivedBlockInfo): Unit = {
      logTrace(s"Recovery: Inserting added block $receivedBlockInfo")
      receivedBlockInfo.setBlockIdInvalid()
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
    }

    // Insert the recovered block-to-batch allocations and removes them from queue of
    // received blocks.
    def insertAllocatedBatch(batchTime: Time, allocatedBlocks: AllocatedBlocks): Unit = {
      logTrace(s"Recovery: Inserting allocated batch for time $batchTime to " +
        s"${allocatedBlocks.streamIdToAllocatedBlocks}")
      allocatedBlocks.streamIdToAllocatedBlocks.foreach {
        case (streamId, allocatedBlocksInStream) =>
          getReceivedBlockQueue(streamId).dequeueAll(allocatedBlocksInStream.toSet)
      }
      timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
      lastAllocatedBatchTime = batchTime
    }

    // Cleanup the batch allocations
    def cleanupBatches(batchTimes: Seq[Time]): Unit = {
      logTrace(s"Recovery: Cleaning up batches $batchTimes")
      timeToAllocatedBlocks --= batchTimes
    }

    writeAheadLogOption.foreach { writeAheadLog =>
      logInfo(s"Recovering from write ahead logs in ${checkpointDirOption.get}")
      writeAheadLog.readAll().asScala.foreach { byteBuffer =>
        logInfo("Recovering record " + byteBuffer)
        Utils.deserialize[ReceivedBlockTrackerLogEvent](
          JavaUtils.bufferToArray(byteBuffer), Thread.currentThread().getContextClassLoader) match {
          case BlockAdditionEvent(receivedBlockInfo) =>
            insertAddedBlock(receivedBlockInfo)
          case BatchAllocationEvent(time, allocatedBlocks) =>
            insertAllocatedBatch(time, allocatedBlocks)
          case BatchCleanupEvent(batchTimes) =>
            cleanupBatches(batchTimes)
        }
      }
    }
  }

  /** Write an update to the tracker to the write ahead log */
  private[streaming] def writeToLog(record: ReceivedBlockTrackerLogEvent): Boolean = {
    if (isWriteAheadLogEnabled) {
      logTrace(s"Writing record: $record")
      try {
        writeAheadLogOption.get.write(ByteBuffer.wrap(Utils.serialize(record)),
          clock.getTimeMillis())
        true
      } catch {
        case NonFatal(e) =>
          logWarning(s"Exception thrown while writing record: $record to the WriteAheadLog.", e)
          false
      }
    } else {
      true
    }
  }

  /** Get the queue of received blocks belonging to a particular stream */
  private def getReceivedBlockQueue(streamId: Int): ReceivedBlockQueue = {
    streamIdToUnallocatedBlockQueues.getOrElseUpdate(streamId, new ReceivedBlockQueue)
  }

  /** Optionally create the write ahead log manager only if the feature is enabled */
  private def createWriteAheadLog(): Option[WriteAheadLog] = {
    checkpointDirOption.map { checkpointDir =>
      val logDir = ReceivedBlockTracker.checkpointDirToLogDir(checkpointDirOption.get)
      WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
    }
  }

  /** Check if the write ahead log is enabled. This is only used for testing purposes. */
  private[streaming] def isWriteAheadLogEnabled: Boolean = writeAheadLogOption.nonEmpty
}

private[streaming] object ReceivedBlockTracker {
  def checkpointDirToLogDir(checkpointDir: String): String = {
    new Path(checkpointDir, "receivedBlockMetadata").toString
  }
}
