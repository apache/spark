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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.util.{WriteAheadLog, WriteAheadLogUtils}
import org.apache.spark.util.{Clock, Utils}
import org.apache.spark.{Logging, SparkConf}

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
  private val walWriteQueue = new ConcurrentLinkedQueue[ReceivedBlockTrackerLogEvent]()

  private trait WALWriteStatus
  private object Pending extends WALWriteStatus
  private object Success extends WALWriteStatus
  private object Fail extends WALWriteStatus

  // stores the status for wal writes added to the queue
  private val walWriteStatusMap =
    new ConcurrentHashMap[ReceivedBlockTrackerLogEvent, WALWriteStatus]()

  private val WAL_WRITE_STATUS_CHECK_BACKOFF = 10 // 10 millis
  private val WAL_WRITE_STATUS_TIMEOUT = 5000 // 5 seconds

  private val writeAheadLogBatchWriter: Option[BatchLogWriter] = createBatchWriteAheadLogWriter()

  private var lastAllocatedBatchTime: Time = null

  // Recover block information from write ahead logs
  if (recoverFromWriteAheadLog) {
    recoverPastEvents()
  }

  /** Add received block. This event will get written to the write ahead log (if enabled). */
  private def addBlock0(
      receivedBlockInfo: ReceivedBlockInfo,
      writeOp: ReceivedBlockTrackerLogEvent => Boolean): Boolean = {
    try {
      val result = writeOp(BlockAdditionEvent(receivedBlockInfo))
      if (result) {
        afterBlockAddAcknowledged(receivedBlockInfo)
        logDebug(s"Stream ${receivedBlockInfo.streamId} received " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId}")
      } else {
        logDebug(s"Failed to acknowledge stream ${receivedBlockInfo.streamId} receiving " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId} in the Write Ahead Log.")
      }
      result
    } catch {
      case e: Exception =>
        logError(s"Error adding block $receivedBlockInfo", e)
        false
    }
  }

  /** Add received block. This event will get written to the write ahead log (if enabled). */
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = synchronized {
    addBlock0(receivedBlockInfo, writeToLog)
  }

  private def afterBlockAddAcknowledged(receivedBlockInfo: ReceivedBlockInfo): Unit = {
    getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
  }

  def addBlockAsync(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    addBlock0(receivedBlockInfo, writeToLogAsync)
  }

  def writeToLogAsync(event: ReceivedBlockTrackerLogEvent): Boolean = {
    if (!isWriteAheadLogEnabled) return true // return early if WAL is not enabled
    walWriteStatusMap.put(event, Pending)
    walWriteQueue.offer(event)
    var timedOut = false
    val start = clock.getTimeMillis()
    while (walWriteStatusMap.get(event) == Pending) {
      Thread.sleep(WAL_WRITE_STATUS_CHECK_BACKOFF)
      if (clock.getTimeMillis() - WAL_WRITE_STATUS_TIMEOUT > start) timedOut = true
    }
    walWriteStatusMap.remove(event) == Success
  }

  /**
   * Allocate all unallocated blocks to the given batch.
   * This event will get written to the write ahead log (if enabled).
   */
  private def allocateBlocksToBatch0(
      batchTime: Time,
      writeOp: ReceivedBlockTrackerLogEvent => Boolean): Unit = {
    if (lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {
      val streamIdToBlocks = streamIds.map { streamId =>
          (streamId, getReceivedBlockQueue(streamId).dequeueAll(x => true))
      }.toMap
      val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)
      val result = writeOp(BatchAllocationEvent(batchTime, allocatedBlocks))
      if (result) {
        afterBatchAllocationAcknowledged(batchTime, allocatedBlocks)
      } else {
        logInfo(s"Possibly processed batch $batchTime need to be processed again in WAL recovery")
      }
    } else {
      // This situation occurs when:
      // 1. WAL is ended with BatchAllocationEvent, but without BatchCleanupEvent,
      // possibly processed batch job or half-processed batch job need to be processed again,
      // so the batchTime will be equal to lastAllocatedBatchTime.
      // 2. Slow checkpointing makes recovered batch time older than WAL recovered
      // lastAllocatedBatchTime.
      // This situation will only occurs in recovery time.
      logInfo(s"Possibly processed batch $batchTime need to be processed again in WAL recovery")
    }
  }

  /**
   * Allocate all unallocated blocks to the given batch.
   * This event will get written to the write ahead log (if enabled).
   */
  def allocateBlocksToBatch(batchTime: Time): Unit = synchronized {
    allocateBlocksToBatch0(batchTime, writeToLog)
  }

  def allocateBlocksToBatchAsync(batchTime: Time): Unit = {
    allocateBlocksToBatch0(batchTime, writeToLogAsync)
  }

  private def afterBatchAllocationAcknowledged(
      batchTime: Time,
      allocatedBlocks: AllocatedBlocks): Unit = {
    timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
    lastAllocatedBatchTime = batchTime
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
  private def cleanupOldBatches0(
      cleanupThreshTime: Time,
      waitForCompletion: Boolean,
      writeOp: ReceivedBlockTrackerLogEvent => Boolean): Unit = {
    require(cleanupThreshTime.milliseconds < clock.getTimeMillis())
    val timesToCleanup = timeToAllocatedBlocks.keys.filter { _ < cleanupThreshTime }.toSeq
    logInfo("Deleting batches " + timesToCleanup)
    if (writeOp(BatchCleanupEvent(timesToCleanup))) {
      afterBatchCleanupAcknowledged(cleanupThreshTime, waitForCompletion, timesToCleanup)
    } else {
      logWarning("Failed to acknowledge batch clean up in the Write Ahead Log.")
    }
  }

  /**
   * Clean up block information of old batches. If waitForCompletion is true, this method
   * returns only after the files are cleaned up.
   */
  def cleanupOldBatches(cleanupThreshTime: Time, waitForCompletion: Boolean): Unit = synchronized {
    cleanupOldBatches0(cleanupThreshTime, waitForCompletion, writeToLog)
  }

  /**
   * Clean up block information of old batches. If waitForCompletion is true, this method
   * returns only after the files are cleaned up.
   */
  def cleanupOldBatchesAsync(cleanupThreshTime: Time, waitForCompletion: Boolean): Unit = {
    cleanupOldBatches0(cleanupThreshTime, waitForCompletion, writeToLogAsync)
  }

  private def afterBatchCleanupAcknowledged(
      cleanupThreshTime: Time,
      waitForCompletion: Boolean,
      timesToCleanup: Seq[Time]): Unit = {
    timeToAllocatedBlocks --= timesToCleanup
    writeAheadLogOption.foreach(_.clean(cleanupThreshTime.milliseconds, waitForCompletion))
  }

  /** Stop the block tracker. */
  def stop() {
    writeAheadLogBatchWriter.foreach { _.stop() }
    writeAheadLogOption.foreach { _.close() }
  }

  /**
   * Recover all the tracker actions from the write ahead logs to recover the state (unallocated
   * and allocated block info) prior to failure.
   */
  private def recoverPastEvents(): Unit = synchronized {
    // Insert the recovered block information
    def insertAddedBlock(receivedBlockInfo: ReceivedBlockInfo) {
      logTrace(s"Recovery: Inserting added block $receivedBlockInfo")
      receivedBlockInfo.setBlockIdInvalid()
      afterBlockAddAcknowledged(receivedBlockInfo)
    }

    // Insert the recovered block-to-batch allocations and clear the queue of received blocks
    // (when the blocks were originally allocated to the batch, the queue must have been cleared).
    def insertAllocatedBatch(batchTime: Time, allocatedBlocks: AllocatedBlocks) {
      logTrace(s"Recovery: Inserting allocated batch for time $batchTime to " +
        s"${allocatedBlocks.streamIdToAllocatedBlocks}")
      streamIdToUnallocatedBlockQueues.values.foreach { _.clear() }
      afterBatchAllocationAcknowledged(batchTime, allocatedBlocks)
    }

    // Cleanup the batch allocations
    def cleanupBatches(batchTimes: Seq[Time]) {
      logTrace(s"Recovery: Cleaning up batches $batchTimes")
      timeToAllocatedBlocks --= batchTimes
    }

    def resolveEvent(event: ReceivedBlockTrackerLogEvent): Unit = {
      event match {
        case BlockAdditionEvent(receivedBlockInfo) =>
          insertAddedBlock(receivedBlockInfo)
        case BatchAllocationEvent(time, allocatedBlocks) =>
          insertAllocatedBatch(time, allocatedBlocks)
        case BatchCleanupEvent(batchTimes) =>
          cleanupBatches(batchTimes)
      }
    }

    writeAheadLogOption.foreach { writeAheadLog =>
      logInfo(s"Recovering from write ahead logs in ${checkpointDirOption.get}")
      writeAheadLog.readAll().asScala.foreach { byteBuffer =>
        logTrace("Recovering record " + byteBuffer)
        try {
          Utils.deserialize[List[ReceivedBlockTrackerLogEvent]](
            byteBuffer.array, Thread.currentThread().getContextClassLoader).foreach(resolveEvent)
        } catch {
          case e: ClassCastException =>
            resolveEvent(Utils.deserialize[ReceivedBlockTrackerLogEvent](
              byteBuffer.array, Thread.currentThread().getContextClassLoader))
        }
      }
    }
  }

  /** Write an update to the tracker to the write ahead log */
  private def writeToLog(record: ReceivedBlockTrackerLogEvent): Boolean = {
    if (isWriteAheadLogEnabled) {
      logDebug(s"Writing to log $record")
      writeAheadLogOption.foreach { logManager =>
        logManager.write(ByteBuffer.wrap(Utils.serialize(record)), clock.getTimeMillis())
      }
    }
    true
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

  private def createBatchWriteAheadLogWriter(): Option[BatchLogWriter] = {
    if (!WriteAheadLogUtils.isBatchingEnabled(conf)) return None
    val writer = checkpointDirOption.map(_ => new BatchLogWriter)
    writer.foreach { runnable =>
      new Thread(runnable, "Batch WAL Writer").start()
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = runnable.stop()
      })
    }
    writer
  }

  /** Check if the write ahead log is enabled. This is only used for testing purposes. */
  private[streaming] def isWriteAheadLogEnabled: Boolean = writeAheadLogOption.nonEmpty

  private class BatchLogWriter extends Runnable {

    var active: Boolean = true

    private def writeRecords(records: List[ReceivedBlockTrackerLogEvent]): Unit = {
      writeAheadLogOption.foreach { logManager =>
        if (records.nonEmpty) {
          logDebug(s"Writing to log $records")
          logManager.write(ByteBuffer.wrap(Utils.serialize(records)), clock.getTimeMillis())
        }
      }
    }

    def stop(): Unit = {
      active = false
    }

    private def flushRecords(): Unit = {
      val buffer = new ArrayBuffer[ReceivedBlockTrackerLogEvent]()
      while (!walWriteQueue.isEmpty) {
        buffer.append(walWriteQueue.poll())
      }
      val records = buffer.toList
      try {
        writeRecords(records)
        records.foreach(walWriteStatusMap.put(_, Success))
      } catch {
        case e: Exception =>
          logWarning(s"Batch WAL Writer failed to write $records")
          records.foreach(walWriteStatusMap.put(_, Fail))
      }
    }

    override def run(): Unit = {
      while (active) {
        flushRecords()
        Thread.sleep(WAL_WRITE_STATUS_CHECK_BACKOFF)
      }
    }
  }
}

private[streaming] object ReceivedBlockTracker {
  def checkpointDirToLogDir(checkpointDir: String): String = {
    new Path(checkpointDir, "receivedBlockMetadata").toString
  }
}
