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
package org.apache.spark.streaming.kinesis

import java.util.concurrent._

import scala.util.control.NonFatal

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{Clock, SystemClock}

/**
 * This is a helper class for managing Kinesis checkpointing.
 *
 * @param receiver The receiver that keeps track of which sequence numbers we can checkpoint
 * @param checkpointInterval How frequently we will checkpoint to DynamoDB
 * @param workerId Worker Id of KCL worker for logging purposes
 * @param clock In order to use ManualClocks for the purpose of testing
 */
private[kinesis] class KinesisCheckpointer(
    receiver: KinesisReceiver[_],
    checkpointInterval: Duration,
    workerId: String,
    clock: Clock = new SystemClock) extends Logging {

  // a map from shardId's to checkpointers
  private val checkpointers = new ConcurrentHashMap[String, IRecordProcessorCheckpointer]()

  private val lastCheckpointedSeqNums = new ConcurrentHashMap[String, String]()

  private val checkpointerThread: RecurringTimer = startCheckpointerThread()

  /** Update the checkpointer instance to the most recent one for the given shardId. */
  def setCheckpointer(shardId: String, checkpointer: IRecordProcessorCheckpointer): Unit = {
    checkpointers.put(shardId, checkpointer)
  }

  /**
   * Stop tracking the specified shardId.
   *
   * If a checkpointer is provided, e.g. on IRecordProcessor.shutdown [[ShutdownReason.TERMINATE]],
   * we will use that to make the final checkpoint. If `null` is provided, we will not make the
   * checkpoint, e.g. in case of [[ShutdownReason.ZOMBIE]].
   */
  def removeCheckpointer(shardId: String, checkpointer: IRecordProcessorCheckpointer): Unit = {
    synchronized {
      checkpointers.remove(shardId)
    }
    if (checkpointer != null) {
      try {
        // We must call `checkpoint()` with no parameter to finish reading shards.
        // See a URL below for details:
        // https://forums.aws.amazon.com/thread.jspa?threadID=244218
        KinesisRecordProcessor.retryRandom(checkpointer.checkpoint(), 4, 100)
      } catch {
        case NonFatal(e) =>
          logError(s"Exception:  WorkerId $workerId encountered an exception while checkpointing" +
            s"to finish reading a shard of $shardId.", e)
          // Rethrow the exception to the Kinesis Worker that is managing this RecordProcessor
          throw e
      }
    }
  }

  /** Perform the checkpoint. */
  private def checkpoint(shardId: String, checkpointer: IRecordProcessorCheckpointer): Unit = {
    try {
      if (checkpointer != null) {
        receiver.getLatestSeqNumToCheckpoint(shardId).foreach { latestSeqNum =>
          val lastSeqNum = lastCheckpointedSeqNums.get(shardId)
          // Kinesis sequence numbers are monotonically increasing strings, therefore we can do
          // safely do the string comparison
          if (lastSeqNum == null || latestSeqNum > lastSeqNum) {
            /* Perform the checkpoint */
            KinesisRecordProcessor.retryRandom(checkpointer.checkpoint(latestSeqNum), 4, 100)
            logDebug(s"Checkpoint:  WorkerId $workerId completed checkpoint at sequence number" +
              s" $latestSeqNum for shardId $shardId")
            lastCheckpointedSeqNums.put(shardId, latestSeqNum)
          }
        }
      } else {
        logDebug(s"Checkpointing skipped for shardId $shardId. Checkpointer not set.")
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to checkpoint shardId $shardId to DynamoDB.", e)
    }
  }

  /** Checkpoint the latest saved sequence numbers for all active shardId's. */
  private def checkpointAll(): Unit = synchronized {
    // if this method throws an exception, then the scheduled task will not run again
    try {
      val shardIds = checkpointers.keys()
      while (shardIds.hasMoreElements) {
        val shardId = shardIds.nextElement()
        checkpoint(shardId, checkpointers.get(shardId))
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to checkpoint to DynamoDB.", e)
    }
  }

  /**
   * Start the checkpointer thread with the given checkpoint duration.
   */
  private def startCheckpointerThread(): RecurringTimer = {
    val period = checkpointInterval.milliseconds
    val threadName = s"Kinesis Checkpointer - Worker $workerId"
    val timer = new RecurringTimer(clock, period, _ => checkpointAll(), threadName)
    timer.start()
    logDebug(s"Started checkpointer thread: $threadName")
    timer
  }

  /**
   * Shutdown the checkpointer. Should be called on the onStop of the Receiver.
   */
  def shutdown(): Unit = {
    // the recurring timer checkpoints for us one last time.
    checkpointerThread.stop(interruptTimer = false)
    checkpointers.clear()
    lastCheckpointedSeqNums.clear()
    logInfo("Successfully shutdown Kinesis Checkpointer.")
  }
}
