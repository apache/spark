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
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason

import org.apache.spark.Logging
import org.apache.spark.streaming.Duration
import org.apache.spark.util.ThreadUtils

/**
 * This is a helper class for managing Kinesis checkpointing.
 *
 * @param receiver The receiver that keeps track of which sequence numbers we can checkpoint
 * @param checkpointInterval How frequently we will checkpoint to DynamoDB
 * @param workerId Worker Id of KCL worker for logging purposes
 */
private[kinesis] class KinesisCheckpointer(
    receiver: KinesisReceiver[_],
    checkpointInterval: Duration,
    workerId: String) extends Logging {

  // a map from shardId's to checkpointers
  private val checkpointers = new ConcurrentHashMap[String, IRecordProcessorCheckpointer]()

  private val lastCheckpointedSeqNums = new ConcurrentHashMap[String, String]()

  private val checkpointerThread = startCheckpointerThread()

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
    checkpoint(shardId, Option(checkpointer))
    checkpointers.remove(shardId)
  }

  /** Perform the checkpoint. Exposed for tests. */
  private[kinesis] def checkpoint(
      shardId: String,
      checkpointer: Option[IRecordProcessorCheckpointer]): Unit = {
    // if this method throws an exception, then the scheduled task will not run again
    try {
      checkpointer.foreach { cp =>
        receiver.getLatestSeqNumToCheckpoint(shardId).foreach { latestSeqNum =>
          val lastSeqNum = lastCheckpointedSeqNums.get(shardId)
          // Kinesis sequence numbers are monotonically increasing strings, therefore we can do
          // safely do the string comparison
          if (lastSeqNum == null || latestSeqNum > lastSeqNum) {
            /* Perform the checkpoint */
            KinesisRecordProcessor.retryRandom(cp.checkpoint(latestSeqNum), 4, 100)
            logDebug(s"Checkpoint:  WorkerId $workerId completed checkpoint at sequence number" +
              s" $latestSeqNum for shardId $shardId")
            lastCheckpointedSeqNums.put(shardId, latestSeqNum)
          }
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to checkpoint to DynamoDB.", e)
    }
  }

  private def checkpointAll(): Unit = {
    val shardIds = checkpointers.keys()
    while (shardIds.hasMoreElements) {
      val shardId = shardIds.nextElement()
      checkpoint(shardId, Option(checkpointers.get(shardId)))
    }
  }

  /**
   * Start the checkpointer thread with the given checkpoint duration. Exposed for tests.
   */
  private[kinesis] def startCheckpointerThread(): ScheduledFuture[_] = {
    val period = checkpointInterval.milliseconds
    val ex =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"Kinesis Checkpointer - Worker $workerId")
    val task = new Runnable {
      def run() = checkpointAll()
    }
    ex.scheduleAtFixedRate(task, period, period, TimeUnit.MILLISECONDS)
  }

  /**
   * Shutdown the checkpointer. Should be called on the onStop of the Receiver.
   */
  def shutdown(): Unit = {
    checkpointerThread.cancel(false)
    checkpointAll()
    checkpointers.clear()
    lastCheckpointedSeqNums.clear()
  }
}
