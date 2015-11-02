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

/**
 * This is a helper class for managing Kinesis checkpointing.
 *
 * @param receiver The receiver that keeps track of which sequence numbers we can checkpoint
 * @param checkpointInterval How frequently we will checkpoint to DynamoDB
 * @param workerId Worker Id of KCL worker for logging purposes
 * @param shardId The shard this worker was consuming data from
 */
private[kinesis] class KinesisCheckpointState[T](
    receiver: KinesisReceiver[T],
    checkpointInterval: Duration,
    workerId: String,
    shardId: String) extends Logging {

  private var _checkpointer: Option[IRecordProcessorCheckpointer] = None

  private val checkpointerThread = startCheckpointerThread()

  /** Update the checkpointer instance to the most recent one. */
  def setCheckpointer(checkpointer: IRecordProcessorCheckpointer): Unit = {
    _checkpointer = Option(checkpointer)
  }

  /** Perform the checkpoint */
  private def checkpoint(checkpointer: Option[IRecordProcessorCheckpointer]): Unit = {
    // if this method throws an exception, then the scheduled task will not run again
    try {
      checkpointer.foreach { cp =>
        receiver.getLatestSeqNumToCheckpoint(shardId).foreach { latestSeqNum =>
          /* Perform the checkpoint */
          KinesisRecordProcessor.retryRandom(cp.checkpoint(latestSeqNum), 4, 100)

          logDebug(s"Checkpoint:  WorkerId $workerId completed checkpoint at sequence number" +
            s" $latestSeqNum for shardId $shardId")
        }
      }
    } catch {
      case NonFatal(e) =>
        logError("Failed to checkpoint to DynamoDB.", e)
    }
  }

  /** Start the checkpointer thread with the given checkpoint duration. */
  private def startCheckpointerThread(): ScheduledFuture[_] = {
    val period = checkpointInterval.milliseconds
    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      def run() = checkpoint(_checkpointer)
    }
    ex.scheduleAtFixedRate(task, period, period, TimeUnit.MILLISECONDS)
  }

  /**
   * Shutdown the checkpointing task. We don't interrupt an ongoing checkpoint process.
   *
   * If a checkpointer is provided, e.g. on IRecordProcessor.shutdown [[ShutdownReason.TERMINATE]],
   * we will use that to make the final checkpoint. If `null` is provided, we will not make the
   * checkpoint, e.g. in case of [[ShutdownReason.ZOMBIE]].
   */
  def shutdown(checkpointer: IRecordProcessorCheckpointer): Unit = {
    checkpointerThread.cancel(false)
    checkpoint(Option(checkpointer))
  }
}
