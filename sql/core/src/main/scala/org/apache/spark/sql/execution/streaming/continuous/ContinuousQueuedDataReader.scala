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

package org.apache.spark.sql.execution.streaming.continuous

import java.io.Closeable
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousPartitionReader, PartitionOffset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils

/**
 * A wrapper for a continuous processing data reader, including a reading queue and epoch markers.
 *
 * This will be instantiated once per partition - successive calls to compute() in the
 * [[ContinuousDataSourceRDD]] will reuse the same reader. This is required to get continuity of
 * offsets across epochs. Each compute() should call the next() method here until null is returned.
 */
class ContinuousQueuedDataReader(
    partitionIndex: Int,
    reader: ContinuousPartitionReader[InternalRow],
    schema: StructType,
    context: TaskContext,
    dataQueueSize: Int,
    epochPollIntervalMs: Long) extends Closeable {
  // Important sequencing - we must get our starting point before the provider threads start running
  private var currentOffset: PartitionOffset = reader.getOffset

  /**
   * The record types in the read buffer.
   */
  sealed trait ContinuousRecord
  case object EpochMarker extends ContinuousRecord
  case class ContinuousRow(row: InternalRow, offset: PartitionOffset) extends ContinuousRecord

  private val queue = new ArrayBlockingQueue[ContinuousRecord](dataQueueSize)

  private val coordinatorId = context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)
  private val epochCoordEndpoint = EpochCoordinatorRef.get(
    context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY), SparkEnv.get)

  private val epochMarkerExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    s"epoch-poll--$coordinatorId--${context.partitionId()}")
  private val epochMarkerGenerator = new EpochMarkerGenerator
  epochMarkerExecutor.scheduleWithFixedDelay(
    epochMarkerGenerator, 0, epochPollIntervalMs, TimeUnit.MILLISECONDS)

  private val dataReaderThread = new DataReaderThread(schema)
  dataReaderThread.setDaemon(true)
  dataReaderThread.start()

  context.addTaskCompletionListener[Unit](_ => {
    this.close()
  })

  private def shouldStop() = {
    context.isInterrupted() || context.isCompleted()
  }

  /**
   * Return the next row to be read in the current epoch, or null if the epoch is done.
   *
   * After returning null, the [[ContinuousDataSourceRDD]] compute() for the following epoch
   * will call next() again to start getting rows.
   */
  def next(): InternalRow = {
    val POLL_TIMEOUT_MS = 1000
    var currentEntry: ContinuousRecord = null

    while (currentEntry == null) {
      if (shouldStop()) {
        // Force the epoch to end here. The writer will notice the context is interrupted
        // or completed and not start a new one. This makes it possible to achieve clean
        // shutdown of the streaming query.
        // TODO: The obvious generalization of this logic to multiple stages won't work. It's
        // invalid to send an epoch marker from the bottom of a task if all its child tasks
        // haven't sent one.
        currentEntry = EpochMarker
      } else {
        if (dataReaderThread.failureReason != null) {
          throw new SparkException("Data read failed", dataReaderThread.failureReason)
        }
        if (epochMarkerGenerator.failureReason != null) {
          throw new SparkException(
            "Epoch marker generation failed",
            epochMarkerGenerator.failureReason)
        }
        currentEntry = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }
    }

    currentEntry match {
      case EpochMarker =>
        epochCoordEndpoint.send(ReportPartitionOffset(
          partitionIndex, EpochTracker.getCurrentEpoch.get, currentOffset))
        null
      case ContinuousRow(row, offset) =>
        currentOffset = offset
        row
    }
  }

  override def close(): Unit = {
    dataReaderThread.interrupt()
    epochMarkerExecutor.shutdown()
  }

  /**
   * The data component of [[ContinuousQueuedDataReader]]. Pushes (row, offset) to the queue when
   * a new row arrives to the [[ContinuousPartitionReader]].
   */
  class DataReaderThread(schema: StructType) extends Thread(
      s"continuous-reader--${context.partitionId()}--" +
        s"${context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)}") with Logging {
    @volatile private[continuous] var failureReason: Throwable = _
    private val toUnsafe = UnsafeProjection.create(schema)

    override def run(): Unit = {
      TaskContext.setTaskContext(context)
      try {
        while (!shouldStop()) {
          if (!reader.next()) {
            // Check again, since reader.next() might have blocked through an incoming interrupt.
            if (!shouldStop()) {
              throw new IllegalStateException(
                "Continuous reader reported no elements! Reader should have blocked waiting.")
            } else {
              return
            }
          }
          // `InternalRow#copy` may not be properly implemented, for safety we convert to unsafe row
          // before copy here.
          queue.put(ContinuousRow(toUnsafe(reader.get()).copy(), reader.getOffset))
        }
      } catch {
        case _: InterruptedException =>
          // Continuous shutdown always involves an interrupt; do nothing and shut down quietly.
          logInfo(s"shutting down interrupted data reader thread $getName")

        case NonFatal(t) =>
          failureReason = t
          logWarning("data reader thread failed", t)
          // If we throw from this thread, we may kill the executor. Let the parent thread handle
          // it.

        case t: Throwable =>
          failureReason = t
          throw t
      } finally {
        reader.close()
      }
    }
  }

  /**
   * The epoch marker component of [[ContinuousQueuedDataReader]]. Populates the queue with
   * EpochMarker when a new epoch marker arrives.
   */
  class EpochMarkerGenerator extends Runnable with Logging {
    @volatile private[continuous] var failureReason: Throwable = _

    private val epochCoordEndpoint = EpochCoordinatorRef.get(
      context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY), SparkEnv.get)
    // Note that this is *not* the same as the currentEpoch in [[ContinuousWriteRDD]]! That
    // field represents the epoch wrt the data being processed. The currentEpoch here is just a
    // counter to ensure we send the appropriate number of markers if we fall behind the driver.
    private var currentEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

    override def run(): Unit = {
      try {
        val newEpoch = epochCoordEndpoint.askSync[Long](GetCurrentEpoch)
        // It's possible to fall more than 1 epoch behind if a GetCurrentEpoch RPC ends up taking
        // a while. We catch up by injecting enough epoch markers immediately to catch up. This will
        // result in some epochs being empty for this partition, but that's fine.
        for (i <- currentEpoch to newEpoch - 1) {
          queue.put(EpochMarker)
          logDebug(s"Sent marker to start epoch ${i + 1}")
        }
        currentEpoch = newEpoch
      } catch {
        case t: Throwable =>
          failureReason = t
          throw t
      }
    }
  }
}
