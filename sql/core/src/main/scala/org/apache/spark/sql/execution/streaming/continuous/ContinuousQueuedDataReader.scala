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
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{Partition, SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset
import org.apache.spark.util.ThreadUtils

/**
 * The record types in a continuous processing buffer.
 */
sealed trait ContinuousRecord
case object EpochMarker extends ContinuousRecord
case class ContinuousRow(row: UnsafeRow, offset: PartitionOffset) extends ContinuousRecord

/**
 * A wrapper for a continuous processing data reader, including a reading queue and epoch markers.
 *
 * This will be instantiated once per partition - successive calls to compute() in the
 * [[ContinuousDataSourceRDD]] will reuse the same reader. This is required to get continuity of
 * offsets across epochs.
 *
 * The RDD is responsible for advancing two fields here, since they need to be updated in line
 * with the data flow:
 *  * currentOffset - contains the offset of the most recent row which a compute() iterator has sent
 *    upwards. The RDD is responsible for advancing this.
 *  * currentEpoch - the epoch which is currently occurring. The RDD is responsible for incrementing
 *    this before ending the compute() iterator.
 */
class ContinuousQueuedDataReader(
    factory: DataReaderFactory[UnsafeRow],
    context: TaskContext,
    dataQueueSize: Int,
    epochPollIntervalMs: Long) extends Closeable {
  private val reader = factory.createDataReader()

  // Important sequencing - we must get our starting point before the provider threads start running
  var currentOffset: PartitionOffset = ContinuousDataSourceRDD.getBaseReader(reader).getOffset
  var currentEpoch: Long = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

  private val queue = new ArrayBlockingQueue[ContinuousRecord](dataQueueSize)

  private val coordinatorId = context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)

  private val epochMarkerExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    s"epoch-poll--$coordinatorId--${context.partitionId()}")
  private val epochMarkerGenerator = new EpochMarkerGenerator
  epochMarkerExecutor.scheduleWithFixedDelay(
    epochMarkerGenerator, 0, epochPollIntervalMs, TimeUnit.MILLISECONDS)

  private val dataReaderThread = new DataReaderThread
  dataReaderThread.setDaemon(true)
  dataReaderThread.start()

  context.addTaskCompletionListener(_ => {
    this.close()
  })

  def next(): ContinuousRecord = {
    val POLL_TIMEOUT_MS = 1000
    var currentEntry: ContinuousRecord = null

    while (currentEntry == null) {
      if (context.isInterrupted() || context.isCompleted()) {
        // Force the epoch to end here. The writer will notice the context is interrupted
        // or completed and not start a new one. This makes it possible to achieve clean
        // shutdown of the streaming query.
        // TODO: The obvious generalization of this logic to multiple stages won't work. It's
        // invalid to send an epoch marker from the bottom of a task if all its child tasks
        // haven't sent one.
        currentEntry = EpochMarker
      } else {
        if (dataReaderThread.failureReason != null) {
          throw new SparkException("data read failed", dataReaderThread.failureReason)
        }
        if (epochMarkerGenerator.failureReason != null) {
          throw new SparkException("epoch poll failed", epochMarkerGenerator.failureReason)
        }
        currentEntry = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }
    }

    currentEntry
  }

  override def close(): Unit = {
    dataReaderThread.interrupt()
    epochMarkerExecutor.shutdown()
  }

  /**
   * The data component of [[ContinuousQueuedDataReader]]. Pushes (row, offset) to the queue when
   * a new row arrives to the [[DataReader]].
   */
  class DataReaderThread extends Thread(
      s"continuous-reader--${context.partitionId()}--" +
        s"${context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)}") {
    @volatile private[continuous] var failureReason: Throwable = _

    override def run(): Unit = {
      TaskContext.setTaskContext(context)
      val baseReader = ContinuousDataSourceRDD.getBaseReader(reader)
      try {
        while (!context.isInterrupted && !context.isCompleted()) {
          if (!reader.next()) {
            // Check again, since reader.next() might have blocked through an incoming interrupt.
            if (!context.isInterrupted && !context.isCompleted()) {
              throw new IllegalStateException(
                "Continuous reader reported no elements! Reader should have blocked waiting.")
            } else {
              return
            }
          }

          queue.put(ContinuousRow(reader.get().copy(), baseReader.getOffset))
        }
      } catch {
        case _: InterruptedException if context.isInterrupted() =>
          // Continuous shutdown always involves an interrupt; do nothing and shut down quietly.

        case t: Throwable =>
          failureReason = t
          // Don't rethrow the exception in this thread. It's not needed, and the default Spark
          // exception handler will kill the executor.
      } finally {
        reader.close()
      }
    }
  }

  /**
   * The epoch marker component of [[ContinuousQueuedDataReader]]. Populates the queue with
   * (null, null) when a new epoch marker arrives.
   */
  class EpochMarkerGenerator extends Runnable with Logging {
    @volatile private[continuous] var failureReason: Throwable = _

    private val epochEndpoint = EpochCoordinatorRef.get(
      context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY), SparkEnv.get)
    // Note that this is *not* the same as the currentEpoch in [[ContinuousDataQueuedReader]]! That
    // field represents the epoch wrt the data being processed. The currentEpoch here is just a
    // counter to ensure we send the appropriate number of markers if we fall behind the driver.
    private var currentEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

    override def run(): Unit = {
      try {
        val newEpoch = epochEndpoint.askSync[Long](GetCurrentEpoch)
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
