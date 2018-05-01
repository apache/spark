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
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset
import org.apache.spark.util.ThreadUtils

/**
 * A wrapper for a continuous processing data reader, including a reading queue and epoch markers.
 *
 * This will be instantiated once per partition - successive calls to compute() in the
 * [[ContinuousDataSourceRDD]] will reuse the same reader. This is required to get continuity of
 * offsets across epochs.
 *
 * For performance reasons, this is very weakly encapsulated. There are six handles for the RDD:
 *  * currentOffset - contains the offset of the most recent row which a compute() iterator has sent
 *    upwards. The RDD is responsible for advancing this.
 *  * currentEpoch - the epoch which is currently occurring. The RDD is responsible for incrementing
 *    this before ending the compute() iterator.
 *  * queue - the queue of incoming rows (row, offset) or epoch markers (null, null). The
 *    ContinuousQueuedDataReader writes into this queue, and RDD.compute() will read from it.
 *  * {epochPoll|dataReader}Failed - flags to check if the epoch poll and data reader threads are
 *    still running. These threads won't be restarted if they fail, so the RDD should intercept
 *    this state when convenient to fail the query.
 *  * close() - to close this reader when the query is going to shut down.
 */
class ContinuousQueuedDataReader(
    split: Partition,
    context: TaskContext,
    dataQueueSize: Int,
    epochPollIntervalMs: Long) extends Closeable {
  private val reader = split.asInstanceOf[DataSourceRDDPartition[UnsafeRow]]
    .readerFactory.createDataReader()

  // Important sequencing - we must get our starting point before the provider threads start running
  var currentOffset: PartitionOffset = ContinuousDataSourceRDD.getBaseReader(reader).getOffset
  var currentEpoch: Long = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

  // This queue contains two types of messages:
  // * (null, null) representing an epoch boundary.
  // * (row, off) containing a data row and its corresponding PartitionOffset.
  val queue = new ArrayBlockingQueue[(UnsafeRow, PartitionOffset)](dataQueueSize)

  val epochPollFailed = new AtomicBoolean(false)
  val dataReaderFailed = new AtomicBoolean(false)

  private val coordinatorId = context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)

  private val epochPollExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    s"epoch-poll--$coordinatorId--${context.partitionId()}")
  val epochPollRunnable = new EpochPollRunnable(queue, context, epochPollFailed)
  epochPollExecutor.scheduleWithFixedDelay(
    epochPollRunnable, 0, epochPollIntervalMs, TimeUnit.MILLISECONDS)

  val dataReaderThread = new DataReaderThread(reader, queue, context, dataReaderFailed)
  dataReaderThread.setDaemon(true)
  dataReaderThread.start()

  context.addTaskCompletionListener(_ => {
    this.close()
  })

  override def close(): Unit = {
    dataReaderThread.interrupt()
    epochPollExecutor.shutdown()
  }
}
