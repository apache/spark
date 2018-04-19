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

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset
import org.apache.spark.util.ThreadUtils

/**
 * An object containing a queue of continuous processing records read in.
 * Instantiated once per task.
 */
class ContinuousReaderForTask(
    split: Partition,
    context: TaskContext,
    dataQueueSize: Int,
    epochPollIntervalMs: Long) {
  // This queue contains two types of messages:
  // * (null, null) representing an epoch boundary.
  // * (row, off) containing a data row and its corresponding PartitionOffset.
  val queue = new ArrayBlockingQueue[(UnsafeRow, PartitionOffset)](dataQueueSize)

  val epochPollFailed = new AtomicBoolean(false)
  val dataReaderFailed = new AtomicBoolean(false)

  private val reader = split.asInstanceOf[DataSourceRDDPartition[UnsafeRow]]
    .readerFactory.createDataReader()

  private val coordinatorId = context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)

  private val epochPollExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    s"epoch-poll--$coordinatorId--${context.partitionId()}")
  val epochPollRunnable = new EpochPollRunnable(queue, context, epochPollFailed)
  epochPollExecutor.scheduleWithFixedDelay(
    epochPollRunnable, 0, epochPollIntervalMs, TimeUnit.MILLISECONDS)

  // Important sequencing - we must get start offset before the data reader thread begins
  var currentOffset = ContinuousDataSourceRDD.getBaseReader(reader).getOffset
  var currentEpoch =
    context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

  val dataReaderThread = new DataReaderThread(reader, queue, context, dataReaderFailed)
  dataReaderThread.setDaemon(true)
  dataReaderThread.start()

  context.addTaskCompletionListener(_ => {
    dataReaderThread.interrupt()
    epochPollExecutor.shutdown()
  })
}
