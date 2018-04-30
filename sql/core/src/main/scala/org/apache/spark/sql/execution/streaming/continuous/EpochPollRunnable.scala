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

import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset

/**
 * The epoch marker component of [[ContinuousQueuedDataReader]]. Populates the queue with
 * (null, null) when a new epoch marker arrives.
 */
class EpochPollRunnable(
    queue: BlockingQueue[(UnsafeRow, PartitionOffset)],
    context: TaskContext,
    failedFlag: AtomicBoolean)
  extends Thread with Logging {
  private[continuous] var failureReason: Throwable = _

  private val epochEndpoint = EpochCoordinatorRef.get(
    context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY), SparkEnv.get)
  // Note that this is *not* the same as the currentEpoch in [[ContinuousDataQueuedReader]]! That
  // field represents the epoch wrt the data being processed. The currentEpoch here is just a
  // counter to ensure we send the appropriate number of markers if we fall behind the driver.
  private var currentEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

  override def run(): Unit = {
    try {
      val newEpoch = epochEndpoint.askSync[Long](GetCurrentEpoch)
      for (i <- currentEpoch to newEpoch - 1) {
        queue.put((null, null))
        logDebug(s"Sent marker to start epoch ${i + 1}")
      }
      currentEpoch = newEpoch
    } catch {
      case t: Throwable =>
        failureReason = t
        failedFlag.set(true)
        throw t
    }
  }
}
