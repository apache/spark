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

import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.streaming.{Time, StreamingContext}

private[streaming] trait DirectRateLimiter extends Serializable with Logging {
  self: RateLimiter =>

  // Map to record the number of processed records of each batch interval
  protected val processedRecordsMap = new mutable.HashMap[Time, Long]()
    with mutable.SynchronizedMap[Time, Long]
  protected val slideDurationInMs = streamingContext.graph.batchDuration.milliseconds
  // To record the number of un-processed records
  protected var unProcessedRecords: Long = 0L

  private val dynamicRateUpdater = new StreamingListener {
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      val processedRecords = processedRecordsMap.getOrElse(batchCompleted.batchInfo.batchTime, 0L)
      val processedTimeInMs = batchCompleted.batchInfo.processingDelay.getOrElse(
        throw new IllegalStateException("Illegal status: cannot get the processed delay"))
      computeEffectiveRate(processedRecords, processedTimeInMs)

      unProcessedRecords -= processedRecords
      processedRecordsMap --= processedRecordsMap.keys.filter(
        _ < batchCompleted.batchInfo.batchTime)
    }
  }

  streamingContext.addStreamingListener(dynamicRateUpdater)

  def streamingContext: StreamingContext

  /**
   * Get the max number of messages to process for the next batch.
   * @return None, no limitation of number of max messages to process. Some[Long]. number of
   *         messages to process in the next batch.
   */
  def maxMessages: Option[Long]

  /** Record the number of processed records in this batch interval */
  def updateProcessedRecords(batchTime: Time, processedRecords: Long): Unit = {
    unProcessedRecords += processedRecords
    processedRecordsMap += ((batchTime, processedRecords))
  }
}

private[streaming]
class FixedDirectRateLimiter(
    val defaultRate: Double,
    @transient val streamingContext: StreamingContext)
  extends FixedRateLimiter with DirectRateLimiter {
  final def isDriver: Boolean = true

  def maxMessages: Option[Long] = {
    if (effectiveRate == Int.MaxValue.toDouble) {
      None
    } else {
      Some((effectiveRate * slideDurationInMs / 1000).toLong)
    }
  }
}

private[streaming]
class DynamicDirectRateLimiter(
    val defaultRate: Double,
    val slowStartInitialRate: Double,
    @transient val streamingContext: StreamingContext)
  extends DynamicRateLimiter with DirectRateLimiter {
  final def isDriver: Boolean = true

  def maxMessages: Option[Long] = {
    if (effectiveRate == Int.MaxValue.toDouble) {
      return None
    }

    val optNumMaxMsgs = if (unProcessedRecords > 0) {
      val timeForUnProcessedRecords = (unProcessedRecords / effectiveRate).toLong * 1000
      if (timeForUnProcessedRecords > slideDurationInMs) {
        Some(0L)
      } else {
        Some(((slideDurationInMs - timeForUnProcessedRecords) / 1000 * effectiveRate).toLong)
      }
    } else {
      Some((effectiveRate * slideDurationInMs / 1000).toLong)
    }

    logDebug(s"Get the number of maximum messages ${optNumMaxMsgs.get} for the next batch")
    optNumMaxMsgs
  }
}
