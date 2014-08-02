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

import scala.collection.mutable.Queue

import org.apache.spark.util.Distribution
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Base trait for events related to StreamingListener
 */
@DeveloperApi
sealed trait StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchSubmitted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchCompleted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchStarted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverStarted(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverError(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverStopped(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

/** An event used in the listener to shutdown the listener daemon thread. */
private[scheduler] case object StreamingListenerShutdown extends StreamingListenerEvent

/**
 * :: DeveloperApi ::
 * A listener interface for receiving information about an ongoing streaming
 * computation.
 */
@DeveloperApi
trait StreamingListener {

  /** Called when a receiver has been started */
  def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) { }

  /** Called when a receiver has reported an error */
  def onReceiverError(receiverError: StreamingListenerReceiverError) { }

  /** Called when a receiver has been stopped */
  def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) { }

  /** Called when a batch of jobs has been submitted for processing. */
  def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) { }

  /** Called when processing of a batch of jobs has started.  */
  def onBatchStarted(batchStarted: StreamingListenerBatchStarted) { }

  /** Called when processing of a batch of jobs has completed. */
  def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) { }
}


/**
 * :: DeveloperApi ::
 * A simple StreamingListener that logs summary statistics across Spark Streaming batches
 * @param numBatchInfos Number of last batches to consider for generating statistics (default: 10)
 */
@DeveloperApi
class StatsReportListener(numBatchInfos: Int = 10) extends StreamingListener {
  // Queue containing latest completed batches
  val batchInfos = new Queue[BatchInfo]()

  override def onBatchCompleted(batchStarted: StreamingListenerBatchCompleted) {
    batchInfos.enqueue(batchStarted.batchInfo)
    if (batchInfos.size > numBatchInfos) batchInfos.dequeue()
    printStats()
  }

  def printStats() {
    showMillisDistribution("Total delay: ", _.totalDelay)
    showMillisDistribution("Processing time: ", _.processingDelay)
  }

  def showMillisDistribution(heading: String, getMetric: BatchInfo => Option[Long]) {
    org.apache.spark.scheduler.StatsReportListener.showMillisDistribution(
      heading, extractDistribution(getMetric))
  }

  def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(batchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }
}
