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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Distribution

/**
 * :: DeveloperApi ::
 * Base trait for events related to StreamingListener
 */
@DeveloperApi
sealed trait StreamingListenerEvent

@DeveloperApi
case class StreamingListenerStreamingStarted(time: Long) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchSubmitted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchCompleted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchStarted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerOutputOperationStarted(outputOperationInfo: OutputOperationInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerOutputOperationCompleted(outputOperationInfo: OutputOperationInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverStarted(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverError(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverStopped(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

/**
 * :: DeveloperApi ::
 * A listener interface for receiving information about an ongoing streaming
 * computation.
 */
@DeveloperApi
trait StreamingListener {

  /** Called when the streaming has been started */
  def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = { }

  /** Called when a receiver has been started */
  def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = { }

  /** Called when a receiver has reported an error */
  def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = { }

  /** Called when a receiver has been stopped */
  def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = { }

  /** Called when a batch of jobs has been submitted for processing. */
  def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = { }

  /** Called when processing of a batch of jobs has started.  */
  def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = { }

  /** Called when processing of a batch of jobs has completed. */
  def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = { }

  /** Called when processing of a job of a batch has started. */
  def onOutputOperationStarted(
      outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = { }

  /** Called when processing of a job of a batch has completed. */
  def onOutputOperationCompleted(
      outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = { }
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

  override def onBatchCompleted(batchStarted: StreamingListenerBatchCompleted): Unit = {
    batchInfos.enqueue(batchStarted.batchInfo)
    if (batchInfos.size > numBatchInfos) batchInfos.dequeue()
    printStats()
  }

  def printStats(): Unit = {
    showMillisDistribution("Total delay: ", _.totalDelay)
    showMillisDistribution("Processing time: ", _.processingDelay)
  }

  def showMillisDistribution(heading: String, getMetric: BatchInfo => Option[Long]): Unit = {
    org.apache.spark.scheduler.StatsReportListener.showMillisDistribution(
      heading, extractDistribution(getMetric))
  }

  def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(batchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }
}
