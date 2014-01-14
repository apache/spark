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

/** Base trait for events related to StreamingListener */
sealed trait StreamingListenerEvent

case class StreamingListenerBatchCompleted(batchInfo: BatchInfo) extends StreamingListenerEvent
case class StreamingListenerBatchStarted(batchInfo: BatchInfo) extends StreamingListenerEvent

/** An event used in the listener to shutdown the listener daemon thread. */
private[scheduler] case object StreamingListenerShutdown extends StreamingListenerEvent

/**
 * A listener interface for receiving information about an ongoing streaming
 * computation.
 */
trait StreamingListener {
  /**
   * Called when processing of a batch has completed
   */
  def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) { }

  /**
   * Called when processing of a batch has started
   */
  def onBatchStarted(batchStarted: StreamingListenerBatchStarted) { }
}


/**
 * A simple StreamingListener that logs summary statistics across Spark Streaming batches
 * @param numBatchInfos Number of last batches to consider for generating statistics (default: 10)
 */
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
