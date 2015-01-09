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

package org.apache.spark.streaming.ui

import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.streaming.scheduler._
import scala.collection.mutable.{Queue, HashMap}
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted
import org.apache.spark.streaming.scheduler.BatchInfo
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted
import org.apache.spark.util.Distribution


private[streaming] class StreamingJobProgressListener(ssc: StreamingContext)
  extends StreamingListener {

  private val waitingBatchInfos = new HashMap[Time, BatchInfo]
  private val runningBatchInfos = new HashMap[Time, BatchInfo]
  private val completedaBatchInfos = new Queue[BatchInfo]
  private val batchInfoLimit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 100)
  private var totalCompletedBatches = 0L
  private var totalReceivedRecords = 0L
  private var totalProcessedRecords = 0L
  private val receiverInfos = new HashMap[Int, ReceiverInfo]

  val batchDuration = ssc.graph.batchDuration.milliseconds

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    synchronized {
      receiverInfos(receiverStarted.receiverInfo.streamId) = receiverStarted.receiverInfo
    }
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    synchronized {
      receiverInfos(receiverError.receiverInfo.streamId) = receiverError.receiverInfo
    }
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    synchronized {
      receiverInfos(receiverStopped.receiverInfo.streamId) = receiverStopped.receiverInfo
    }
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) = synchronized {
    runningBatchInfos(batchSubmitted.batchInfo.batchTime) = batchSubmitted.batchInfo
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) = synchronized {
    runningBatchInfos(batchStarted.batchInfo.batchTime) = batchStarted.batchInfo
    waitingBatchInfos.remove(batchStarted.batchInfo.batchTime)

    batchStarted.batchInfo.receivedBlockInfo.foreach { case (_, infos) =>
      totalReceivedRecords += infos.map(_.numRecords).sum
    }
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = synchronized {
    waitingBatchInfos.remove(batchCompleted.batchInfo.batchTime)
    runningBatchInfos.remove(batchCompleted.batchInfo.batchTime)
    completedaBatchInfos.enqueue(batchCompleted.batchInfo)
    if (completedaBatchInfos.size > batchInfoLimit) completedaBatchInfos.dequeue()
    totalCompletedBatches += 1L

    batchCompleted.batchInfo.receivedBlockInfo.foreach { case (_, infos) =>
      totalProcessedRecords += infos.map(_.numRecords).sum
    }
  }

  def numReceivers = synchronized {
    ssc.graph.getReceiverInputStreams().size
  }

  def numTotalCompletedBatches: Long = synchronized {
    totalCompletedBatches
  }

  def numTotalReceivedRecords: Long = synchronized {
    totalReceivedRecords
  }

  def numTotalProcessedRecords: Long = synchronized {
    totalProcessedRecords
  }

  def numUnprocessedBatches: Long = synchronized {
    waitingBatchInfos.size + runningBatchInfos.size
  }

  def waitingBatches: Seq[BatchInfo] = synchronized {
    waitingBatchInfos.values.toSeq
  }

  def runningBatches: Seq[BatchInfo] = synchronized {
    runningBatchInfos.values.toSeq
  }

  def retainedCompletedBatches: Seq[BatchInfo] = synchronized {
    completedaBatchInfos.toSeq
  }

  def processingDelayDistribution: Option[Distribution] = synchronized {
    extractDistribution(_.processingDelay)
  }

  def schedulingDelayDistribution: Option[Distribution] = synchronized {
    extractDistribution(_.schedulingDelay)
  }

  def totalDelayDistribution: Option[Distribution] = synchronized {
    extractDistribution(_.totalDelay)
  }

  def receivedRecordsDistributions: Map[Int, Option[Distribution]] = synchronized {
    val latestBatchInfos = retainedBatches.reverse.take(batchInfoLimit)
    val latestBlockInfos = latestBatchInfos.map(_.receivedBlockInfo)
    (0 until numReceivers).map { receiverId =>
      val blockInfoOfParticularReceiver = latestBlockInfos.map { batchInfo =>
        batchInfo.get(receiverId).getOrElse(Array.empty)
      }
      val recordsOfParticularReceiver = blockInfoOfParticularReceiver.map { blockInfo =>
      // calculate records per second for each batch
        blockInfo.map(_.numRecords).sum.toDouble * 1000 / batchDuration
      }
      val distributionOption = Distribution(recordsOfParticularReceiver)
      (receiverId, distributionOption)
    }.toMap
  }

  def lastReceivedBatchRecords: Map[Int, Long] = {
    val lastReceivedBlockInfoOption = lastReceivedBatch.map(_.receivedBlockInfo)
    lastReceivedBlockInfoOption.map { lastReceivedBlockInfo =>
      (0 until numReceivers).map { receiverId =>
        (receiverId, lastReceivedBlockInfo(receiverId).map(_.numRecords).sum)
      }.toMap
    }.getOrElse {
      (0 until numReceivers).map(receiverId => (receiverId, 0L)).toMap
    }
  }

  def receiverInfo(receiverId: Int): Option[ReceiverInfo] = {
    receiverInfos.get(receiverId)
  }

  def lastCompletedBatch: Option[BatchInfo] = {
    completedaBatchInfos.sortBy(_.batchTime)(Time.ordering).lastOption
  }

  def lastReceivedBatch: Option[BatchInfo] = {
    retainedBatches.lastOption
  }

  private def retainedBatches: Seq[BatchInfo] = synchronized {
    (waitingBatchInfos.values.toSeq ++
      runningBatchInfos.values.toSeq ++ completedaBatchInfos).sortBy(_.batchTime)(Time.ordering)
  }

  private def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(completedaBatchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }
}
