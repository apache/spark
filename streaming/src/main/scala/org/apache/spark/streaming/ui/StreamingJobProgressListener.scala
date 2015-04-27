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

import java.util.LinkedHashMap
import java.util.{Map => JMap}
import java.util.Properties

import scala.collection.mutable.{ArrayBuffer, Queue, HashMap}

import org.apache.spark.scheduler._
import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted
import org.apache.spark.streaming.scheduler.BatchInfo
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted
import org.apache.spark.util.Distribution


private[streaming] class StreamingJobProgressListener(ssc: StreamingContext)
  extends StreamingListener with SparkListener {

  import StreamingJobProgressListener._

  private val waitingBatchInfos = new HashMap[Time, BatchInfo]
  private val runningBatchInfos = new HashMap[Time, BatchInfo]
  private val completedBatchInfos = new Queue[BatchInfo]
  private val batchInfoLimit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 100)
  private var totalCompletedBatches = 0L
  private var totalReceivedRecords = 0L
  private var totalProcessedRecords = 0L
  private val receiverInfos = new HashMap[Int, ReceiverInfo]

  // Because onJobStart and onBatchXXX messages are processed in different threads,
  // we may not be able to get the corresponding BatchInfo when receiving onJobStart. So here we
  // cannot use a map of (Time, BatchUIData).
  private[ui] val batchTimeToOutputOpIdToSparkJobIds =
    new LinkedHashMap[Time, OutputOpIdToSparkJobIds] {
      override def removeEldestEntry(p1: JMap.Entry[Time, OutputOpIdToSparkJobIds]): Boolean = {
        // If a lot of "onBatchCompleted"s happen before "onJobStart" (image if
        // SparkContext.listenerBus is very slow), "batchTimeToOutputOpIdToSparkJobIds"
        // may add some information for a removed batch when processing "onJobStart". It will be a
        // memory leak.
        //
        // To avoid the memory leak, we control the size of "batchTimeToOutputOpIdToSparkJobIds" and
        // evict the eldest one.
        //
        // Note: if "onJobStart" happens before "onBatchSubmitted", the size of
        // "batchTimeToOutputOpIdToSparkJobIds" may be greater than the number of the retained
        // batches temporarily, so here we use "10" to handle such case. This is not a perfect
        // solution, but at least it can handle most of cases.
        size() > waitingBatchInfos.size + runningBatchInfos.size + completedBatchInfos.size + 10
      }
    }


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

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    synchronized {
      waitingBatchInfos(batchSubmitted.batchInfo.batchTime) = batchSubmitted.batchInfo
    }
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = synchronized {
    runningBatchInfos(batchStarted.batchInfo.batchTime) = batchStarted.batchInfo
    waitingBatchInfos.remove(batchStarted.batchInfo.batchTime)

    totalReceivedRecords += batchStarted.batchInfo.numRecords
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    synchronized {
      waitingBatchInfos.remove(batchCompleted.batchInfo.batchTime)
      runningBatchInfos.remove(batchCompleted.batchInfo.batchTime)
      completedBatchInfos.enqueue(batchCompleted.batchInfo)
      if (completedBatchInfos.size > batchInfoLimit) {
        val removedBatch = completedBatchInfos.dequeue()
        batchTimeToOutputOpIdToSparkJobIds.remove(removedBatch.batchTime)
      }
      totalCompletedBatches += 1L

      totalProcessedRecords += batchCompleted.batchInfo.numRecords
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    getBatchTimeAndOutputOpId(jobStart.properties).foreach { case (batchTime, outputOpId) =>
      var outputOpIdToSparkJobIds = batchTimeToOutputOpIdToSparkJobIds.get(batchTime)
      if (outputOpIdToSparkJobIds == null) {
        outputOpIdToSparkJobIds = new OutputOpIdToSparkJobIds()
        batchTimeToOutputOpIdToSparkJobIds.put(batchTime, outputOpIdToSparkJobIds)
      }
      outputOpIdToSparkJobIds.getOrElseUpdate(outputOpId, ArrayBuffer()) += jobStart.jobId
    }
  }

  private def getBatchTimeAndOutputOpId(properties: Properties): Option[(Time, Int)] = {
    val batchTime = properties.getProperty(JobScheduler.BATCH_TIME_PROPERTY_KEY)
    if (batchTime == null) {
      // Not submitted from JobScheduler
      None
    } else {
      val outputOpId = properties.getProperty(JobScheduler.OUTPUT_OP_ID_PROPERTY_KEY)
      assert(outputOpId != null)
      Some(Time(batchTime.toLong) -> outputOpId.toInt)
    }
  }

  def numReceivers: Int = ssc.graph.getReceiverInputStreams().size

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
    completedBatchInfos.toSeq
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

  def lastReceivedBatchRecords: Map[Int, Long] = synchronized {
    val lastReceivedBlockInfoOption = lastReceivedBatch.map(_.receivedBlockInfo)
    lastReceivedBlockInfoOption.map { lastReceivedBlockInfo =>
      (0 until numReceivers).map { receiverId =>
        (receiverId, lastReceivedBlockInfo(receiverId).map(_.numRecords).sum)
      }.toMap
    }.getOrElse {
      (0 until numReceivers).map(receiverId => (receiverId, 0L)).toMap
    }
  }

  def receiverInfo(receiverId: Int): Option[ReceiverInfo] = synchronized {
    receiverInfos.get(receiverId)
  }

  def lastCompletedBatch: Option[BatchInfo] = synchronized {
    completedBatchInfos.sortBy(_.batchTime)(Time.ordering).lastOption
  }

  def lastReceivedBatch: Option[BatchInfo] = synchronized {
    retainedBatches.lastOption
  }

  private def retainedBatches: Seq[BatchInfo] = {
    (waitingBatchInfos.values.toSeq ++
      runningBatchInfos.values.toSeq ++ completedBatchInfos).sortBy(_.batchTime)(Time.ordering)
  }

  private def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(completedBatchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }

  private def getBatchInfo(batchTime: Time): Option[BatchInfo] = {
    waitingBatchInfos.get(batchTime).orElse {
      runningBatchInfos.get(batchTime).orElse {
        completedBatchInfos.find(batch => batch.batchTime == batchTime)
      }
    }
  }

  def getBatchUIData(batchTime: Time): Option[BatchUIData] = synchronized {
    for (batchInfo <- getBatchInfo(batchTime)) yield {
      // outputOpIdToSparkJobIds is a sorted copy of the original one so that the caller can feel
      // free to use the data in BatchUIData.
      val outputOpIdToSparkJobIds = Option(batchTimeToOutputOpIdToSparkJobIds.get(batchTime)).
        getOrElse(Map.empty).toSeq.sortWith(_._1 < _._1). // sorted by OutputOpId
        map { case (outputOpId, jobs) =>
          (outputOpId, jobs.sorted.toSeq) // sort JobIds for each OutputOpId
        }
      BatchUIData(batchInfo, outputOpIdToSparkJobIds)
    }
  }
}

private[streaming] object StreamingJobProgressListener {
  type SparkJobId = Int
  type OutputOpId = Int
  private type OutputOpIdToSparkJobIds = HashMap[OutputOpId, ArrayBuffer[SparkJobId]]
}
