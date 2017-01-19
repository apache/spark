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

import java.util.{LinkedHashMap, Map => JMap, Properties}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, Queue}

import org.apache.spark.scheduler._
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.scheduler._

private[spark] class StreamingJobProgressListener(ssc: StreamingContext)
  extends SparkListener with StreamingListener {

  private val waitingBatchUIData = new HashMap[Time, BatchUIData]
  private val runningBatchUIData = new HashMap[Time, BatchUIData]
  private val completedBatchUIData = new Queue[BatchUIData]
  private val batchUIDataLimit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 1000)
  private var totalCompletedBatches = 0L
  private var totalReceivedRecords = 0L
  private var totalProcessedRecords = 0L
  private val receiverInfos = new HashMap[Int, ReceiverInfo]

  private var _startTime = -1L

  // Because onJobStart and onBatchXXX messages are processed in different threads,
  // we may not be able to get the corresponding BatchUIData when receiving onJobStart. So here we
  // cannot use a map of (Time, BatchUIData).
  private[ui] val batchTimeToOutputOpIdSparkJobIdPair =
    new LinkedHashMap[Time, ConcurrentLinkedQueue[OutputOpIdAndSparkJobId]] {
      override def removeEldestEntry(
          p1: JMap.Entry[Time, ConcurrentLinkedQueue[OutputOpIdAndSparkJobId]]): Boolean = {
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
        size() >
          waitingBatchUIData.size + runningBatchUIData.size + completedBatchUIData.size + 10
      }
    }


  val batchDuration = ssc.graph.batchDuration.milliseconds

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted) {
    _startTime = streamingStarted.time
  }

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
      waitingBatchUIData(batchSubmitted.batchInfo.batchTime) =
        BatchUIData(batchSubmitted.batchInfo)
    }
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = synchronized {
    val batchUIData = BatchUIData(batchStarted.batchInfo)
    runningBatchUIData(batchStarted.batchInfo.batchTime) = batchUIData
    waitingBatchUIData.remove(batchStarted.batchInfo.batchTime)

    totalReceivedRecords += batchUIData.numRecords
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    synchronized {
      waitingBatchUIData.remove(batchCompleted.batchInfo.batchTime)
      runningBatchUIData.remove(batchCompleted.batchInfo.batchTime)
      val batchUIData = BatchUIData(batchCompleted.batchInfo)
      completedBatchUIData.enqueue(batchUIData)
      if (completedBatchUIData.size > batchUIDataLimit) {
        val removedBatch = completedBatchUIData.dequeue()
        batchTimeToOutputOpIdSparkJobIdPair.remove(removedBatch.batchTime)
      }
      totalCompletedBatches += 1L

      totalProcessedRecords += batchUIData.numRecords
    }
  }

  override def onOutputOperationStarted(
      outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = synchronized {
    // This method is called after onBatchStarted
    runningBatchUIData(outputOperationStarted.outputOperationInfo.batchTime).
      updateOutputOperationInfo(outputOperationStarted.outputOperationInfo)
  }

  override def onOutputOperationCompleted(
      outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = synchronized {
    // This method is called before onBatchCompleted
    runningBatchUIData(outputOperationCompleted.outputOperationInfo.batchTime).
      updateOutputOperationInfo(outputOperationCompleted.outputOperationInfo)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    getBatchTimeAndOutputOpId(jobStart.properties).foreach { case (batchTime, outputOpId) =>
      var outputOpIdToSparkJobIds = batchTimeToOutputOpIdSparkJobIdPair.get(batchTime)
      if (outputOpIdToSparkJobIds == null) {
        outputOpIdToSparkJobIds = new ConcurrentLinkedQueue[OutputOpIdAndSparkJobId]()
        batchTimeToOutputOpIdSparkJobIdPair.put(batchTime, outputOpIdToSparkJobIds)
      }
      outputOpIdToSparkJobIds.add(OutputOpIdAndSparkJobId(outputOpId, jobStart.jobId))
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

  def startTime: Long = _startTime

  def numReceivers: Int = synchronized {
    receiverInfos.size
  }

  def numActiveReceivers: Int = synchronized {
    receiverInfos.count(_._2.active)
  }

  def numInactiveReceivers: Int = {
    ssc.graph.getNumReceivers - numActiveReceivers
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
    waitingBatchUIData.size + runningBatchUIData.size
  }

  def waitingBatches: Seq[BatchUIData] = synchronized {
    waitingBatchUIData.values.toSeq
  }

  def runningBatches: Seq[BatchUIData] = synchronized {
    runningBatchUIData.values.toSeq
  }

  def retainedCompletedBatches: Seq[BatchUIData] = synchronized {
    completedBatchUIData.toIndexedSeq
  }

  def streamName(streamId: Int): Option[String] = {
    ssc.graph.getInputStreamNameAndID.find(_._2 == streamId).map(_._1)
  }

  /**
   * Return all InputDStream Ids
   */
  def streamIds: Seq[Int] = ssc.graph.getInputStreamNameAndID.map(_._2)

  /**
   * Return all of the record rates for each InputDStream in each batch. The key of the return value
   * is the stream id, and the value is a sequence of batch time with its record rate.
   */
  def receivedRecordRateWithBatchTime: Map[Int, Seq[(Long, Double)]] = synchronized {
    val _retainedBatches = retainedBatches
    val latestBatches = _retainedBatches.map { batchUIData =>
      (batchUIData.batchTime.milliseconds, batchUIData.streamIdToInputInfo.mapValues(_.numRecords))
    }
    streamIds.map { streamId =>
      val recordRates = latestBatches.map {
        case (batchTime, streamIdToNumRecords) =>
          val numRecords = streamIdToNumRecords.getOrElse(streamId, 0L)
          (batchTime, numRecords * 1000.0 / batchDuration)
      }
      (streamId, recordRates)
    }.toMap
  }

  def lastReceivedBatchRecords: Map[Int, Long] = synchronized {
    val lastReceivedBlockInfoOption =
      lastReceivedBatch.map(_.streamIdToInputInfo.mapValues(_.numRecords))
    lastReceivedBlockInfoOption.map { lastReceivedBlockInfo =>
      streamIds.map { streamId =>
        (streamId, lastReceivedBlockInfo.getOrElse(streamId, 0L))
      }.toMap
    }.getOrElse {
      streamIds.map(streamId => (streamId, 0L)).toMap
    }
  }

  def receiverInfo(receiverId: Int): Option[ReceiverInfo] = synchronized {
    receiverInfos.get(receiverId)
  }

  def lastCompletedBatch: Option[BatchUIData] = synchronized {
    completedBatchUIData.sortBy(_.batchTime)(Time.ordering).lastOption
  }

  def lastReceivedBatch: Option[BatchUIData] = synchronized {
    retainedBatches.lastOption
  }

  def retainedBatches: Seq[BatchUIData] = synchronized {
    (waitingBatchUIData.values.toSeq ++
      runningBatchUIData.values.toSeq ++ completedBatchUIData).sortBy(_.batchTime)(Time.ordering)
  }

  def getBatchUIData(batchTime: Time): Option[BatchUIData] = synchronized {
    val batchUIData = waitingBatchUIData.get(batchTime).orElse {
      runningBatchUIData.get(batchTime).orElse {
        completedBatchUIData.find(batch => batch.batchTime == batchTime)
      }
    }
    batchUIData.foreach { _batchUIData =>
      // We use an Iterable rather than explicitly converting to a seq so that updates
      // will propagate
      val outputOpIdToSparkJobIds: Iterable[OutputOpIdAndSparkJobId] =
        Option(batchTimeToOutputOpIdSparkJobIdPair.get(batchTime)).map(_.asScala)
          .getOrElse(Seq.empty)
      _batchUIData.outputOpIdSparkJobIdPairs = outputOpIdToSparkJobIds
    }
    batchUIData
  }
}

private[spark] object StreamingJobProgressListener {
  type SparkJobId = Int
  type OutputOpId = Int
}
