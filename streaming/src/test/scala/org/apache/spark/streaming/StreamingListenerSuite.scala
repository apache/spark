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

package org.apache.spark.streaming

import scala.collection.mutable.{ArrayBuffer, HashMap, SynchronizedBuffer, SynchronizedMap}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.SparkException
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler._

import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._
import org.apache.spark.Logging

class StreamingListenerSuite extends TestSuiteBase with Matchers {

  val input = (1 to 4).map(Seq(_)).toSeq
  val operation = (d: DStream[Int]) => d.map(x => x)

  var ssc: StreamingContext = _

  override def afterFunction() {
    super.afterFunction()
    if (ssc != null) {
      ssc.stop()
    }
  }

  // To make sure that the processing start and end times in collected
  // information are different for successive batches
  override def batchDuration: Duration = Milliseconds(100)
  override def actuallyWait: Boolean = true

  test("batch info reporting") {
    ssc = setupStreams(input, operation)
    val collector = new BatchInfoCollector
    ssc.addStreamingListener(collector)
    runStreams(ssc, input.size, input.size)

    // SPARK-6766: batch info should be submitted
    val batchInfosSubmitted = collector.batchInfosSubmitted
    batchInfosSubmitted should have size 4

    batchInfosSubmitted.foreach(info => {
      info.schedulingDelay should be (None)
      info.processingDelay should be (None)
      info.totalDelay should be (None)
    })

    batchInfosSubmitted.foreach { info =>
      info.numRecords should be (1L)
      info.streamIdToInputInfo should be (Map(0 -> StreamInputInfo(0, 1L)))
    }

    isInIncreasingOrder(batchInfosSubmitted.map(_.submissionTime)) should be (true)

    // SPARK-6766: processingStartTime of batch info should not be None when starting
    val batchInfosStarted = collector.batchInfosStarted
    batchInfosStarted should have size 4

    batchInfosStarted.foreach(info => {
      info.schedulingDelay should not be None
      info.schedulingDelay.get should be >= 0L
      info.processingDelay should be (None)
      info.totalDelay should be (None)
    })

    batchInfosStarted.foreach { info =>
      info.numRecords should be (1L)
      info.streamIdToInputInfo should be (Map(0 -> StreamInputInfo(0, 1L)))
    }

    isInIncreasingOrder(batchInfosStarted.map(_.submissionTime)) should be (true)
    isInIncreasingOrder(batchInfosStarted.map(_.processingStartTime.get)) should be (true)

    // test onBatchCompleted
    val batchInfosCompleted = collector.batchInfosCompleted
    batchInfosCompleted should have size 4

    batchInfosCompleted.foreach(info => {
      info.schedulingDelay should not be None
      info.processingDelay should not be None
      info.totalDelay should not be None
      info.schedulingDelay.get should be >= 0L
      info.processingDelay.get should be >= 0L
      info.totalDelay.get should be >= 0L
    })

    batchInfosCompleted.foreach { info =>
      info.numRecords should be (1L)
      info.streamIdToInputInfo should be (Map(0 -> StreamInputInfo(0, 1L)))
    }

    isInIncreasingOrder(batchInfosCompleted.map(_.submissionTime)) should be (true)
    isInIncreasingOrder(batchInfosCompleted.map(_.processingStartTime.get)) should be (true)
    isInIncreasingOrder(batchInfosCompleted.map(_.processingEndTime.get)) should be (true)
  }

  test("receiver info reporting") {
    ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD(_.count)

    val collector = new ReceiverInfoCollector
    ssc.addStreamingListener(collector)

    ssc.start()
    try {
      eventually(timeout(30 seconds), interval(20 millis)) {
        collector.startedReceiverStreamIds.size should equal (1)
        collector.startedReceiverStreamIds(0) should equal (0)
        collector.stoppedReceiverStreamIds should have size 1
        collector.stoppedReceiverStreamIds(0) should equal (0)
        collector.receiverErrors should have size 1
        collector.receiverErrors(0)._1 should equal (0)
        collector.receiverErrors(0)._2 should include ("report error")
        collector.receiverErrors(0)._3 should include ("report exception")
      }
    } finally {
      ssc.stop()
    }
  }

  test("output operation reporting") {
    ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD(_.count())
    inputStream.foreachRDD(_.collect())
    inputStream.foreachRDD(_.count())

    val collector = new OutputOperationInfoCollector
    ssc.addStreamingListener(collector)

    ssc.start()
    try {
      eventually(timeout(30 seconds), interval(20 millis)) {
        collector.startedOutputOperationIds.take(3) should be (Seq(0, 1, 2))
        collector.completedOutputOperationIds.take(3) should be (Seq(0, 1, 2))
      }
    } finally {
      ssc.stop()
    }
  }

  test("don't call ssc.stop in listener") {
    ssc = new StreamingContext("local[2]", "ssc", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD(_.count)

    startStreamingContextAndCallStop(ssc)
  }

  test("onBatchCompleted with successful batch") {
    ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD(_.count)

    val failureReasons = startStreamingContextAndCollectFailureReasons(ssc)
    assert(failureReasons != null && failureReasons.isEmpty,
      "A successful batch should not set errorMessage")
  }

  test("onBatchCompleted with failed batch and one failed job") {
    ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD { _ =>
      throw new RuntimeException("This is a failed job")
    }

    // Check if failureReasons contains the correct error message
    val failureReasons = startStreamingContextAndCollectFailureReasons(ssc, isFailed = true)
    assert(failureReasons != null)
    assert(failureReasons.size === 1)
    assert(failureReasons.contains(0))
    assert(failureReasons(0).contains("This is a failed job"))
  }

  test("onBatchCompleted with failed batch and multiple failed jobs") {
    ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD { _ =>
      throw new RuntimeException("This is a failed job")
    }
    inputStream.foreachRDD { _ =>
      throw new RuntimeException("This is another failed job")
    }

    // Check if failureReasons contains the correct error messages
    val failureReasons =
      startStreamingContextAndCollectFailureReasons(ssc, isFailed = true)
    assert(failureReasons != null)
    assert(failureReasons.size === 2)
    assert(failureReasons.contains(0))
    assert(failureReasons.contains(1))
    assert(failureReasons(0).contains("This is a failed job"))
    assert(failureReasons(1).contains("This is another failed job"))
  }

  private def startStreamingContextAndCallStop(_ssc: StreamingContext): Unit = {
    val contextStoppingCollector = new StreamingContextStoppingCollector(_ssc)
    _ssc.addStreamingListener(contextStoppingCollector)
    val batchCounter = new BatchCounter(_ssc)
    _ssc.start()
    // Make sure running at least one batch
    if (!batchCounter.waitUntilBatchesCompleted(expectedNumCompletedBatches = 1, timeout = 10000)) {
      fail("The first batch cannot complete in 10 seconds")
    }
    // When reaching here, we can make sure `StreamingContextStoppingCollector` won't call
    // `ssc.stop()`, so it's safe to call `_ssc.stop()` now.
    _ssc.stop()
    assert(contextStoppingCollector.sparkExSeen)
  }

  private def startStreamingContextAndCollectFailureReasons(
      _ssc: StreamingContext, isFailed: Boolean = false): Map[Int, String] = {
    val failureReasonsCollector = new FailureReasonsCollector()
    _ssc.addStreamingListener(failureReasonsCollector)
    val batchCounter = new BatchCounter(_ssc)
    _ssc.start()
    // Make sure running at least one batch
    batchCounter.waitUntilBatchesCompleted(expectedNumCompletedBatches = 1, timeout = 10000)
    if (isFailed) {
      intercept[RuntimeException] {
        _ssc.awaitTerminationOrTimeout(10000)
      }
    }
    _ssc.stop()
    failureReasonsCollector.failureReasons.toMap
  }

  /** Check if a sequence of numbers is in increasing order */
  def isInIncreasingOrder(seq: Seq[Long]): Boolean = {
    for (i <- 1 until seq.size) {
      if (seq(i - 1) > seq(i)) {
        return false
      }
    }
    true
  }
}

/** Listener that collects information on processed batches */
class BatchInfoCollector extends StreamingListener {
  val batchInfosCompleted = new ArrayBuffer[BatchInfo] with SynchronizedBuffer[BatchInfo]
  val batchInfosStarted = new ArrayBuffer[BatchInfo] with SynchronizedBuffer[BatchInfo]
  val batchInfosSubmitted = new ArrayBuffer[BatchInfo] with SynchronizedBuffer[BatchInfo]

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    batchInfosSubmitted += batchSubmitted.batchInfo
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    batchInfosStarted += batchStarted.batchInfo
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    batchInfosCompleted += batchCompleted.batchInfo
  }
}

/** Listener that collects information on processed batches */
class ReceiverInfoCollector extends StreamingListener {
  val startedReceiverStreamIds = new ArrayBuffer[Int] with SynchronizedBuffer[Int]
  val stoppedReceiverStreamIds = new ArrayBuffer[Int] with SynchronizedBuffer[Int]
  val receiverErrors =
    new ArrayBuffer[(Int, String, String)] with SynchronizedBuffer[(Int, String, String)]

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    startedReceiverStreamIds += receiverStarted.receiverInfo.streamId
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    stoppedReceiverStreamIds += receiverStopped.receiverInfo.streamId
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    receiverErrors += ((receiverError.receiverInfo.streamId,
      receiverError.receiverInfo.lastErrorMessage, receiverError.receiverInfo.lastError))
  }
}

/** Listener that collects information on processed output operations */
class OutputOperationInfoCollector extends StreamingListener {
  val startedOutputOperationIds = new ArrayBuffer[Int] with SynchronizedBuffer[Int]
  val completedOutputOperationIds = new ArrayBuffer[Int] with SynchronizedBuffer[Int]

  override def onOutputOperationStarted(
      outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    startedOutputOperationIds += outputOperationStarted.outputOperationInfo.id
  }

  override def onOutputOperationCompleted(
      outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    completedOutputOperationIds += outputOperationCompleted.outputOperationInfo.id
  }
}

class StreamingListenerSuiteReceiver extends Receiver[Any](StorageLevel.MEMORY_ONLY) with Logging {
  def onStart() {
    Future {
      logInfo("Started receiver and sleeping")
      Thread.sleep(10)
      logInfo("Reporting error and sleeping")
      reportError("test report error", new Exception("test report exception"))
      Thread.sleep(10)
      logInfo("Stopping")
      stop("test stop error")
    }
  }
  def onStop() { }
}

/**
 * A StreamingListener that saves all latest `failureReasons` in a batch.
 */
class FailureReasonsCollector extends StreamingListener {

  val failureReasons = new HashMap[Int, String] with SynchronizedMap[Int, String]

  override def onOutputOperationCompleted(
      outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    outputOperationCompleted.outputOperationInfo.failureReason.foreach { f =>
      failureReasons(outputOperationCompleted.outputOperationInfo.id) = f
    }
  }
}
/**
 * A StreamingListener that calls StreamingContext.stop().
 */
class StreamingContextStoppingCollector(val ssc: StreamingContext) extends StreamingListener {
  @volatile var sparkExSeen = false

  private var isFirstBatch = true

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    if (isFirstBatch) {
      // We should only call `ssc.stop()` in the first batch. Otherwise, it's possible that the main
      // thread is calling `ssc.stop()`, while StreamingContextStoppingCollector is also calling
      // `ssc.stop()` in the listener thread, which becomes a dead-lock.
      isFirstBatch = false
      try {
        ssc.stop()
      } catch {
        case se: SparkException =>
          sparkExSeen = true
      }
    }
  }
}
