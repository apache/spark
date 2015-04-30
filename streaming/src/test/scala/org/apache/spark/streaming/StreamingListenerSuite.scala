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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

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

  // To make sure that the processing start and end times in collected
  // information are different for successive batches
  override def batchDuration: Duration = Milliseconds(100)
  override def actuallyWait: Boolean = true

  test("batch info reporting") {
    val ssc = setupStreams(input, operation)
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

    isInIncreasingOrder(batchInfosSubmitted.map(_.submissionTime).toSeq) should be (true)

    // SPARK-6766: processingStartTime of batch info should not be None when starting
    val batchInfosStarted = collector.batchInfosStarted
    batchInfosStarted should have size 4

    batchInfosStarted.foreach(info => {
      info.schedulingDelay should not be None
      info.schedulingDelay.get should be >= 0L
      info.processingDelay should be (None)
      info.totalDelay should be (None)
    })

    isInIncreasingOrder(batchInfosStarted.map(_.submissionTime).toSeq) should be (true)
    isInIncreasingOrder(batchInfosStarted.map(_.processingStartTime.get).toSeq) should be (true)

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

    isInIncreasingOrder(batchInfosCompleted.map(_.submissionTime).toSeq) should be (true)
    isInIncreasingOrder(batchInfosCompleted.map(_.processingStartTime.get).toSeq) should be (true)
    isInIncreasingOrder(batchInfosCompleted.map(_.processingEndTime.get).toSeq) should be (true)
  }

  test("receiver info reporting") {
    val ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD(_.count)

    val collector = new ReceiverInfoCollector
    ssc.addStreamingListener(collector)

    ssc.start()
    try {
      eventually(timeout(2000 millis), interval(20 millis)) {
        collector.startedReceiverStreamIds.size should equal (1)
        collector.startedReceiverStreamIds.peek() should equal (0)
        collector.stoppedReceiverStreamIds should have size 1
        collector.stoppedReceiverStreamIds.peek() should equal (0)
        collector.receiverErrors should have size 1
        collector.receiverErrors.peek()._1 should equal (0)
        collector.receiverErrors.peek()._2 should include ("report error")
        collector.receiverErrors.peek()._3 should include ("report exception")
      }
    } finally {
      ssc.stop()
    }
  }

  /** Check if a sequence of numbers is in increasing order */
  def isInIncreasingOrder(seq: Seq[Long]): Boolean = {
    for(i <- 1 until seq.size) {
      if (seq(i - 1) > seq(i)) return false
    }
    true
  }
}

/** Listener that collects information on processed batches */
class BatchInfoCollector extends StreamingListener {
  val batchInfosCompleted = new ConcurrentLinkedQueue[BatchInfo]
  val batchInfosStarted = new ConcurrentLinkedQueue[BatchInfo]
  val batchInfosSubmitted = new ConcurrentLinkedQueue[BatchInfo]

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    batchInfosSubmitted.add(batchSubmitted.batchInfo)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    batchInfosStarted.add(batchStarted.batchInfo)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    batchInfosCompleted.add(batchCompleted.batchInfo)
  }
}

/** Listener that collects information on processed batches */
class ReceiverInfoCollector extends StreamingListener {
  val startedReceiverStreamIds = new ConcurrentLinkedQueue[Int]
  val stoppedReceiverStreamIds = new ConcurrentLinkedQueue[Int]()
  val receiverErrors = new ConcurrentLinkedQueue[(Int, String, String)]()

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    startedReceiverStreamIds.add(receiverStarted.receiverInfo.streamId)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    stoppedReceiverStreamIds.add(receiverStopped.receiverInfo.streamId)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    receiverErrors.add((receiverError.receiverInfo.streamId,
      receiverError.receiverInfo.lastErrorMessage, receiverError.receiverInfo.lastError))
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
