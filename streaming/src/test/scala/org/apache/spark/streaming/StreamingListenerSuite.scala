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

import scala.collection.mutable.ArrayBuffer
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
  override def batchDuration = Milliseconds(100)
  override def actuallyWait = true

  test("batch info reporting") {
    val ssc = setupStreams(input, operation)
    val collector = new BatchInfoCollector
    ssc.addStreamingListener(collector)
    runStreams(ssc, input.size, input.size)
    val batchInfos = collector.batchInfos
    batchInfos should have size 4

    batchInfos.foreach(info => {
      info.schedulingDelay should not be None
      info.processingDelay should not be None
      info.totalDelay should not be None
      info.schedulingDelay.get should be >= 0L
      info.processingDelay.get should be >= 0L
      info.totalDelay.get should be >= 0L
    })

    isInIncreasingOrder(batchInfos.map(_.submissionTime)) should be (true)
    isInIncreasingOrder(batchInfos.map(_.processingStartTime.get)) should be (true)
    isInIncreasingOrder(batchInfos.map(_.processingEndTime.get)) should be (true)
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
  val batchInfos = new ArrayBuffer[BatchInfo]
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    batchInfos += batchCompleted.batchInfo
  }
}

/** Listener that collects information on processed batches */
class ReceiverInfoCollector extends StreamingListener {
  val startedReceiverStreamIds = new ArrayBuffer[Int]
  val stoppedReceiverStreamIds = new ArrayBuffer[Int]()
  val receiverErrors = new ArrayBuffer[(Int, String, String)]()

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
