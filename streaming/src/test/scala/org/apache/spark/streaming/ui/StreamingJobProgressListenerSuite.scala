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

import org.scalatest.Matchers

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.{Time, Milliseconds, TestSuiteBase}

class StreamingJobProgressListenerSuite extends TestSuiteBase with Matchers {

  val input = (1 to 4).map(Seq(_)).toSeq
  val operation = (d: DStream[Int]) => d.map(x => x)

  override def batchDuration = Milliseconds(100)

  test("onBatchSubmitted") {
    val ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)

    val batchInfoSubmitted = BatchInfo(Time(1000), Map(), 1000, None, None)
    listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))

    listener.waitingBatches should be(List(batchInfoSubmitted))
  }

  test("onBatchStarted") {
    val ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)

    val batchInfoSubmitted = BatchInfo(Time(1000), Map(), 1000, None, None)
    listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))

    val receivedBlockInfo = Map(
      0 -> Array(ReceivedBlockInfo(0, 100, null), ReceivedBlockInfo(0, 200, null)),
      1 -> Array(ReceivedBlockInfo(1, 300, null))
    )
    val batchInfoStarted = BatchInfo(Time(1000), receivedBlockInfo, 1000, Some(2000), None)
    listener.onBatchStarted(StreamingListenerBatchStarted(batchInfoStarted))

    listener.runningBatches should be(List(batchInfoStarted))
    listener.waitingBatches should be(Nil)
    listener.numTotalReceivedRecords should be(600)
  }

  test("onBatchCompleted") {
    val ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)

    val batchInfoSubmitted = BatchInfo(Time(1000), Map(), 1000, None, None)
    listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))

    val receivedBlockInfo = Map(
      0 -> Array(ReceivedBlockInfo(0, 100, null), ReceivedBlockInfo(0, 200, null)),
      1 -> Array(ReceivedBlockInfo(1, 300, null))
    )
    val batchInfoStarted = BatchInfo(Time(1000), receivedBlockInfo, 1000, Some(2000), None)
    listener.onBatchStarted(StreamingListenerBatchStarted(batchInfoStarted))

    val batchInfoCompleted = BatchInfo(Time(1000), receivedBlockInfo, 1000, Some(2000), None)
    listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))

    listener.runningBatches should be (Nil)
    listener.waitingBatches should be (Nil)
    listener.lastCompletedBatch should be (Some(batchInfoCompleted))
    listener.retainedCompletedBatches should be (List(batchInfoCompleted))
    listener.numTotalCompletedBatches should be (1)
    listener.numTotalProcessedRecords should be (600)
    listener.numTotalReceivedRecords should be (600)
  }

  test("retain completed batch") {
    val ssc = setupStreams(input, operation)
    val limit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 100)
    val listener = new StreamingJobProgressListener(ssc)

    val receivedBlockInfo = Map(
      0 -> Array(ReceivedBlockInfo(0, 100, null), ReceivedBlockInfo(0, 200, null)),
      1 -> Array(ReceivedBlockInfo(1, 300, null))
    )
    val batchInfoCompleted = BatchInfo(Time(1000), receivedBlockInfo, 1000, Some(2000), None)

    for(_ <- 0 until (limit + 10)) {
      listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    }

    listener.retainedCompletedBatches.size should be (limit)
    listener.numTotalCompletedBatches should be(limit + 10)
  }

  test("onReceiverStarted") {
    val ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)

    val receiverInfo = ReceiverInfo(0, "test", null, true, "localhost")
    listener.onReceiverStarted(StreamingListenerReceiverStarted(receiverInfo))

    listener.receiverInfo(0) should be (Some(receiverInfo))
    listener.receiverInfo(1) should be (None)
  }

  test("onReceiverError") {
    val ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)

    val receiverInfo = ReceiverInfo(0, "test", null, true, "localhost")
    listener.onReceiverError(StreamingListenerReceiverError(receiverInfo))

    listener.receiverInfo(0) should be (Some(receiverInfo))
    listener.receiverInfo(1) should be (None)
  }

  test("onReceiverStopped") {
    val ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)

    val receiverInfo = ReceiverInfo(0, "test", null, true, "localhost")
    listener.onReceiverStopped(StreamingListenerReceiverStopped(receiverInfo))

    listener.receiverInfo(0) should be (Some(receiverInfo))
    listener.receiverInfo(1) should be (None)
  }

}
