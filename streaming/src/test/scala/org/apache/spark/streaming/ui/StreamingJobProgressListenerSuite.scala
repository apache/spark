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
import org.apache.spark.streaming.{Duration, Time, Milliseconds, TestSuiteBase}

class StreamingJobProgressListenerSuite extends TestSuiteBase with Matchers {

  val input = (1 to 4).map(Seq(_)).toSeq
  val operation = (d: DStream[Int]) => d.map(x => x)

  override def batchDuration: Duration = Milliseconds(100)

  test("onBatchSubmitted, onBatchStarted, onBatchCompleted, " +
    "onReceiverStarted, onReceiverError, onReceiverStopped") {
    val ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)

    val receivedBlockInfo = Map(
      0 -> Array(ReceivedBlockInfo(0, 100, null), ReceivedBlockInfo(0, 200, null)),
      1 -> Array(ReceivedBlockInfo(1, 300, null))
    )

    // onBatchSubmitted
    val batchInfoSubmitted = BatchInfo(Time(1000), receivedBlockInfo, 1000, None, None)
    listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))
    listener.waitingBatches should be (List(batchInfoSubmitted))
    listener.runningBatches should be (Nil)
    listener.retainedCompletedBatches should be (Nil)
    listener.lastCompletedBatch should be (None)
    listener.numUnprocessedBatches should be (1)
    listener.numTotalCompletedBatches should be (0)
    listener.numTotalProcessedRecords should be (0)
    listener.numTotalReceivedRecords should be (0)

    // onBatchStarted
    val batchInfoStarted = BatchInfo(Time(1000), receivedBlockInfo, 1000, Some(2000), None)
    listener.onBatchStarted(StreamingListenerBatchStarted(batchInfoStarted))
    listener.waitingBatches should be (Nil)
    listener.runningBatches should be (List(batchInfoStarted))
    listener.retainedCompletedBatches should be (Nil)
    listener.lastCompletedBatch should be (None)
    listener.numUnprocessedBatches should be (1)
    listener.numTotalCompletedBatches should be (0)
    listener.numTotalProcessedRecords should be (0)
    listener.numTotalReceivedRecords should be (600)

    // onBatchCompleted
    val batchInfoCompleted = BatchInfo(Time(1000), receivedBlockInfo, 1000, Some(2000), None)
    listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    listener.waitingBatches should be (Nil)
    listener.runningBatches should be (Nil)
    listener.retainedCompletedBatches should be (List(batchInfoCompleted))
    listener.lastCompletedBatch should be (Some(batchInfoCompleted))
    listener.numUnprocessedBatches should be (0)
    listener.numTotalCompletedBatches should be (1)
    listener.numTotalProcessedRecords should be (600)
    listener.numTotalReceivedRecords should be (600)

    // onReceiverStarted
    val receiverInfoStarted = ReceiverInfo(0, "test", null, true, "localhost")
    listener.onReceiverStarted(StreamingListenerReceiverStarted(receiverInfoStarted))
    listener.receiverInfo(0) should be (Some(receiverInfoStarted))
    listener.receiverInfo(1) should be (None)

    // onReceiverError
    val receiverInfoError = ReceiverInfo(1, "test", null, true, "localhost")
    listener.onReceiverError(StreamingListenerReceiverError(receiverInfoError))
    listener.receiverInfo(0) should be (Some(receiverInfoStarted))
    listener.receiverInfo(1) should be (Some(receiverInfoError))
    listener.receiverInfo(2) should be (None)

    // onReceiverStopped
    val receiverInfoStopped = ReceiverInfo(2, "test", null, true, "localhost")
    listener.onReceiverStopped(StreamingListenerReceiverStopped(receiverInfoStopped))
    listener.receiverInfo(0) should be (Some(receiverInfoStarted))
    listener.receiverInfo(1) should be (Some(receiverInfoError))
    listener.receiverInfo(2) should be (Some(receiverInfoStopped))
    listener.receiverInfo(3) should be (None)
  }

  test("Remove the old completed batches when exceeding the limit") {
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
}
