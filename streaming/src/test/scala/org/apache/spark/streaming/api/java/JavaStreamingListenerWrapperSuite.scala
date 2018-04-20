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

package org.apache.spark.streaming.api.java

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler._

class JavaStreamingListenerWrapperSuite extends SparkFunSuite {

  test("basic") {
    val listener = new TestJavaStreamingListener()
    val listenerWrapper = new JavaStreamingListenerWrapper(listener)

    val streamingStarted = StreamingListenerStreamingStarted(1000L)
    listenerWrapper.onStreamingStarted(streamingStarted)
    assert(listener.streamingStarted.time === streamingStarted.time)

    val receiverStarted = StreamingListenerReceiverStarted(ReceiverInfo(
      streamId = 2,
      name = "test",
      active = true,
      location = "localhost",
      executorId = "1"
    ))
    listenerWrapper.onReceiverStarted(receiverStarted)
    assertReceiverInfo(listener.receiverStarted.receiverInfo, receiverStarted.receiverInfo)

    val receiverStopped = StreamingListenerReceiverStopped(ReceiverInfo(
      streamId = 2,
      name = "test",
      active = false,
      location = "localhost",
      executorId = "1"
    ))
    listenerWrapper.onReceiverStopped(receiverStopped)
    assertReceiverInfo(listener.receiverStopped.receiverInfo, receiverStopped.receiverInfo)

    val receiverError = StreamingListenerReceiverError(ReceiverInfo(
      streamId = 2,
      name = "test",
      active = false,
      location = "localhost",
      executorId = "1",
      lastErrorMessage = "failed",
      lastError = "failed",
      lastErrorTime = System.currentTimeMillis()
    ))
    listenerWrapper.onReceiverError(receiverError)
    assertReceiverInfo(listener.receiverError.receiverInfo, receiverError.receiverInfo)

    val batchSubmitted = StreamingListenerBatchSubmitted(BatchInfo(
      batchTime = Time(1000L),
      streamIdToInputInfo = Map(
        0 -> StreamInputInfo(
          inputStreamId = 0,
          numRecords = 1000,
          metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "receiver1")),
        1 -> StreamInputInfo(
          inputStreamId = 1,
          numRecords = 2000,
          metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "receiver2"))),
      submissionTime = 1001L,
      None,
      None,
      outputOperationInfos = Map(
        0 -> OutputOperationInfo(
          batchTime = Time(1000L),
          id = 0,
          name = "op1",
          description = "operation1",
          startTime = None,
          endTime = None,
          failureReason = None),
        1 -> OutputOperationInfo(
          batchTime = Time(1000L),
          id = 1,
          name = "op2",
          description = "operation2",
          startTime = None,
          endTime = None,
          failureReason = None))
    ))
    listenerWrapper.onBatchSubmitted(batchSubmitted)
    assertBatchInfo(listener.batchSubmitted.batchInfo, batchSubmitted.batchInfo)

    val batchStarted = StreamingListenerBatchStarted(BatchInfo(
      batchTime = Time(1000L),
      streamIdToInputInfo = Map(
        0 -> StreamInputInfo(
          inputStreamId = 0,
          numRecords = 1000,
          metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "receiver1")),
        1 -> StreamInputInfo(
          inputStreamId = 1,
          numRecords = 2000,
          metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "receiver2"))),
      submissionTime = 1001L,
      Some(1002L),
      None,
      outputOperationInfos = Map(
        0 -> OutputOperationInfo(
          batchTime = Time(1000L),
          id = 0,
          name = "op1",
          description = "operation1",
          startTime = Some(1003L),
          endTime = None,
          failureReason = None),
        1 -> OutputOperationInfo(
          batchTime = Time(1000L),
          id = 1,
          name = "op2",
          description = "operation2",
          startTime = Some(1005L),
          endTime = None,
          failureReason = None))
    ))
    listenerWrapper.onBatchStarted(batchStarted)
    assertBatchInfo(listener.batchStarted.batchInfo, batchStarted.batchInfo)

    val batchCompleted = StreamingListenerBatchCompleted(BatchInfo(
      batchTime = Time(1000L),
      streamIdToInputInfo = Map(
        0 -> StreamInputInfo(
          inputStreamId = 0,
          numRecords = 1000,
          metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "receiver1")),
        1 -> StreamInputInfo(
          inputStreamId = 1,
          numRecords = 2000,
          metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "receiver2"))),
      submissionTime = 1001L,
      Some(1002L),
      Some(1010L),
      outputOperationInfos = Map(
        0 -> OutputOperationInfo(
          batchTime = Time(1000L),
          id = 0,
          name = "op1",
          description = "operation1",
          startTime = Some(1003L),
          endTime = Some(1004L),
          failureReason = None),
        1 -> OutputOperationInfo(
          batchTime = Time(1000L),
          id = 1,
          name = "op2",
          description = "operation2",
          startTime = Some(1005L),
          endTime = Some(1010L),
          failureReason = None))
    ))
    listenerWrapper.onBatchCompleted(batchCompleted)
    assertBatchInfo(listener.batchCompleted.batchInfo, batchCompleted.batchInfo)

    val outputOperationStarted = StreamingListenerOutputOperationStarted(OutputOperationInfo(
      batchTime = Time(1000L),
      id = 0,
      name = "op1",
      description = "operation1",
      startTime = Some(1003L),
      endTime = None,
      failureReason = None
    ))
    listenerWrapper.onOutputOperationStarted(outputOperationStarted)
    assertOutputOperationInfo(listener.outputOperationStarted.outputOperationInfo,
      outputOperationStarted.outputOperationInfo)

    val outputOperationCompleted = StreamingListenerOutputOperationCompleted(OutputOperationInfo(
      batchTime = Time(1000L),
      id = 0,
      name = "op1",
      description = "operation1",
      startTime = Some(1003L),
      endTime = Some(1004L),
      failureReason = None
    ))
    listenerWrapper.onOutputOperationCompleted(outputOperationCompleted)
    assertOutputOperationInfo(listener.outputOperationCompleted.outputOperationInfo,
      outputOperationCompleted.outputOperationInfo)
  }

  private def assertReceiverInfo(
      javaReceiverInfo: JavaReceiverInfo, receiverInfo: ReceiverInfo): Unit = {
    assert(javaReceiverInfo.streamId === receiverInfo.streamId)
    assert(javaReceiverInfo.name === receiverInfo.name)
    assert(javaReceiverInfo.active === receiverInfo.active)
    assert(javaReceiverInfo.location === receiverInfo.location)
    assert(javaReceiverInfo.executorId === receiverInfo.executorId)
    assert(javaReceiverInfo.lastErrorMessage === receiverInfo.lastErrorMessage)
    assert(javaReceiverInfo.lastError === receiverInfo.lastError)
    assert(javaReceiverInfo.lastErrorTime === receiverInfo.lastErrorTime)
  }

  private def assertBatchInfo(javaBatchInfo: JavaBatchInfo, batchInfo: BatchInfo): Unit = {
    assert(javaBatchInfo.batchTime === batchInfo.batchTime)
    assert(javaBatchInfo.streamIdToInputInfo.size === batchInfo.streamIdToInputInfo.size)
    batchInfo.streamIdToInputInfo.foreach { case (streamId, streamInputInfo) =>
      assertStreamingInfo(javaBatchInfo.streamIdToInputInfo.get(streamId), streamInputInfo)
    }
    assert(javaBatchInfo.submissionTime === batchInfo.submissionTime)
    assert(javaBatchInfo.processingStartTime === batchInfo.processingStartTime.getOrElse(-1))
    assert(javaBatchInfo.processingEndTime === batchInfo.processingEndTime.getOrElse(-1))
    assert(javaBatchInfo.schedulingDelay === batchInfo.schedulingDelay.getOrElse(-1))
    assert(javaBatchInfo.processingDelay === batchInfo.processingDelay.getOrElse(-1))
    assert(javaBatchInfo.totalDelay === batchInfo.totalDelay.getOrElse(-1))
    assert(javaBatchInfo.numRecords === batchInfo.numRecords)
    assert(javaBatchInfo.outputOperationInfos.size === batchInfo.outputOperationInfos.size)
    batchInfo.outputOperationInfos.foreach { case (outputOperationId, outputOperationInfo) =>
      assertOutputOperationInfo(
        javaBatchInfo.outputOperationInfos.get(outputOperationId), outputOperationInfo)
    }
  }

  private def assertStreamingInfo(
      javaStreamInputInfo: JavaStreamInputInfo, streamInputInfo: StreamInputInfo): Unit = {
    assert(javaStreamInputInfo.inputStreamId === streamInputInfo.inputStreamId)
    assert(javaStreamInputInfo.numRecords === streamInputInfo.numRecords)
    assert(javaStreamInputInfo.metadata === streamInputInfo.metadata.asJava)
    assert(javaStreamInputInfo.metadataDescription === streamInputInfo.metadataDescription.orNull)
  }

  private def assertOutputOperationInfo(
      javaOutputOperationInfo: JavaOutputOperationInfo,
      outputOperationInfo: OutputOperationInfo): Unit = {
    assert(javaOutputOperationInfo.batchTime === outputOperationInfo.batchTime)
    assert(javaOutputOperationInfo.id === outputOperationInfo.id)
    assert(javaOutputOperationInfo.name === outputOperationInfo.name)
    assert(javaOutputOperationInfo.description === outputOperationInfo.description)
    assert(javaOutputOperationInfo.startTime === outputOperationInfo.startTime.getOrElse(-1))
    assert(javaOutputOperationInfo.endTime === outputOperationInfo.endTime.getOrElse(-1))
    assert(javaOutputOperationInfo.failureReason === outputOperationInfo.failureReason.orNull)
  }
}

class TestJavaStreamingListener extends JavaStreamingListener {

  var streamingStarted: JavaStreamingListenerStreamingStarted = null
  var receiverStarted: JavaStreamingListenerReceiverStarted = null
  var receiverError: JavaStreamingListenerReceiverError = null
  var receiverStopped: JavaStreamingListenerReceiverStopped = null
  var batchSubmitted: JavaStreamingListenerBatchSubmitted = null
  var batchStarted: JavaStreamingListenerBatchStarted = null
  var batchCompleted: JavaStreamingListenerBatchCompleted = null
  var outputOperationStarted: JavaStreamingListenerOutputOperationStarted = null
  var outputOperationCompleted: JavaStreamingListenerOutputOperationCompleted = null

  override def onStreamingStarted(streamingStarted: JavaStreamingListenerStreamingStarted): Unit = {
    this.streamingStarted = streamingStarted
  }

  override def onReceiverStarted(receiverStarted: JavaStreamingListenerReceiverStarted): Unit = {
    this.receiverStarted = receiverStarted
  }

  override def onReceiverError(receiverError: JavaStreamingListenerReceiverError): Unit = {
    this.receiverError = receiverError
  }

  override def onReceiverStopped(receiverStopped: JavaStreamingListenerReceiverStopped): Unit = {
    this.receiverStopped = receiverStopped
  }

  override def onBatchSubmitted(batchSubmitted: JavaStreamingListenerBatchSubmitted): Unit = {
    this.batchSubmitted = batchSubmitted
  }

  override def onBatchStarted(batchStarted: JavaStreamingListenerBatchStarted): Unit = {
    this.batchStarted = batchStarted
  }

  override def onBatchCompleted(batchCompleted: JavaStreamingListenerBatchCompleted): Unit = {
    this.batchCompleted = batchCompleted
  }

  override def onOutputOperationStarted(
      outputOperationStarted: JavaStreamingListenerOutputOperationStarted): Unit = {
    this.outputOperationStarted = outputOperationStarted
  }

  override def onOutputOperationCompleted(
      outputOperationCompleted: JavaStreamingListenerOutputOperationCompleted): Unit = {
    this.outputOperationCompleted = outputOperationCompleted
  }
}
