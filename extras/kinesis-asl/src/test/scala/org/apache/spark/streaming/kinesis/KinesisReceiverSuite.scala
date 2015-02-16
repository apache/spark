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
package org.apache.spark.streaming.kinesis

import java.nio.ByteBuffer

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.streaming.util.Clock
import org.apache.spark.streaming.util.ManualClock

import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

/**
 * Suite of Kinesis streaming receiver tests focusing mostly on the KinesisRecordProcessor
 */
class KinesisReceiverSuite extends TestSuiteBase with Matchers with BeforeAndAfter
    with MockitoSugar {

  val app = "TestKinesisReceiver"
  val stream = "mySparkStream"
  val endpoint = "endpoint-url"
  val workerId = "dummyWorkerId"
  val shardId = "dummyShardId"

  val record1 = new Record()
  record1.setData(ByteBuffer.wrap("Spark In Action".getBytes()))
  val record2 = new Record()
  record2.setData(ByteBuffer.wrap("Learning Spark".getBytes()))
  val batch = List[Record](record1, record2)

  var receiverMock: KinesisReceiver = _
  var checkpointerMock: IRecordProcessorCheckpointer = _
  var checkpointClockMock: ManualClock = _
  var checkpointStateMock: KinesisCheckpointState = _
  var currentClockMock: Clock = _

  override def beforeFunction() = {
    receiverMock = mock[KinesisReceiver]
    checkpointerMock = mock[IRecordProcessorCheckpointer]
    checkpointClockMock = mock[ManualClock]
    checkpointStateMock = mock[KinesisCheckpointState]
    currentClockMock = mock[Clock]
  }

  override def afterFunction(): Unit = {
    super.afterFunction()
    // Since this suite was originally written using EasyMock, add this to preserve the old
    // mocking semantics (see SPARK-5735 for more details)
    verifyNoMoreInteractions(receiverMock, checkpointerMock, checkpointClockMock,
      checkpointStateMock, currentClockMock)
  }

  test("kinesis utils api") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    // Tests the API, does not actually test data receiving
    val kinesisStream = KinesisUtils.createStream(ssc, "mySparkStream",
      "https://kinesis.us-west-2.amazonaws.com", Seconds(2),
      InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2);
    ssc.stop()
  }

  test("process records including store and checkpoint") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(checkpointStateMock.shouldCheckpoint()).thenReturn(true)

    val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
    recordProcessor.processRecords(batch, checkpointerMock)

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).store(record1.getData().array())
    verify(receiverMock, times(1)).store(record2.getData().array())
    verify(checkpointStateMock, times(1)).shouldCheckpoint()
    verify(checkpointerMock, times(1)).checkpoint()
    verify(checkpointStateMock, times(1)).advanceCheckpoint()
  }

  test("shouldn't store and checkpoint when receiver is stopped") {
    when(receiverMock.isStopped()).thenReturn(true)

    val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
    recordProcessor.processRecords(batch, checkpointerMock)

    verify(receiverMock, times(1)).isStopped()
  }

  test("shouldn't checkpoint when exception occurs during store") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(receiverMock.store(record1.getData().array())).thenThrow(new RuntimeException())

    intercept[RuntimeException] {
      val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
      recordProcessor.processRecords(batch, checkpointerMock)
    }

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).store(record1.getData().array())
  }

  test("should set checkpoint time to currentTime + checkpoint interval upon instantiation") {
    when(currentClockMock.currentTime()).thenReturn(0)

    val checkpointIntervalMillis = 10
    val checkpointState =
      new KinesisCheckpointState(Milliseconds(checkpointIntervalMillis), currentClockMock)
    assert(checkpointState.checkpointClock.currentTime() == checkpointIntervalMillis)

    verify(currentClockMock, times(1)).currentTime()
  }

  test("should checkpoint if we have exceeded the checkpoint interval") {
    when(currentClockMock.currentTime()).thenReturn(0)

    val checkpointState = new KinesisCheckpointState(Milliseconds(Long.MinValue), currentClockMock)
    assert(checkpointState.shouldCheckpoint())

    verify(currentClockMock, times(1)).currentTime()
  }

  test("shouldn't checkpoint if we have not exceeded the checkpoint interval") {
    when(currentClockMock.currentTime()).thenReturn(0)

    val checkpointState = new KinesisCheckpointState(Milliseconds(Long.MaxValue), currentClockMock)
    assert(!checkpointState.shouldCheckpoint())

    verify(currentClockMock, times(1)).currentTime()
  }

  test("should add to time when advancing checkpoint") {
    when(currentClockMock.currentTime()).thenReturn(0)

    val checkpointIntervalMillis = 10
    val checkpointState =
      new KinesisCheckpointState(Milliseconds(checkpointIntervalMillis), currentClockMock)
    assert(checkpointState.checkpointClock.currentTime() == checkpointIntervalMillis)
    checkpointState.advanceCheckpoint()
    assert(checkpointState.checkpointClock.currentTime() == (2 * checkpointIntervalMillis))

    verify(currentClockMock, times(1)).currentTime()
  }

  test("shutdown should checkpoint if the reason is TERMINATE") {
    val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
    val reason = ShutdownReason.TERMINATE
    recordProcessor.shutdown(checkpointerMock, reason)

    verify(checkpointerMock, times(1)).checkpoint()
  }

  test("shutdown should not checkpoint if the reason is something other than TERMINATE") {
    val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
    recordProcessor.shutdown(checkpointerMock, ShutdownReason.ZOMBIE)
    recordProcessor.shutdown(checkpointerMock, null)

    verify(checkpointerMock, never()).checkpoint()
  }

  test("retry success on first attempt") {
    val expectedIsStopped = false
    when(receiverMock.isStopped()).thenReturn(expectedIsStopped)

    val actualVal = KinesisRecordProcessor.retryRandom(receiverMock.isStopped(), 2, 100)
    assert(actualVal == expectedIsStopped)

    verify(receiverMock, times(1)).isStopped()
  }

  test("retry success on second attempt after a Kinesis throttling exception") {
    val expectedIsStopped = false
    when(receiverMock.isStopped())
        .thenThrow(new ThrottlingException("error message"))
        .thenReturn(expectedIsStopped)

    val actualVal = KinesisRecordProcessor.retryRandom(receiverMock.isStopped(), 2, 100)
    assert(actualVal == expectedIsStopped)

    verify(receiverMock, times(2)).isStopped()
  }

  test("retry success on second attempt after a Kinesis dependency exception") {
    val expectedIsStopped = false
    when(receiverMock.isStopped())
        .thenThrow(new KinesisClientLibDependencyException("error message"))
        .thenReturn(expectedIsStopped)

    val actualVal = KinesisRecordProcessor.retryRandom(receiverMock.isStopped(), 2, 100)
    assert(actualVal == expectedIsStopped)

    verify(receiverMock, times(2)).isStopped()
  }

  test("retry failed after a shutdown exception") {
    when(checkpointerMock.checkpoint()).thenThrow(new ShutdownException("error message"))

    intercept[ShutdownException] {
      KinesisRecordProcessor.retryRandom(checkpointerMock.checkpoint(), 2, 100)
    }

    verify(checkpointerMock, times(1)).checkpoint()
  }

  test("retry failed after an invalid state exception") {
    when(checkpointerMock.checkpoint()).thenThrow(new InvalidStateException("error message"))

    intercept[InvalidStateException] {
      KinesisRecordProcessor.retryRandom(checkpointerMock.checkpoint(), 2, 100)
    }

    verify(checkpointerMock, times(1)).checkpoint()
  }

  test("retry failed after unexpected exception") {
    when(checkpointerMock.checkpoint()).thenThrow(new RuntimeException("error message"))

    intercept[RuntimeException] {
      KinesisRecordProcessor.retryRandom(checkpointerMock.checkpoint(), 2, 100)
    }

    verify(checkpointerMock, times(1)).checkpoint()
  }

  test("retry failed after exhausing all retries") {
    val expectedErrorMessage = "final try error message"
    when(checkpointerMock.checkpoint())
        .thenThrow(new ThrottlingException("error message"))
        .thenThrow(new ThrottlingException(expectedErrorMessage))

    val exception = intercept[RuntimeException] {
      KinesisRecordProcessor.retryRandom(checkpointerMock.checkpoint(), 2, 100)
    }
    exception.getMessage().shouldBe(expectedErrorMessage)

    verify(checkpointerMock, times(2)).checkpoint()
  }
}
