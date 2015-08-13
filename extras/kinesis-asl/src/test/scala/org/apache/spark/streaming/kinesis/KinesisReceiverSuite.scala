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

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, KinesisClientLibDependencyException, ShutdownException, ThrottlingException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.streaming.{Milliseconds, TestSuiteBase}
import org.apache.spark.util.{Clock, ManualClock, Utils}

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
  val seqNum = "dummySeqNum"
  val someSeqNum = Some(seqNum)

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

  override def beforeFunction(): Unit = {
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

  test("check serializability of SerializableAWSCredentials") {
    Utils.deserialize[SerializableAWSCredentials](
      Utils.serialize(new SerializableAWSCredentials("x", "y")))
  }

  test("process records including store and checkpoint") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)
    when(checkpointStateMock.shouldCheckpoint()).thenReturn(true)

    val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
    recordProcessor.initialize(shardId)
    recordProcessor.processRecords(batch, checkpointerMock)

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).addRecords(shardId, batch)
    verify(receiverMock, times(1)).getLatestSeqNumToCheckpoint(shardId)
    verify(checkpointStateMock, times(1)).shouldCheckpoint()
    verify(checkpointerMock, times(1)).checkpoint(anyString)
    verify(checkpointStateMock, times(1)).advanceCheckpoint()
  }

  test("shouldn't store and checkpoint when receiver is stopped") {
    when(receiverMock.isStopped()).thenReturn(true)

    val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
    recordProcessor.processRecords(batch, checkpointerMock)

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, never).addRecords(anyString, anyListOf(classOf[Record]))
    verify(checkpointerMock, never).checkpoint(anyString)
  }

  test("shouldn't checkpoint when exception occurs during store") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(
      receiverMock.addRecords(anyString, anyListOf(classOf[Record]))
    ).thenThrow(new RuntimeException())

    intercept[RuntimeException] {
      val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
      recordProcessor.initialize(shardId)
      recordProcessor.processRecords(batch, checkpointerMock)
    }

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).addRecords(shardId, batch)
    verify(checkpointerMock, never).checkpoint(anyString)
  }

  test("should set checkpoint time to currentTime + checkpoint interval upon instantiation") {
    when(currentClockMock.getTimeMillis()).thenReturn(0)

    val checkpointIntervalMillis = 10
    val checkpointState =
      new KinesisCheckpointState(Milliseconds(checkpointIntervalMillis), currentClockMock)
    assert(checkpointState.checkpointClock.getTimeMillis() == checkpointIntervalMillis)

    verify(currentClockMock, times(1)).getTimeMillis()
  }

  test("should checkpoint if we have exceeded the checkpoint interval") {
    when(currentClockMock.getTimeMillis()).thenReturn(0)

    val checkpointState = new KinesisCheckpointState(Milliseconds(Long.MinValue), currentClockMock)
    assert(checkpointState.shouldCheckpoint())

    verify(currentClockMock, times(1)).getTimeMillis()
  }

  test("shouldn't checkpoint if we have not exceeded the checkpoint interval") {
    when(currentClockMock.getTimeMillis()).thenReturn(0)

    val checkpointState = new KinesisCheckpointState(Milliseconds(Long.MaxValue), currentClockMock)
    assert(!checkpointState.shouldCheckpoint())

    verify(currentClockMock, times(1)).getTimeMillis()
  }

  test("should add to time when advancing checkpoint") {
    when(currentClockMock.getTimeMillis()).thenReturn(0)

    val checkpointIntervalMillis = 10
    val checkpointState =
      new KinesisCheckpointState(Milliseconds(checkpointIntervalMillis), currentClockMock)
    assert(checkpointState.checkpointClock.getTimeMillis() == checkpointIntervalMillis)
    checkpointState.advanceCheckpoint()
    assert(checkpointState.checkpointClock.getTimeMillis() == (2 * checkpointIntervalMillis))

    verify(currentClockMock, times(1)).getTimeMillis()
  }

  test("shutdown should checkpoint if the reason is TERMINATE") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)

    val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
    recordProcessor.initialize(shardId)
    recordProcessor.shutdown(checkpointerMock, ShutdownReason.TERMINATE)

    verify(receiverMock, times(1)).getLatestSeqNumToCheckpoint(shardId)
    verify(checkpointerMock, times(1)).checkpoint(anyString)
  }

  test("shutdown should not checkpoint if the reason is something other than TERMINATE") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)

    val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
    recordProcessor.initialize(shardId)
    recordProcessor.shutdown(checkpointerMock, ShutdownReason.ZOMBIE)
    recordProcessor.shutdown(checkpointerMock, null)

    verify(checkpointerMock, never).checkpoint(anyString)
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
