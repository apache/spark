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
import java.nio.CharBuffer
import java.nio.charset.Charset
import scala.collection.JavaConversions.seqAsJavaList
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.PrivateMethodTester
import org.scalatest.mock.EasyMockSugar
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.model.Record
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.receiver.Receiver
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.util.ManualClock
import org.apache.spark.streaming.util.SystemClock
import org.apache.spark.streaming.util.Clock

/**
 *  Suite of Kinesis streaming receiver tests focusing mostly on the KinesisRecordProcessor 
 */
class KinesisReceiverSuite extends FunSuite with Matchers with BeforeAndAfter with EasyMockSugar {
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
  val expectedArrayBuffer = new ArrayBuffer[Array[Byte]]() += record1.getData().array() += record2.getData().array()

  var receiverMock: KinesisReceiver = _
  var checkpointerMock: IRecordProcessorCheckpointer = _
  var checkpointClockMock: ManualClock = _
  var checkpointStateMock: CheckpointState = _
  var currentClockMock: Clock = _

  before {
    receiverMock = mock[KinesisReceiver]
    checkpointerMock = mock[IRecordProcessorCheckpointer]
    checkpointClockMock = mock[ManualClock]
    checkpointStateMock = mock[CheckpointState]
    currentClockMock = mock[Clock]
  }

  test("process records including store and checkpoint") {
    val expectedCheckpointIntervalMillis = 10
    expecting {
      receiverMock.isStopped().andReturn(false).once()
      receiverMock.store(expectedArrayBuffer).once()
      checkpointStateMock.shouldCheckpoint().andReturn(true).once()
      checkpointerMock.checkpoint().once()
      checkpointStateMock.advanceCheckpoint().once()
    }
    whenExecuting(receiverMock, checkpointerMock, checkpointStateMock) {
      val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
      recordProcessor.processRecords(batch, checkpointerMock)
    }
  }

  test("shouldn't store and checkpoint when receiver is stopped") {
    expecting {
      receiverMock.isStopped().andReturn(true).once()
    }
    whenExecuting(receiverMock, checkpointerMock, checkpointStateMock) {
      val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
      recordProcessor.processRecords(batch, checkpointerMock)
    }
  }

  test("shouldn't checkpoint when exception occurs during store") {
    expecting {
      receiverMock.isStopped().andReturn(false).once()
      receiverMock.store(expectedArrayBuffer).andThrow(new RuntimeException()).once()
    }
    whenExecuting(receiverMock, checkpointerMock, checkpointStateMock) {
      intercept[RuntimeException] {
        val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
        recordProcessor.processRecords(batch, checkpointerMock)
      }
    }
  }

  test("should set checkpoint time to currentTime + checkpoint interval upon instantiation") {
    expecting {
      currentClockMock.currentTime().andReturn(0).once()
    }
    whenExecuting(currentClockMock) {
    val checkpointIntervalMillis = 10
    val checkpointState = new CheckpointState(checkpointIntervalMillis, currentClockMock)
    assert(checkpointState.checkpointClock.currentTime() == checkpointIntervalMillis)
    }
  }

  test("should checkpoint if we have exceeded the checkpoint interval") {
    expecting {
      currentClockMock.currentTime().andReturn(0).once()
    }
    whenExecuting(currentClockMock) {
      val checkpointState = new CheckpointState(Long.MinValue, currentClockMock)
      assert(checkpointState.shouldCheckpoint())
    }
  }

  test("shouldn't checkpoint if we have not exceeded the checkpoint interval") {
    expecting {
      currentClockMock.currentTime().andReturn(0).once()
    }
    whenExecuting(currentClockMock) {
      val checkpointState = new CheckpointState(Long.MaxValue, currentClockMock)
      assert(!checkpointState.shouldCheckpoint())
    }
  }

  test("should add to time when advancing checkpoint") {
    expecting {
      currentClockMock.currentTime().andReturn(0).once()
    }
    whenExecuting(currentClockMock) {
      val checkpointIntervalMillis = 10
      val checkpointState = new CheckpointState(checkpointIntervalMillis, currentClockMock)
      assert(checkpointState.checkpointClock.currentTime() == checkpointIntervalMillis)
      checkpointState.advanceCheckpoint()
      assert(checkpointState.checkpointClock.currentTime() == (2 * checkpointIntervalMillis))
    }
  }

  test("shutdown should checkpoint if the reason is TERMINATE") {
    expecting {
      checkpointerMock.checkpoint().once()
    }
    whenExecuting(checkpointerMock, checkpointStateMock) {
      val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
      val reason = ShutdownReason.TERMINATE
      recordProcessor.shutdown(checkpointerMock, reason)
    }
  }

  test("shutdown should not checkpoint if the reason is something other than TERMINATE") {
    expecting {
    }
    whenExecuting(checkpointerMock, checkpointStateMock) {
      val recordProcessor = new KinesisRecordProcessor(receiverMock, workerId, checkpointStateMock)
      recordProcessor.shutdown(checkpointerMock, ShutdownReason.ZOMBIE)
      recordProcessor.shutdown(checkpointerMock, null)
    }
  }

  test("string record converter") {
    val expectedString = "http://sparkinaction.com"
    val expectedByteArray = expectedString.getBytes()
    val stringRecordSerializer = new KinesisStringRecordSerializer()

    expectedByteArray should be(stringRecordSerializer.serialize(expectedString))

    expectedString should be(stringRecordSerializer.deserialize(expectedByteArray))
    expectedString should be(stringRecordSerializer.deserialize(stringRecordSerializer.serialize(expectedString)))
  }

  test("retry success on first attempt") {
    val expectedIsStopped = false
    expecting {
      receiverMock.isStopped().andReturn(expectedIsStopped).once()
    }
    whenExecuting(receiverMock) {
      val actualVal = KinesisUtils.retry(receiverMock.isStopped(), 2, 100)
      assert(actualVal == expectedIsStopped)
    }
  }

  test("retry success on second attempt after a Kinesis throttling exception") {
    val expectedIsStopped = false
    expecting {
      receiverMock.isStopped().andThrow(new ThrottlingException("error message")).andReturn(expectedIsStopped).once()
    }
    whenExecuting(receiverMock) {
      val actualVal = KinesisUtils.retry(receiverMock.isStopped(), 2, 100)
      assert(actualVal == expectedIsStopped)
    }
  }

  test("retry success on second attempt after a Kinesis dependency exception") {
    val expectedIsStopped = false
    expecting {
      receiverMock.isStopped().andThrow(new KinesisClientLibDependencyException("error message")).andReturn(expectedIsStopped).once()
    }
    whenExecuting(receiverMock) {
      val actualVal = KinesisUtils.retry(receiverMock.isStopped(), 2, 100)
      assert(actualVal == expectedIsStopped)
    }
  }

  test("retry failed after a shutdown exception") {
    expecting {
      checkpointerMock.checkpoint().andThrow(new ShutdownException("error message")).once()
    }
    whenExecuting(checkpointerMock) {
      intercept[ShutdownException] {
        KinesisUtils.retry(checkpointerMock.checkpoint(), 2, 100)
      }
    }
  }

  test("retry failed after an invalid state exception") {
    expecting {
      checkpointerMock.checkpoint().andThrow(new InvalidStateException("error message")).once()
    }
    whenExecuting(checkpointerMock) {
      intercept[InvalidStateException] {
        KinesisUtils.retry(checkpointerMock.checkpoint(), 2, 100)
      }
    }
  }

  test("retry failed after unexpected exception") {
    expecting {
      checkpointerMock.checkpoint().andThrow(new RuntimeException("error message")).once()
    }
    whenExecuting(checkpointerMock) {
      intercept[RuntimeException] {
        KinesisUtils.retry(checkpointerMock.checkpoint(), 2, 100)
      }
    }
  }

  test("retry failed after exhausing all retries") {
    val expectedErrorMessage = "final try error message"
    expecting {
      checkpointerMock.checkpoint().andThrow(new ThrottlingException("error message")).andThrow(new ThrottlingException(expectedErrorMessage)).once()
    }
    whenExecuting(checkpointerMock) {
      val exception = intercept[RuntimeException] {
        KinesisUtils.retry(checkpointerMock.checkpoint(), 2, 100)
      }
      exception.getMessage().shouldBe(expectedErrorMessage)
    }
  }
}
