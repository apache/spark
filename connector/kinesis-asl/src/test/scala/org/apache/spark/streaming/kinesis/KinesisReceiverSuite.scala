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
import java.nio.charset.StandardCharsets
import java.util.Arrays

import org.mockito.ArgumentMatchers.{any, anyList, anyString, eq => meq}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.kinesis.exceptions.{InvalidStateException, KinesisClientLibDependencyException, ShutdownException, ThrottlingException}
import software.amazon.kinesis.lifecycle.events.{InitializationInput, LeaseLostInput, ProcessRecordsInput, ShardEndedInput, ShutdownRequestedInput}
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.KinesisClientRecord

import org.apache.spark.streaming.{Duration, TestSuiteBase}

/**
 * Suite of Kinesis streaming receiver tests focusing mostly on the KinesisRecordProcessor
 */
class KinesisReceiverSuite extends TestSuiteBase with Matchers with BeforeAndAfter
    with MockitoSugar {

  val app = "TestKinesisReceiver"
  val stream = "mySparkStream"
  val endpoint = "endpoint-url"
  val schedulerId = "dummySchedulerId"
  val shardId = "dummyShardId"
  val seqNum = "dummySeqNum"
  val checkpointInterval = Duration(10)
  val someSeqNum = Some(seqNum)

  val dummyInitializationInput = InitializationInput.builder()
    .shardId(shardId)
    .build()

  val record1 = KinesisClientRecord.builder()
    .data(ByteBuffer.wrap("Spark In Action".getBytes(StandardCharsets.UTF_8)))
    .build()
  val record2 = KinesisClientRecord.builder()
    .data(ByteBuffer.wrap("Learning Spark".getBytes(StandardCharsets.UTF_8)))
    .build()
  val batch = Arrays.asList(record1, record2)

  var receiverMock: KinesisReceiver[Array[Byte]] = _
  var checkpointerMock: RecordProcessorCheckpointer = _

  override def beforeFunction(): Unit = {
    receiverMock = mock[KinesisReceiver[Array[Byte]]]
    checkpointerMock = mock[RecordProcessorCheckpointer]
  }

  test("process records including store and set checkpointer") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(receiverMock.getCurrentLimit).thenReturn(Int.MaxValue)

    val recordProcessor = new KinesisRecordProcessor(receiverMock, schedulerId)
    recordProcessor.initialize(dummyInitializationInput)
    val processRecordsInput = ProcessRecordsInput.builder()
      .records(batch)
      .checkpointer(checkpointerMock)
      .build()
    recordProcessor.processRecords(processRecordsInput)

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).addRecords(shardId, batch)
    verify(receiverMock, times(1)).setCheckpointer(shardId, checkpointerMock)
  }

  test("split into multiple processes if a limitation is set") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(receiverMock.getCurrentLimit).thenReturn(1)

    val recordProcessor = new KinesisRecordProcessor(receiverMock, schedulerId)
    recordProcessor.initialize(dummyInitializationInput)
    val processRecordsInput = ProcessRecordsInput.builder()
      .records(batch)
      .checkpointer(checkpointerMock)
      .build()
    recordProcessor.processRecords(processRecordsInput)

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).addRecords(shardId, batch.subList(0, 1))
    verify(receiverMock, times(1)).addRecords(shardId, batch.subList(1, 2))
    verify(receiverMock, times(1)).setCheckpointer(shardId, checkpointerMock)
  }

  test("shouldn't store and update checkpointer when receiver is stopped") {
    when(receiverMock.isStopped()).thenReturn(true)
    when(receiverMock.getCurrentLimit).thenReturn(Int.MaxValue)

    val recordProcessor = new KinesisRecordProcessor(receiverMock, schedulerId)
    val processRecordsInput = ProcessRecordsInput.builder()
      .records(batch)
      .checkpointer(checkpointerMock)
      .build()
    recordProcessor.processRecords(processRecordsInput)

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, never).addRecords(anyString, anyList())
    verify(receiverMock, never).setCheckpointer(anyString, meq(checkpointerMock))
  }

  test("shouldn't update checkpointer when exception occurs during store") {
    when(receiverMock.isStopped()).thenReturn(false)
    when(receiverMock.getCurrentLimit).thenReturn(Int.MaxValue)
    when(
      receiverMock.addRecords(anyString, anyList())
    ).thenThrow(new RuntimeException())

    intercept[RuntimeException] {
      val recordProcessor = new KinesisRecordProcessor(receiverMock, schedulerId)
      recordProcessor.initialize(dummyInitializationInput)
      val processRecordsInput = ProcessRecordsInput.builder()
        .records(batch)
        .checkpointer(checkpointerMock)
        .build()
      recordProcessor.processRecords(processRecordsInput)
    }

    verify(receiverMock, times(1)).isStopped()
    verify(receiverMock, times(1)).addRecords(shardId, batch)
    verify(receiverMock, never).setCheckpointer(anyString, meq(checkpointerMock))
  }

  test("should not checkpoint when the method leaseLost is called") {
    val recordProcessor = new KinesisRecordProcessor(receiverMock, schedulerId)
    val leaseLostInput = LeaseLostInput.builder().build()
    recordProcessor.leaseLost(leaseLostInput)
    verify(checkpointerMock, times(0)).checkpoint()
  }

  test("should checkpoint when the method shardEnded is called") {
    val recordProcessor = new KinesisRecordProcessor(receiverMock, schedulerId)
    val shardEndedInput = ShardEndedInput.builder()
      .checkpointer(checkpointerMock)
      .build()
    recordProcessor.initialize(dummyInitializationInput)
    recordProcessor.shardEnded(shardEndedInput)

    verify(receiverMock, times(1)).removeCheckpointer(shardId, checkpointerMock)
  }

  test("should not checkpoint when the method shardEnded is called, but shardId is null") {
    val recordProcessor = new KinesisRecordProcessor(receiverMock, schedulerId)
    val initializationInput = InitializationInput.builder()
      .shardId(null)
      .build()
    recordProcessor.initialize(initializationInput)

    val shardEndedInput = ShardEndedInput.builder()
      .checkpointer(checkpointerMock)
      .build()
    recordProcessor.shardEnded(shardEndedInput)

    verify(receiverMock, times(0)).removeCheckpointer(any(), meq(checkpointerMock))
  }

  test("should checkpoint when the method shutdownRequested is called") {
    val recordProcessor = new KinesisRecordProcessor(receiverMock, schedulerId)
    val shutdownRequestedInput = ShutdownRequestedInput.builder()
      .checkpointer(checkpointerMock)
      .build()
    recordProcessor.initialize(dummyInitializationInput)
    recordProcessor.shutdownRequested(shutdownRequestedInput)

    verify(receiverMock, times(1)).removeCheckpointer(shardId, checkpointerMock)
  }

  test("should not checkpoint when the method shutdownRequested is called, but shardId is null") {
    val recordProcessor = new KinesisRecordProcessor(receiverMock, schedulerId)
    val initializationInput = InitializationInput.builder()
      .shardId(null)
      .build()
    recordProcessor.initialize(initializationInput)

    val shutdownRequestedInput = ShutdownRequestedInput.builder()
      .checkpointer(checkpointerMock)
      .build()
    recordProcessor.shutdownRequested(shutdownRequestedInput)

    verify(receiverMock, times(0)).removeCheckpointer(any(), meq(checkpointerMock))
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

  test("retry failed after exhausting all retries") {
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
