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

import java.util.concurrent.TimeoutException

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}
import org.scalatest.concurrent.Eventually
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.streaming.{Duration, TestSuiteBase}
import org.apache.spark.util.ManualClock

class KinesisCheckpointerSuite extends TestSuiteBase
  with MockitoSugar
  with BeforeAndAfterEach
  with PrivateMethodTester
  with Eventually {

  private val workerId = "dummyWorkerId"
  private val shardId = "dummyShardId"
  private val seqNum = "123"
  private val otherSeqNum = "245"
  private val checkpointInterval = Duration(10)
  private val someSeqNum = Some(seqNum)
  private val someOtherSeqNum = Some(otherSeqNum)

  private var receiverMock: KinesisReceiver[Array[Byte]] = _
  private var checkpointerMock: IRecordProcessorCheckpointer = _
  private var kinesisCheckpointer: KinesisCheckpointer = _
  private var clock: ManualClock = _

  private val checkpoint = PrivateMethod[Unit](Symbol("checkpoint"))

  override def beforeEach(): Unit = {
    receiverMock = mock[KinesisReceiver[Array[Byte]]]
    checkpointerMock = mock[IRecordProcessorCheckpointer]
    clock = new ManualClock()
    kinesisCheckpointer = new KinesisCheckpointer(receiverMock, checkpointInterval, workerId, clock)
  }

  test("checkpoint is not called twice for the same sequence number") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)
    kinesisCheckpointer.invokePrivate(checkpoint(shardId, checkpointerMock))
    kinesisCheckpointer.invokePrivate(checkpoint(shardId, checkpointerMock))

    verify(checkpointerMock, times(1)).checkpoint(anyString())
  }

  test("checkpoint is called after sequence number increases") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId))
      .thenReturn(someSeqNum).thenReturn(someOtherSeqNum)
    kinesisCheckpointer.invokePrivate(checkpoint(shardId, checkpointerMock))
    kinesisCheckpointer.invokePrivate(checkpoint(shardId, checkpointerMock))

    verify(checkpointerMock, times(1)).checkpoint(seqNum)
    verify(checkpointerMock, times(1)).checkpoint(otherSeqNum)
  }

  test("should checkpoint if we have exceeded the checkpoint interval") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId))
      .thenReturn(someSeqNum).thenReturn(someOtherSeqNum)

    kinesisCheckpointer.setCheckpointer(shardId, checkpointerMock)
    clock.advance(5 * checkpointInterval.milliseconds)

    eventually(timeout(1.second)) {
      verify(checkpointerMock, times(1)).checkpoint(seqNum)
      verify(checkpointerMock, times(1)).checkpoint(otherSeqNum)
    }
  }

  test("shouldn't checkpoint if we have not exceeded the checkpoint interval") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)

    kinesisCheckpointer.setCheckpointer(shardId, checkpointerMock)
    clock.advance(checkpointInterval.milliseconds / 2)

    verify(checkpointerMock, never()).checkpoint(anyString())
  }

  test("should not checkpoint for the same sequence number") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)

    kinesisCheckpointer.setCheckpointer(shardId, checkpointerMock)

    clock.advance(checkpointInterval.milliseconds * 5)
    eventually(timeout(1.second)) {
      verify(checkpointerMock, atMost(1)).checkpoint(anyString())
    }
  }

  test("removing checkpointer checkpoints one last time") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)

    kinesisCheckpointer.removeCheckpointer(shardId, checkpointerMock)
    verify(checkpointerMock, times(1)).checkpoint()
  }

  test("if checkpointing is going on, wait until finished before removing and checkpointing") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId))
      .thenReturn(someSeqNum).thenReturn(someOtherSeqNum)
    when(checkpointerMock.checkpoint(anyString)).thenAnswer { (_: InvocationOnMock) =>
      clock.waitTillTime(clock.getTimeMillis() + checkpointInterval.milliseconds / 2)
    }

    kinesisCheckpointer.setCheckpointer(shardId, checkpointerMock)
    clock.advance(checkpointInterval.milliseconds)
    eventually(timeout(1.second)) {
      verify(checkpointerMock, times(1)).checkpoint(anyString())
    }
    // don't block test thread
    val f = Future(kinesisCheckpointer.removeCheckpointer(shardId, checkpointerMock))(
      ExecutionContext.global)

    intercept[TimeoutException] {
      // scalastyle:off awaitready
      Await.ready(f, 50.milliseconds)
      // scalastyle:on awaitready
    }

    clock.advance(checkpointInterval.milliseconds / 2)
    eventually(timeout(1.second)) {
      verify(checkpointerMock, times(1)).checkpoint(anyString)
      verify(checkpointerMock, times(1)).checkpoint()
    }
  }
}
