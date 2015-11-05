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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar

import org.apache.spark.streaming.{Duration, TestSuiteBase, Milliseconds}

class KinesisCheckpointerSuite extends TestSuiteBase with MockitoSugar with BeforeAndAfter {

  val app = "TestKinesisReceiver"
  val stream = "mySparkStream"
  val endpoint = "endpoint-url"
  val workerId = "dummyWorkerId"
  val shardId = "dummyShardId"
  val seqNum = "123"
  val otherSeqNum = "245"
  val checkpointInterval = Duration(10)
  val someSeqNum = Some(seqNum)
  val someOtherSeqNum = Some(otherSeqNum)

  var receiverMock: KinesisReceiver[Array[Byte]] = _
  var checkpointerMock: IRecordProcessorCheckpointer = _

  override def beforeFunction(): Unit = {
    receiverMock = mock[KinesisReceiver[Array[Byte]]]
    checkpointerMock = mock[IRecordProcessorCheckpointer]
  }

  test("checkpoint is not called for None's and nulls") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)
    val checkpointState =
      new KinesisCheckpointer(receiverMock, checkpointInterval, workerId)
    checkpointState.checkpoint(shardId, Option(checkpointerMock))

    verify(checkpointerMock, times(1)).checkpoint(anyString())
    checkpointState.checkpoint(shardId, None)
    checkpointState.checkpoint(shardId, Option(null))
    // the above two calls should be No-Ops
    verify(checkpointerMock, times(1)).checkpoint(anyString())
  }

  test("checkpoint is not called twice for the same sequence number") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)
    val checkpointState =
      new KinesisCheckpointer(receiverMock, checkpointInterval, workerId)
    checkpointState.checkpoint(shardId, Option(checkpointerMock))
    checkpointState.checkpoint(shardId, Option(checkpointerMock))

    verify(checkpointerMock, times(1)).checkpoint(anyString())
  }

  test("checkpoint is called after sequence number increases") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId))
      .thenReturn(someSeqNum).thenReturn(someOtherSeqNum)
    val checkpointState =
      new KinesisCheckpointer(receiverMock, checkpointInterval, workerId)
    checkpointState.checkpoint(shardId, Option(checkpointerMock))
    checkpointState.checkpoint(shardId, Option(checkpointerMock))

    verify(checkpointerMock, times(1)).checkpoint(seqNum)
    verify(checkpointerMock, times(1)).checkpoint(otherSeqNum)
  }

  test("should checkpoint if we have exceeded the checkpoint interval") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId))
      .thenReturn(someSeqNum).thenReturn(someOtherSeqNum)
    val checkpointState =
      new KinesisCheckpointer(receiverMock, checkpointInterval, workerId)
    checkpointState.setCheckpointer(shardId, checkpointerMock)
    Thread.sleep(checkpointInterval.milliseconds * 5)
    verify(checkpointerMock, times(1)).checkpoint(seqNum)
    verify(checkpointerMock, times(1)).checkpoint(otherSeqNum)
    checkpointState.shutdown()
  }

  test("shouldn't checkpoint if we have not exceeded the checkpoint interval") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)
    val checkpointState =
      new KinesisCheckpointer(receiverMock, Milliseconds(Long.MaxValue), workerId)
    checkpointState.setCheckpointer(shardId, checkpointerMock)
    Thread.sleep(checkpointInterval.milliseconds)
    verify(checkpointerMock, never()).checkpoint(anyString())
    checkpointState.shutdown()
  }

  test("should not checkpoint for the same sequence number") {
    when(receiverMock.getLatestSeqNumToCheckpoint(shardId)).thenReturn(someSeqNum)
    val checkpointState =
      new KinesisCheckpointer(receiverMock, checkpointInterval, workerId)
    checkpointState.setCheckpointer(shardId, checkpointerMock)
    Thread.sleep(checkpointInterval.milliseconds * 5)
    verify(checkpointerMock, times(1)).checkpoint(anyString())
    checkpointState.shutdown()
  }
}
