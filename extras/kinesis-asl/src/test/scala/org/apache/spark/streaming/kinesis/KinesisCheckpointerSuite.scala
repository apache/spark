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
