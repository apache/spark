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

package org.apache.spark.sql.streaming.continuous

import org.mockito.{ArgumentCaptor, InOrder}
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.LocalSparkSession
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, PartitionOffset}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.internal.SQLConf.CONTINUOUS_STREAMING_EPOCH_BACKLOG_QUEUE_SIZE
import org.apache.spark.sql.test.TestSparkSession

class EpochCoordinatorSuite
  extends SparkFunSuite
    with LocalSparkSession
    with MockitoSugar
    with BeforeAndAfterEach {

  private var epochCoordinator: RpcEndpointRef = _

  private var writeSupport: StreamingWrite = _
  private var query: ContinuousExecution = _
  private var orderVerifier: InOrder = _
  private val epochBacklogQueueSize = 10

  override def beforeEach(): Unit = {
    val stream = mock[ContinuousStream]
    writeSupport = mock[StreamingWrite]
    query = mock[ContinuousExecution]
    orderVerifier = inOrder(writeSupport, query)

    spark = new TestSparkSession(
    new SparkContext(
      "local[2]", "test-sql-context",
      new SparkConf().set("spark.sql.testkey", "true")
        .set(CONTINUOUS_STREAMING_EPOCH_BACKLOG_QUEUE_SIZE, epochBacklogQueueSize)))

    epochCoordinator
      = EpochCoordinatorRef.create(writeSupport, stream, query, "test", 1, spark, SparkEnv.get)
  }

  test("single epoch") {
    setWriterPartitions(3)
    setReaderPartitions(2)

    commitPartitionEpoch(0, 1)
    commitPartitionEpoch(1, 1)
    commitPartitionEpoch(2, 1)
    reportPartitionOffset(0, 1)
    reportPartitionOffset(1, 1)

    // Here and in subsequent tests this is called to make a synchronous call to EpochCoordinator
    // so that mocks would have been acted upon by the time verification happens
    makeSynchronousCall()

    verifyCommit(1)
  }

  test("single epoch, all but one writer partition has committed") {
    setWriterPartitions(3)
    setReaderPartitions(2)

    commitPartitionEpoch(0, 1)
    commitPartitionEpoch(1, 1)
    reportPartitionOffset(0, 1)
    reportPartitionOffset(1, 1)

    makeSynchronousCall()

    verifyNoCommitFor(1)
  }

  test("single epoch, all but one reader partition has reported an offset") {
    setWriterPartitions(3)
    setReaderPartitions(2)

    commitPartitionEpoch(0, 1)
    commitPartitionEpoch(1, 1)
    commitPartitionEpoch(2, 1)
    reportPartitionOffset(0, 1)

    makeSynchronousCall()

    verifyNoCommitFor(1)
  }

  test("consequent epochs, messages for epoch (k + 1) arrive after messages for epoch k") {
    setWriterPartitions(2)
    setReaderPartitions(2)

    commitPartitionEpoch(0, 1)
    commitPartitionEpoch(1, 1)
    reportPartitionOffset(0, 1)
    reportPartitionOffset(1, 1)

    commitPartitionEpoch(0, 2)
    commitPartitionEpoch(1, 2)
    reportPartitionOffset(0, 2)
    reportPartitionOffset(1, 2)

    makeSynchronousCall()

    verifyCommitsInOrderOf(List(1, 2))
  }

  test("consequent epochs, a message for epoch k arrives after messages for epoch (k + 1)") {
    setWriterPartitions(2)
    setReaderPartitions(2)

    commitPartitionEpoch(0, 1)
    commitPartitionEpoch(1, 1)
    reportPartitionOffset(0, 1)

    commitPartitionEpoch(0, 2)
    commitPartitionEpoch(1, 2)
    reportPartitionOffset(0, 2)
    reportPartitionOffset(1, 2)

    // Message that arrives late
    reportPartitionOffset(1, 1)

    makeSynchronousCall()

    verifyCommitsInOrderOf(List(1, 2))
  }

  test("several epochs, messages arrive in order 1 -> 3 -> 4 -> 2") {
    setWriterPartitions(1)
    setReaderPartitions(1)

    commitPartitionEpoch(0, 1)
    reportPartitionOffset(0, 1)

    commitPartitionEpoch(0, 3)
    reportPartitionOffset(0, 3)

    commitPartitionEpoch(0, 4)
    reportPartitionOffset(0, 4)

    commitPartitionEpoch(0, 2)
    reportPartitionOffset(0, 2)

    makeSynchronousCall()

    verifyCommitsInOrderOf(List(1, 2, 3, 4))
  }

  test("several epochs, messages arrive in order 1 -> 3 -> 5 -> 4 -> 2") {
    setWriterPartitions(1)
    setReaderPartitions(1)

    commitPartitionEpoch(0, 1)
    reportPartitionOffset(0, 1)

    commitPartitionEpoch(0, 3)
    reportPartitionOffset(0, 3)

    commitPartitionEpoch(0, 5)
    reportPartitionOffset(0, 5)

    commitPartitionEpoch(0, 4)
    reportPartitionOffset(0, 4)

    commitPartitionEpoch(0, 2)
    reportPartitionOffset(0, 2)

    makeSynchronousCall()

    verifyCommitsInOrderOf(List(1, 2, 3, 4, 5))
  }

  test("several epochs, max epoch backlog reached by partitionOffsets") {
    setWriterPartitions(1)
    setReaderPartitions(1)

    reportPartitionOffset(0, 1)
    // Commit messages not arriving
    for (i <- 2 to epochBacklogQueueSize + 1) {
      reportPartitionOffset(0, i)
    }

    makeSynchronousCall()

    for (i <- 1 to epochBacklogQueueSize + 1) {
      verifyNoCommitFor(i)
    }
    verifyStoppedWithException("Size of the partition offset queue has exceeded its maximum")
  }

  test("several epochs, max epoch backlog reached by partitionCommits") {
    setWriterPartitions(1)
    setReaderPartitions(1)

    commitPartitionEpoch(0, 1)
    // Offset messages not arriving
    for (i <- 2 to epochBacklogQueueSize + 1) {
      commitPartitionEpoch(0, i)
    }

    makeSynchronousCall()

    for (i <- 1 to epochBacklogQueueSize + 1) {
      verifyNoCommitFor(i)
    }
    verifyStoppedWithException("Size of the partition commit queue has exceeded its maximum")
  }

  test("several epochs, max epoch backlog reached by epochsWaitingToBeCommitted") {
    setWriterPartitions(2)
    setReaderPartitions(2)

    commitPartitionEpoch(0, 1)
    reportPartitionOffset(0, 1)

    // For partition 2 epoch 1 messages never arriving
    // +2 because the first epoch not yet arrived
    for (i <- 2 to epochBacklogQueueSize + 2) {
      commitPartitionEpoch(0, i)
      reportPartitionOffset(0, i)
      commitPartitionEpoch(1, i)
      reportPartitionOffset(1, i)
    }

    makeSynchronousCall()

    for (i <- 1 to epochBacklogQueueSize + 2) {
      verifyNoCommitFor(i)
    }
    verifyStoppedWithException("Size of the epoch queue has exceeded its maximum")
  }

  private def setWriterPartitions(numPartitions: Int): Unit = {
    epochCoordinator.askSync[Unit](SetWriterPartitions(numPartitions))
  }

  private def setReaderPartitions(numPartitions: Int): Unit = {
    epochCoordinator.askSync[Unit](SetReaderPartitions(numPartitions))
  }

  private def commitPartitionEpoch(partitionId: Int, epoch: Long): Unit = {
    val dummyMessage: WriterCommitMessage = mock[WriterCommitMessage]
    epochCoordinator.send(CommitPartitionEpoch(partitionId, epoch, dummyMessage))
  }

  private def reportPartitionOffset(partitionId: Int, epoch: Long): Unit = {
    val dummyOffset: PartitionOffset = mock[PartitionOffset]
    epochCoordinator.send(ReportPartitionOffset(partitionId, epoch, dummyOffset))
  }

  private def makeSynchronousCall(): Unit = {
    epochCoordinator.askSync[Long](GetCurrentEpoch)
  }

  private def verifyCommit(epoch: Long): Unit = {
    orderVerifier.verify(writeSupport).commit(eqTo(epoch), any())
    orderVerifier.verify(query).commit(epoch)
  }

  private def verifyNoCommitFor(epoch: Long): Unit = {
    verify(writeSupport, never()).commit(eqTo(epoch), any())
    verify(query, never()).commit(epoch)
  }

  private def verifyCommitsInOrderOf(epochs: Seq[Long]): Unit = {
    epochs.foreach(verifyCommit)
  }

  private def verifyStoppedWithException(msg: String): Unit = {
    val exceptionCaptor = ArgumentCaptor.forClass(classOf[Throwable]);
    verify(query, atLeastOnce()).stopInNewThread(exceptionCaptor.capture())

    import scala.jdk.CollectionConverters._
    val throwable = exceptionCaptor.getAllValues.asScala.find(_.getMessage === msg)
    assert(throwable != null, "Stream stopped with an exception but expected message is missing")
  }
}
