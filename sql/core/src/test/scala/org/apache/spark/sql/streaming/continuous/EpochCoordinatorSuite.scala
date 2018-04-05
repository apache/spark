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

import org.mockito.InOrder
import org.mockito.Matchers.{any, eq => eqTo}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousReader, PartitionOffset}
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.test.SharedSparkSession

class EpochCoordinatorSuite
  extends SparkFunSuite
    with SharedSparkSession
    with MockitoSugar
    with BeforeAndAfterEach {

  private var epochCoordinator: RpcEndpointRef = _

  private var writer: StreamWriter = _
  private var query: ContinuousExecution = _
  private var orderVerifier: InOrder = _

  private val startEpoch = 1L

  override def beforeEach(): Unit = {
    val reader = mock[ContinuousReader]
    writer = mock[StreamWriter]
    query = mock[ContinuousExecution]
    orderVerifier = inOrder(writer, query)

    epochCoordinator
      = EpochCoordinatorRef.create(writer, reader, query, "test", startEpoch, spark, SparkEnv.get)
  }

  override def afterEach(): Unit = {
    SparkEnv.get.rpcEnv.stop(epochCoordinator)
  }

  test("single epoch") {
    setWriterPartitions(3)
    setReaderPartitions(2)

    commitPartitionEpoch(0, startEpoch)
    commitPartitionEpoch(1, startEpoch)
    commitPartitionEpoch(2, startEpoch)
    reportPartitionOffset(0, startEpoch)
    reportPartitionOffset(1, startEpoch)

    // Here and in subsequent tests this is called to make a synchronous call to EpochCoordinator
    // so that mocks would have been acted upon by the time verification happens
    makeSynchronousCall()

    verifyCommit(startEpoch)
  }

  test("consequent epochs, messages for epoch (k + 1) arrive after messages for epoch k") {
    setWriterPartitions(2)
    setReaderPartitions(2)

    val epochs = startEpoch to (startEpoch + 1)

    commitPartitionEpoch(0, epochs(0))
    commitPartitionEpoch(1, epochs(0))
    reportPartitionOffset(0, epochs(0))
    reportPartitionOffset(1, epochs(0))

    commitPartitionEpoch(0, epochs(1))
    commitPartitionEpoch(1, epochs(1))
    reportPartitionOffset(0, epochs(1))
    reportPartitionOffset(1, epochs(1))

    makeSynchronousCall()

    verifyCommitsInOrderOf(epochs)
  }

  ignore("consequent epochs, a message for epoch k arrives after messages for epoch (k + 1)") {
    setWriterPartitions(2)
    setReaderPartitions(2)

    val epochs = startEpoch to (startEpoch + 1)

    commitPartitionEpoch(0, epochs(0))
    commitPartitionEpoch(1, epochs(0))
    reportPartitionOffset(0, epochs(0))

    commitPartitionEpoch(0, epochs(1))
    commitPartitionEpoch(1, epochs(1))
    reportPartitionOffset(0, epochs(1))
    reportPartitionOffset(1, epochs(1))

    // Message that arrives late
    reportPartitionOffset(1, epochs(0))

    makeSynchronousCall()

    verifyCommitsInOrderOf(epochs)
  }

  ignore("several epochs, messages arrive in order 1 -> 3 -> 4 -> 2") {
    setWriterPartitions(1)
    setReaderPartitions(1)

    val epochs = startEpoch to (startEpoch + 3)

    commitPartitionEpoch(0, epochs(0))
    reportPartitionOffset(0, epochs(0))

    commitPartitionEpoch(0, epochs(2))
    reportPartitionOffset(0, epochs(2))

    commitPartitionEpoch(0, epochs(3))
    reportPartitionOffset(0, epochs(3))

    commitPartitionEpoch(0, epochs(1))
    reportPartitionOffset(0, epochs(1))

    makeSynchronousCall()

    verifyCommitsInOrderOf(epochs)
  }

  ignore("several epochs, messages arrive in order 1 -> 3 -> 5 -> 4 -> 2") {
    setWriterPartitions(1)
    setReaderPartitions(1)

    val epochs = startEpoch to (startEpoch + 4)

    commitPartitionEpoch(0, epochs(0))
    reportPartitionOffset(0, epochs(0))

    commitPartitionEpoch(0, epochs(2))
    reportPartitionOffset(0, epochs(2))

    commitPartitionEpoch(0, epochs(4))
    reportPartitionOffset(0, epochs(4))

    commitPartitionEpoch(0, epochs(3))
    reportPartitionOffset(0, epochs(3))

    commitPartitionEpoch(0, epochs(1))
    reportPartitionOffset(0, epochs(1))

    makeSynchronousCall()

    verifyCommitsInOrderOf(epochs)
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
    orderVerifier.verify(writer).commit(eqTo(epoch), any())
    orderVerifier.verify(query).commit(epoch)
  }

  private def verifyCommitsInOrderOf(epochs: Seq[Long]): Unit = {
    epochs.foreach(verifyCommit)
  }
}
