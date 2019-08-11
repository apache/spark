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

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousPartitionReader, ContinuousStream, PartitionOffset}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamingWrite
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

class ContinuousQueuedDataReaderSuite extends StreamTest with MockitoSugar {
  case class LongPartitionOffset(offset: Long) extends PartitionOffset

  val coordinatorId = s"${getClass.getSimpleName}-epochCoordinatorIdForUnitTest"
  val startEpoch = 0

  var epochEndpoint: RpcEndpointRef = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    epochEndpoint = EpochCoordinatorRef.create(
      mock[StreamingWrite],
      mock[ContinuousStream],
      mock[ContinuousExecution],
      coordinatorId,
      startEpoch,
      spark,
      SparkEnv.get)
    EpochTracker.initializeCurrentEpoch(0)
  }

  override def afterEach(): Unit = {
    SparkEnv.get.rpcEnv.stop(epochEndpoint)
    epochEndpoint = null
    super.afterEach()
  }


  private val mockContext = mock[TaskContext]
  when(mockContext.getLocalProperty(ContinuousExecution.START_EPOCH_KEY))
    .thenReturn(startEpoch.toString)
  when(mockContext.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY))
    .thenReturn(coordinatorId)

  /**
   * Set up a ContinuousQueuedDataReader for testing. The blocking queue can be used to send
   * rows to the wrapped data reader.
   */
  private def setup(): (BlockingQueue[UnsafeRow], ContinuousQueuedDataReader) = {
    val queue = new ArrayBlockingQueue[UnsafeRow](1024)
    val partitionReader = new ContinuousPartitionReader[InternalRow] {
      var index = -1
      var curr: UnsafeRow = _

      override def next() = {
        curr = queue.take()
        index += 1
        true
      }

      override def get = curr

      override def getOffset = LongPartitionOffset(index)

      override def close() = {}
    }
    val reader = new ContinuousQueuedDataReader(
      0,
      partitionReader,
      new StructType().add("i", "int"),
      mockContext,
      dataQueueSize = sqlContext.conf.continuousStreamingExecutorQueueSize,
      epochPollIntervalMs = sqlContext.conf.continuousStreamingExecutorPollIntervalMs)

    (queue, reader)
  }

  private def unsafeRow(value: Int) = {
    UnsafeProjection.create(Array(IntegerType : DataType))(
      new GenericInternalRow(Array(value: Any)))
  }

  test("basic data read") {
    val (input, reader) = setup()

    input.add(unsafeRow(12345))
    assert(reader.next().getInt(0) == 12345)
  }

  test("basic epoch marker") {
    val (input, reader) = setup()

    epochEndpoint.askSync[Long](IncrementAndGetEpoch)
    assert(reader.next() == null)
  }

  test("new rows after markers") {
    val (input, reader) = setup()

    epochEndpoint.askSync[Long](IncrementAndGetEpoch)
    epochEndpoint.askSync[Long](IncrementAndGetEpoch)
    epochEndpoint.askSync[Long](IncrementAndGetEpoch)
    assert(reader.next() == null)
    assert(reader.next() == null)
    assert(reader.next() == null)
    input.add(unsafeRow(11111))
    input.add(unsafeRow(22222))
    assert(reader.next().getInt(0) == 11111)
    assert(reader.next().getInt(0) == 22222)
  }

  test("new markers after rows") {
    val (input, reader) = setup()

    input.add(unsafeRow(11111))
    input.add(unsafeRow(22222))
    assert(reader.next().getInt(0) == 11111)
    assert(reader.next().getInt(0) == 22222)
    epochEndpoint.askSync[Long](IncrementAndGetEpoch)
    epochEndpoint.askSync[Long](IncrementAndGetEpoch)
    epochEndpoint.askSync[Long](IncrementAndGetEpoch)
    assert(reader.next() == null)
    assert(reader.next() == null)
    assert(reader.next() == null)
  }

  test("alternating markers and rows") {
    val (input, reader) = setup()

    input.add(unsafeRow(11111))
    assert(reader.next().getInt(0) == 11111)
    input.add(unsafeRow(22222))
    assert(reader.next().getInt(0) == 22222)
    epochEndpoint.askSync[Long](IncrementAndGetEpoch)
    assert(reader.next() == null)
    input.add(unsafeRow(33333))
    assert(reader.next().getInt(0) == 33333)
    input.add(unsafeRow(44444))
    assert(reader.next().getInt(0) == 44444)
    epochEndpoint.askSync[Long](IncrementAndGetEpoch)
    assert(reader.next() == null)
  }
}
