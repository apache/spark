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

package org.apache.spark.shuffle

import java.net.ConnectException
import java.nio.ByteBuffer
import java.util.concurrent.ExecutorService

import scala.collection.mutable.ArrayBuffer

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach

import org.apache.spark._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockPushException, BlockStoreClient}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage._

class ShuffleWriterSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency: ShuffleDependency[Int, Int, Int] = _
  @Mock(answer = RETURNS_SMART_NULLS) private var shuffleClient: BlockStoreClient = _

  private val conf: SparkConf = new SparkConf(loadDefaults = false)

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.initMocks(this)
    when(dependency.partitioner).thenReturn(new HashPartitioner(8))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    when(dependency.getMergerLocs).thenReturn(Seq(BlockManagerId("test-client", "test-client", 1)))
    conf.set("spark.shuffle.push.based.enabled", "true")
    conf.set("spark.shuffle.service.enabled", "true")
    // Set the env because the shuffler writer gets the shuffle client instance from the env.
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)
    when(blockManager.blockStoreClient).thenReturn(shuffleClient)
  }

  test("test block push") {
    val testWriter = new TestShuffleWriter(dependency.partitioner.numPartitions)
    val allBlocks = new ArrayBuffer[String]

    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val pushedBlocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        allBlocks ++= pushedBlocks
        val managedBuffers = invocation.getArguments()(3).asInstanceOf[Array[ManagedBuffer]]
        val blockFetchListener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        (pushedBlocks, managedBuffers).zipped.foreach((blockId, buffer) => {
          blockFetchListener.onBlockFetchSuccess(blockId, buffer)
        })
      })
    testWriter.initiateBlockPush(
      blockResolver, testWriter.getPartitionLengths(), dependency, 0, 0, conf)

    verify(shuffleClient, times(1))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(allBlocks.length == dependency.partitioner.numPartitions)
    testWriter.stop(true)
  }

  test("error retries") {
    val testWriter = new TestShuffleWriter(dependency.partitioner.numPartitions)
    val errorHandler = testWriter.createErrorHandler()
    assert(
      !errorHandler.shouldRetryError(new RuntimeException(
        new IllegalArgumentException(BlockPushException.TOO_LATE_MESSAGE_SUFFIX))))
    assert(errorHandler.shouldRetryError(new RuntimeException(new ConnectException())))
    assert(
      errorHandler.shouldRetryError(new RuntimeException(
        new IllegalArgumentException(BlockPushException.COULD_NOT_FIND_OPPORTUNITY_MSG_PREFIX))))
    assert (errorHandler.shouldRetryError(new Throwable()))
  }

  test("error logging") {
    val testWriter = new TestShuffleWriter(dependency.partitioner.numPartitions)
    val errorHandler = testWriter.createErrorHandler()
    assert(
      !errorHandler.shouldLogError(new RuntimeException(
        new IllegalArgumentException(BlockPushException.TOO_LATE_MESSAGE_SUFFIX))))
    assert(
      !errorHandler.shouldLogError(new RuntimeException(
        new IllegalArgumentException(BlockPushException.COULD_NOT_FIND_OPPORTUNITY_MSG_PREFIX))))
    assert(errorHandler.shouldLogError(new Throwable()))
  }

  test("connect exceptions removes all the push requests for that host") {
    when(dependency.getMergerLocs).thenReturn(
      Seq(BlockManagerId("client1", "client1", 1), BlockManagerId("client2", "client2", 2)))
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "2")
    val executorService = mock(classOf[ExecutorService])
    when(executorService.submit(any[Runnable]())).thenAnswer(new Answer[Unit] {
      override def answer(invocationOnMock: InvocationOnMock): Unit = {
      }
    })
    val testWriter = new TestShuffleWriter(dependency.partitioner.numPartitions)
    val allBlocks = new ArrayBuffer[String]

    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val pushedBlocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        allBlocks ++= pushedBlocks
        val blockFetchListener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        pushedBlocks.foreach(blockId => {
          blockFetchListener.onBlockFetchFailure(
            blockId, new RuntimeException(new ConnectException()))
        })
      })
    testWriter.initiateBlockPush(
      blockResolver, testWriter.getPartitionLengths(), dependency, 0, 0, conf)
    verify(shuffleClient, times(2))
      .pushBlocks(any(), any(), any(), any(), any())
    // 2 blocks for each merger locations
    assert(allBlocks.length == 4)
  }

  private class TestShuffleWriter(
    private val numPartitions: Int) extends ShuffleWriter[Int, Int] {

    override protected def submitTask(task: Runnable): Unit = {
     // Making this synchronous for testing
      task.run()
    }

    /** Write a sequence of records to this task's output */
    override def write(records: Iterator[Product2[Int, Int]]): Unit = {}

    /** Close this writer, passing along whether the map completed */
    override def stop(success: Boolean): Option[MapStatus] = {
      Option.empty
    }

    override def getPartitionLengths(): Array[Long] = {
      Array.fill(numPartitions) {
        2
      }
    }

    override protected def sliceReqBufferIntoBlockBuffers(
      reqBuffer: ManagedBuffer, blockSizes: Seq[Long]) = {
      Array.fill(blockSizes.length) {
        new NioManagedBuffer(ByteBuffer.wrap(Array[Byte](2)))
      }
    }
  }
}
