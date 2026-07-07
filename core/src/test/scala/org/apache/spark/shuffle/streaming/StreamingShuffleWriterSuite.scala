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

package org.apache.spark.shuffle.streaming

import java.util.Properties
import java.util.zip.CRC32C

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{Channel, ChannelConfig, ChannelFuture}
import io.netty.util.concurrent.GenericFutureListener
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, STREAMING_SHUFFLE_CHECKSUM_ENABLED, STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.client.TransportClient
import org.apache.spark.network.shuffle.streaming.{DataMessage, StreamingShuffleMessage, TerminationControlMessage}
import org.apache.spark.shuffle.streaming.StreamingShuffleManager.QUERY_ID_PROPERTY_KEY
import org.apache.spark.util.ErrorNotifier

/**
 * Writer-side unit tests that do not require a shuffle reader. End-to-end writer <-> reader
 * behavior is covered by `StreamingShuffleSuite` in the tests PR of this stack.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
  with LocalSparkContext
  with Matchers
  with MockitoSugar {

  private def newConf(): SparkConf =
    new SparkConf().set(SHUFFLE_MANAGER, classOf[StreamingShuffleManager].getName)

  private def createTaskContext(conf: SparkConf, partitionId: Int): TaskContextImpl = {
    val properties = new Properties()
    properties.setProperty(QUERY_ID_PROPERTY_KEY, "test-query-id")
    val taskMemoryManager = new TaskMemoryManager(new TestMemoryManager(conf), 0)
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId,
      taskAttemptId = 0,
      attemptNumber = 0,
      numPartitions = 1,
      taskMemoryManager = taskMemoryManager,
      localProperties = properties,
      metricsSystem = mock[MetricsSystem],
      cpus = 1)
  }

  test("getWriter returns a StreamingShuffleWriter") {
    withSpark(new SparkContext("local", "StreamingShuffleWriterSuite", newConf())) { sc =>
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId = 0, numMaps = 1, numReduces = 1, jobId = 0)
      val rdd = sc.parallelize(1 to 4).map(x => (x, x))
      val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(1))
      val handle = new StreamingShuffleHandle(0, dep)
      val context = createTaskContext(sc.conf, 0)
      try {
        val writer = new StreamingShuffleManager()
          .getWriter[Int, Int](handle, 0, context, null)
        writer shouldBe a[StreamingShuffleWriter[_, _]]
      } finally {
        // Constructing the writer starts a Netty server; the task-completion listener
        // (cleanupResources) tears it down.
        context.markTaskCompleted(None)
      }
    }
  }

  test("writer rejects a memory budget that overflows the Int range") {
    // With a large network buffer size, numPartitions * BUFFER_SIZE * 2 exceeds Int.MaxValue
    // for even a handful of readers. The writer must reject this up front rather than let the
    // 32-bit product wrap negative and hang on a semaphore that can never grant permits. The
    // guard fires in the constructor before the Netty server is started, so nothing to clean up.
    val conf = newConf().set(STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE, 256 * 1024 * 1024)
    withSpark(new SparkContext("local", "StreamingShuffleWriterSuite", conf)) { sc =>
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId = 0, numMaps = 1, numReduces = 16, jobId = 0)
      val rdd = sc.parallelize(1 to 4).map(x => (x, x))
      val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(16))
      val handle = new StreamingShuffleHandle(0, dep)
      val context = createTaskContext(sc.conf, 0)
      val e = intercept[IllegalArgumentException] {
        new StreamingShuffleWriter[Int, Int](handle, 0, context)
      }
      assert(e.getMessage.contains("memory budget"))
    }
  }

  // Builds a single-partition writer against a freshly registered shuffle. The caller must run
  // this inside a withSpark block and must eventually call context.markTaskCompleted(None) to
  // tear down the Netty server the writer starts in its constructor.
  private def newWriter(
      sc: SparkContext,
      context: TaskContext,
      errorNotifier: ErrorNotifier = new ErrorNotifier()): StreamingShuffleWriter[Int, Int] = {
    SparkEnv.get.streamingShuffleOutputTracker.get
      .asInstanceOf[StreamingShuffleOutputTrackerMaster]
      .registerShuffle(shuffleId = 0, numMaps = 1, numReduces = 1, jobId = 0)
    val rdd = sc.parallelize(1 to 4).map(x => (x, x))
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(1))
    val handle = new StreamingShuffleHandle(0, dep)
    new StreamingShuffleWriter[Int, Int](handle, 0, context, errorNotifier = errorNotifier)
  }

  // A mock TransportClient whose send(ByteBuf) invokes `onSend` and then completes the write
  // successfully (its returned ChannelFuture reports isSuccess = true so the writer's normal
  // done()/buffer-recycling path runs). Binding it into a shard's futureClients makes the shard
  // send directly to this mock instead of over the network.
  private def bindMockClient(
      writer: StreamingShuffleWriter[Int, Int],
      shardId: Int)(onSend: ByteBuf => Unit): Unit = {
    val client = mock[TransportClient]
    val channel = mock[Channel]
    val channelConfig = mock[ChannelConfig]
    when(client.getChannel).thenReturn(channel)
    when(channel.config).thenReturn(channelConfig)
    val succeededFuture = mock[ChannelFuture]
    when(succeededFuture.isSuccess).thenReturn(true)
    when(succeededFuture.addListener(any[GenericFutureListener[ChannelFuture]]))
      .thenAnswer { invocation =>
        invocation.getArgument[GenericFutureListener[ChannelFuture]](0)
          .operationComplete(succeededFuture)
        succeededFuture
      }
    when(client.send(any[ByteBuf])).thenAnswer { invocation =>
      onSend(invocation.getArgument[ByteBuf](0))
      succeededFuture
    }
    writer.transportServerHandler.futureClients(shardId).complete(client)
  }

  test("a synchronous send failure is surfaced through the ErrorNotifier") {
    withSpark(new SparkContext("local", "StreamingShuffleWriterSuite", newConf())) { sc =>
      val context = createTaskContext(sc.conf, 0)
      val errorNotifier = new ErrorNotifier()
      try {
        val writer = newWriter(sc, context, errorNotifier)
        // Bind a client whose send throws synchronously; the shard's send must record the failure
        // via the ErrorNotifier and rethrow.
        val client = mock[TransportClient]
        val channel = mock[Channel]
        val channelConfig = mock[ChannelConfig]
        when(client.getChannel).thenReturn(channel)
        when(channel.config).thenReturn(channelConfig)
        when(client.send(any[ByteBuf])).thenThrow(new RuntimeException("send failed"))
        writer.transportServerHandler.futureClients(0).complete(client)

        intercept[RuntimeException] {
          writer.shards(0).send(new TerminationControlMessage(0, 0))
        }
        // The failure is recorded synchronously on this thread (the future is already complete).
        writer.errorNotifier.getError() shouldBe defined
        writer.errorNotifier.getError().get.getMessage should include("send failed")
      } finally {
        context.markTaskCompleted(None)
      }
    }
  }

  test("an asynchronous write failure is surfaced through the ErrorNotifier") {
    withSpark(new SparkContext("local", "StreamingShuffleWriterSuite", newConf())) { sc =>
      val context = createTaskContext(sc.conf, 0)
      val errorNotifier = new ErrorNotifier()
      try {
        val writer = newWriter(sc, context, errorNotifier)
        // Bind a client whose send is accepted but whose write completes unsuccessfully (the
        // common network-failure case). The shard's send-completion listener must record the
        // future's cause via the ErrorNotifier.
        val client = mock[TransportClient]
        val channel = mock[Channel]
        val channelConfig = mock[ChannelConfig]
        when(client.getChannel).thenReturn(channel)
        when(channel.config).thenReturn(channelConfig)
        val failedFuture = mock[ChannelFuture]
        when(failedFuture.isSuccess).thenReturn(false)
        when(failedFuture.cause()).thenReturn(new RuntimeException("write failed"))
        when(failedFuture.addListener(any[GenericFutureListener[ChannelFuture]]))
          .thenAnswer { invocation =>
            invocation.getArgument[GenericFutureListener[ChannelFuture]](0)
              .operationComplete(failedFuture)
            failedFuture
          }
        when(client.send(any[ByteBuf])).thenReturn(failedFuture)
        writer.transportServerHandler.futureClients(0).complete(client)

        writer.shards(0).send(new TerminationControlMessage(0, 0))

        writer.errorNotifier.getError() shouldBe defined
        writer.errorNotifier.getError().get.getMessage should include("write failed")
      } finally {
        context.markTaskCompleted(None)
      }
    }
  }

  test("cleanupResources releases queued pooled buffers") {
    withSpark(new SparkContext("local", "StreamingShuffleWriterSuite", newConf())) { sc =>
      val context = createTaskContext(sc.conf, 0)
      val writer = newWriter(sc, context)
      val buf = Unpooled.buffer(16)
      writer.bufferPool.offerLast(buf)
      buf.refCnt() should be(1)
      writer.bufferPool.size() should be(1)

      // cleanupResources is idempotent; call it directly so we can observe the buffer's refcount.
      writer.cleanupResources()

      writer.bufferPool.size() should be(0)
      buf.refCnt() should be(0)
      // The task-completion listener will call cleanupResources again (a no-op now).
      context.markTaskCompleted(None)
    }
  }

  test("checksum is computed and embedded in the DataMessage sent on the wire") {
    val conf = newConf().set(STREAMING_SHUFFLE_CHECKSUM_ENABLED, true)
    withSpark(new SparkContext("local", "StreamingShuffleWriterSuite", conf)) { sc =>
      val context = createTaskContext(sc.conf, 0)
      try {
        val writer = newWriter(sc, context)
        val sentBuffers = new java.util.concurrent.CopyOnWriteArrayList[ByteBuf]()
        // Capture a copy of every buffer the writer sends (the original is released by the
        // transport layer after the write completes). The send completes synchronously since the
        // client future is already resolved.
        bindMockClient(writer, 0) { buf => sentBuffers.add(buf.copy()) }

        // Serialize a record through the writer's own buffer/checksum path and send it.
        val tsBuffer = writer.TimestampedBuffer(Unpooled.directBuffer(1024))
        tsBuffer.serializationStream.writeKey(1.asInstanceOf[Any])
        tsBuffer.serializationStream.writeValue(2.asInstanceOf[Any])
        tsBuffer.serializationStream.flush()
        writer.shards(0).send(tsBuffer)

        sentBuffers.size() should be(1)
        val decoded = StreamingShuffleMessage.decode(sentBuffers.get(0))
        decoded shouldBe a[DataMessage]
        val dataMessage = decoded.asInstanceOf[DataMessage]

        // The checksum embedded on the wire must match an independent CRC32C over the payload.
        // Compute the expected value with java.util.zip.CRC32C directly (not ShuffleChecksum) so
        // this stays independent of the production checksum wrapper.
        val payload = dataMessage.getRecordData()
        val expectedCrc = new CRC32C()
        expectedCrc.update(payload.nioBuffer(payload.readerIndex(), payload.readableBytes()))
        dataMessage.checksum should be(expectedCrc.getValue)
        dataMessage.checksum should not be 0L

        dataMessage.release()
        sentBuffers.forEach(_.release())
      } finally {
        context.markTaskCompleted(None)
      }
    }
  }
}
