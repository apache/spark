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
import java.util.concurrent.LinkedBlockingQueue

import io.netty.buffer.Unpooled
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.internal.config.SHUFFLE_MANAGER_INCREMENTAL
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.shuffle.streaming.{DataMessage, StreamingShuffleMessage, TerminationControlMessage}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.streaming.StreamingShuffleManager.QUERY_ID_PROPERTY_KEY

/**
 * Reader-side unit tests that do not require a shuffle writer. End-to-end writer <-> reader
 * behavior is covered by `StreamingShuffleSuite` in the tests PR of this stack.
 */
class StreamingShuffleReaderSuite
  extends SparkFunSuite
  with LocalSparkContext
  with Matchers
  with MockitoSugar {

  private def newConf(): SparkConf =
    // StreamingShuffleManager is pipelined, so it belongs in the incremental slot (the default
    // spark.shuffle.manager must be a BlockingShuffleManager). This is what initializes the
    // streaming output tracker that the reader constructor asserts.
    new SparkConf().set(SHUFFLE_MANAGER_INCREMENTAL, classOf[StreamingShuffleManager].getName)

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
      cpuAmount = 1)
  }

  // A minimal DataMessage that is only used to exercise the iterator factory's type dispatch;
  // its contents are never decoded here because handleDataMessage is stubbed in these tests.
  private def emptyDataMessage(): DataMessage =
    new DataMessage(0, 0, 0, Unpooled.EMPTY_BUFFER, 0L)

  // The iterator factory drives four collaborators; these tests supply in-memory fakes for all of
  // them so the reader's consumer-loop control flow can be verified without Netty or a SparkEnv.
  private val factory = new StreamingShuffleReaderIteratorFactory()

  test("iterator emits all rows from data messages then stops on termination") {
    val queue = new LinkedBlockingQueue[StreamingShuffleMessage]()
    queue.put(emptyDataMessage())
    queue.put(emptyDataMessage())
    queue.put(new TerminationControlMessage(0, 0))

    // Each data message yields a fixed pair of rows; the single termination message ends the read.
    val rowsPerMessage = Seq(Iterator((1, 1), (2, 2)), Iterator((3, 3), (4, 4)))
    val nextRows = rowsPerMessage.iterator
    val it = factory.create[Int, Int](
      queue,
      handleTerminationMessage = _ => true,
      handleDataMessage = _ => nextRows.next(),
      checkTaskFailure = () => ())

    it.toSeq should contain theSameElementsInOrderAs Seq((1, 1), (2, 2), (3, 3), (4, 4))
  }

  test("iterator does not stop until handleTerminationMessage returns true") {
    val queue = new LinkedBlockingQueue[StreamingShuffleMessage]()
    // Two writers: the first termination message must not end the read, only the second.
    queue.put(new TerminationControlMessage(0, 0))
    queue.put(emptyDataMessage())
    queue.put(new TerminationControlMessage(1, 0))

    var terminationsSeen = 0
    val it = factory.create[Int, Int](
      queue,
      handleTerminationMessage = _ => {
        terminationsSeen += 1
        terminationsSeen == 2 // only the second termination completes the read
      },
      handleDataMessage = _ => Iterator((1, 1)),
      checkTaskFailure = () => ())

    it.toSeq should contain theSameElementsInOrderAs Seq((1, 1))
    terminationsSeen should be(2)
  }

  test("iterator surfaces a background error before dequeuing the next message") {
    val queue = new LinkedBlockingQueue[StreamingShuffleMessage]()
    // A message is available in the queue, but a background error has already been recorded
    // (e.g. the channelInactive premature-disconnect path). Because checkTaskFailure runs before
    // every dequeue, the iterator must throw before this message is dequeued and handled, rather
    // than emitting stale data and only failing later.
    queue.put(emptyDataMessage())

    var dataHandled = false
    val it = factory.create[Int, Int](
      queue,
      handleTerminationMessage = _ => true,
      handleDataMessage = _ => {
        dataHandled = true
        Iterator((1, 1))
      },
      checkTaskFailure = () => throw new SparkException("background failure"))

    val e = intercept[SparkException] {
      it.hasNext
    }
    e.getMessage should include("background failure")
    // The pending message must not have been processed: the error was surfaced first. This is
    // what would keep the reader from emitting rows after a writer has already failed.
    dataHandled should be(false)
  }

  test("getReader routes to a StreamingShuffleReader wired with the given context and reduce " +
      "partition") {
    // A construction/routing smoke test: the manager must dispatch to the streaming reader (not a
    // fallback shuffle reader), the reader must construct successfully (its constructor asserts the
    // output tracker, starts the task-discovery and client-creation executors, and registers the
    // task-completion listener), and it must be wired with the context we passed in. The task runs
    // at partition 2 but reads reduce partition 1, as in a Union stage whose shuffled child is not
    // its first child, so the reader must take its reduce partition from getReader.
    withSpark(new SparkContext("local", "StreamingShuffleReaderSuite", newConf())) { sc =>
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId = 0, numMaps = 1, numReduces = 2, jobId = 0)
      val rdd = sc.parallelize(1 to 4).map(x => (x, x))
      val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2))
      val handle = new StreamingShuffleHandle(0, dep)
      val context = createTaskContext(sc.conf, 2)
      try {
        val reader = new StreamingShuffleManager()
          .getReader[Int, Int](handle, 0, 1, 1, 2, context, null)
        reader shouldBe a[StreamingShuffleReader[_, _]]
        val streamingReader = reader.asInstanceOf[StreamingShuffleReader[_, _]]
        streamingReader.context should be theSameInstanceAs context
        streamingReader.reducePartitionId should be(1)
      } finally {
        // Constructing the reader starts a background task-discovery thread; the task-completion
        // listener (cleanupResources) shuts it down.
        context.markTaskCompleted(None)
      }
    }
  }

  test("getReader rejects a range of more than one reduce partition") {
    // A reader connects to the writers as a single reduce partition, so it cannot serve a range.
    // The check runs before the reader is constructed, so no SparkEnv is needed.
    val handle = new ShuffleHandle(0) {}
    val e = intercept[SparkException] {
      new StreamingShuffleManager().getReader[Int, Int](handle, 0, 1, 0, 2, null, null)
    }
    checkError(
      e,
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> ("Streaming shuffle 0 must be read one reduce partition at a " +
        "time, but got reduce partitions [0, 2)")))
  }
}
