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

import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.shuffle.streaming.StreamingShuffleManager.QUERY_ID_PROPERTY_KEY

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
}
