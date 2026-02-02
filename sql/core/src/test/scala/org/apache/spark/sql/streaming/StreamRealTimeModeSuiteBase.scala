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

package org.apache.spark.sql.streaming

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.mutable

import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.execution.datasources.v2.LowLatencyClock
import org.apache.spark.sql.execution.streaming.{LowLatencyMemoryStream, RealTimeTrigger}
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.GlobalSingletonManualClock
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.util.SystemClock

/**
 * Base class for tests that require real-time mode.
 */
trait StreamRealTimeModeSuiteBase extends StreamTest with Matchers {
  override protected val defaultTrigger = RealTimeTrigger.apply("4 seconds")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.STREAMING_REAL_TIME_MODE_MIN_BATCH_DURATION,
        defaultTrigger.batchDurationMs)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    LowLatencyClock.setClock(new SystemClock)
  }

  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]", // Ensure enough number of cores to ensure concurrent schedule of all tasks.
      "streaming-rtm-context",
      sparkConf.set("spark.sql.testkey", "true")))

  /**
   * Should only be used in real-time mode where the batch duration is long enough to ensure
   * eventually does not skip the batch due to long refresh interval.
   */
  def waitForTasksToStart(numTasks: Int): Unit = {
    eventually(timeout(60.seconds)) {
      val tasksRunning = spark.sparkContext.statusTracker
        .getExecutorInfos.map(_.numRunningTasks()).sum
      assert(tasksRunning == numTasks, s"tasksRunning: ${tasksRunning}")
    }
  }
}

/**
 * Must be a singleton object to ensure serializable when used in ForeachWriter.
 * Users must make sure different test suites use different sink names to avoid race conditions.
 */
object ResultsCollector extends ConcurrentHashMap[String, ConcurrentLinkedQueue[String]] {
  def reset(): Unit = {
    clear()
  }
}

/**
 * Base class that contains helper methods to test Real-Time Mode streaming queries.
 *
 * The general procedure to use this suite is as follows:
 * 1. Call createMemoryStream to create a memory stream with manual clock.
 * 2. Call runStreamingQuery to start a streaming query with custom logic.
 * 3. Call processBatches to add data to the memory stream and validate results.
 *
 * It uses foreach to collect results into [[ResultsCollector]]. It also tests whether
 * results are emitted in real-time by having longer batch durations than the waiting time.
 */
trait StreamRealTimeModeE2ESuiteBase extends StreamRealTimeModeSuiteBase {
  import testImplicits._

  override protected val defaultTrigger = RealTimeTrigger.apply("300 seconds")

  protected final def sinkName: String = getClass.getName + "Sink"

  override def beforeEach(): Unit = {
    super.beforeEach()
    ResultsCollector.reset()
  }

  // Create a ForeachWriter that collects results into ResultsCollector.
  def foreachWriter(sinkName: String): ForeachWriter[String] = new ForeachWriter[String] {
    override def open(partitionId: Long, epochId: Long): Boolean = {
      true
    }

    override def process(value: String): Unit = {
      val collector =
        ResultsCollector.computeIfAbsent(sinkName, (_) => new ConcurrentLinkedQueue[String]())
      collector.add(value)
    }

    override def close(errorOrNull: Throwable): Unit = {}
  }

  def createMemoryStream(numPartitions: Int = 5)
      : (LowLatencyMemoryStream[(String, Int)], GlobalSingletonManualClock) = {
    val clock = new GlobalSingletonManualClock()
    LowLatencyClock.setClock(clock)
    val read = LowLatencyMemoryStream[(String, Int)](numPartitions)
    (read, clock)
  }

  def runStreamingQuery(queryName: String, df: org.apache.spark.sql.DataFrame): StreamingQuery = {
    df.as[String]
      .writeStream
      .outputMode(OutputMode.Update())
      .foreach(foreachWriter(sinkName))
      .queryName(queryName)
      .trigger(defaultTrigger)
      .start()
  }

  // Add test data to the memory source and validate results
  def processBatches(
      query: StreamingQuery,
      read: LowLatencyMemoryStream[(String, Int)],
      clock: GlobalSingletonManualClock,
      numRowsPerBatch: Int,
      numBatches: Int,
      expectedResultsGenerator: (String, Int) => Array[String]): Unit = {
    val expectedResults = mutable.ListBuffer[String]()
    for (i <- 0 until numBatches) {
      for (key <- List("a", "b", "c")) {
        for (j <- 1 to numRowsPerBatch) {
          val value = i * numRowsPerBatch + j
          read.addData((key, value))
          expectedResults ++= expectedResultsGenerator(key, value)
        }
      }

      eventually(timeout(60.seconds)) {
        ResultsCollector
          .get(sinkName)
          .toArray(new Array[String](ResultsCollector.get(sinkName).size()))
          .toList
          .sorted should equal(expectedResults.sorted)
      }

      clock.advance(defaultTrigger.batchDurationMs)

      eventually(timeout(60.seconds)) {
        query
          .asInstanceOf[StreamingQueryWrapper]
          .streamingQuery
          .getLatestExecutionContext()
          .batchId should be(i + 1)
        query.lastProgress.sources(0).numInputRows should be(numRowsPerBatch * 3)
      }
    }
  }
}

abstract class StreamRealTimeModeManualClockSuiteBase extends StreamRealTimeModeSuiteBase {
  var clock = new GlobalSingletonManualClock()

  override def beforeAll(): Unit = {
    super.beforeAll()
    LowLatencyClock.setClock(clock)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    GlobalSingletonManualClock.reset()
  }

  val advanceRealTimeClock = new ExternalAction {
    override def runAction(): Unit = {
      clock.advance(defaultTrigger.batchDurationMs)
    }
  }
}

