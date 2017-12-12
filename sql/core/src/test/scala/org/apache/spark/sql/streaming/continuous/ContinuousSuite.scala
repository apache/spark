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

import java.io.{File, InterruptedIOException, IOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeoutException, TimeUnit}

import scala.reflect.ClassTag
import scala.util.control.ControlThrowable

import com.google.common.util.concurrent.UncheckedExecutionException
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class ContinuousSuite extends StreamTest {
  import testImplicits._

  // We need more than the default local[2] to be able to schedule all partitions simultaneously.
  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]",
      "continuous-stream-test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))

  private def waitForRateSourceTriggers(query: StreamExecution, numTriggers: Int): Unit = {
    query match {
      case s: ContinuousExecution =>
        assert(numTriggers >= 2, "must wait for at least 2 triggers to ensure query is initialized")
        val reader = s.lastExecution.executedPlan.collectFirst {
          case DataSourceV2ScanExec(_, r: ContinuousRateStreamReader) => r
        }.get

        assert(reader.lastStartTime != 0, "reader last start time not initialized yet")

        val deltaMs = (numTriggers - 1) * 1000 + 300
        while (System.currentTimeMillis < reader.lastStartTime + deltaMs) {
          Thread.sleep(reader.lastStartTime + deltaMs - System.currentTimeMillis)
        }
    }
  }

  // A continuous trigger that will only fire the initial time for the duration of a test.
  // This allows clean testing with manual epoch advancement.
  private val longContinuousTrigger = Trigger.Continuous("1 hour")

  test("basic rate source") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)

    testStream(df, useV2Sink = true)(
      StartStream(longContinuousTrigger),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 2)),
      IncrementEpoch(),
      CheckAnswer(scala.Range(0, 10): _*),
      StopStream,
      StartStream(longContinuousTrigger),
      AwaitEpoch(2),
      Execute(waitForRateSourceTriggers(_, 2)),
      IncrementEpoch(),
      CheckAnswer(scala.Range(0, 20): _*),
      StopStream)
  }

  test("repeatedly restart") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)

    testStream(df, useV2Sink = true)(
      StartStream(longContinuousTrigger),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 2)),
      IncrementEpoch(),
      CheckAnswer(scala.Range(0, 10): _*),
      StopStream,
      StartStream(longContinuousTrigger),
      StopStream,
      StartStream(longContinuousTrigger),
      StopStream,
      StartStream(longContinuousTrigger),
      AwaitEpoch(2),
      Execute(waitForRateSourceTriggers(_, 2)),
      IncrementEpoch(),
      CheckAnswer(scala.Range(0, 20): _*),
      StopStream)
  }

  test("rate latency") {
    import org.apache.spark.sql.functions.udf
    val current_timestamp = udf(() => System.currentTimeMillis())
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "2")
      .option("rowsPerSecond", "2")
      .load()
      .select('timestamp.cast("long") as 'ingest, 'value, current_timestamp() as 'processing)
    val query = df.writeStream
      .format("memory")
      .queryName("latency")
      .trigger(Trigger.Continuous(100))
      .start()
    val continuousExecution =
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.asInstanceOf[ContinuousExecution]
    continuousExecution.awaitEpoch(0)
    waitForRateSourceTriggers(continuousExecution, 2)
    query.stop()

    print(spark.read.table("latency").collect().mkString)
  }

  test("query without test harness") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "2")
      .option("rowsPerSecond", "2")
      .load()
      .select('value)
    val query = df.writeStream
      .format("memory")
      .queryName("noharness")
      .trigger(Trigger.Continuous(100))
      .start()
    val continuousExecution =
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.asInstanceOf[ContinuousExecution]
    continuousExecution.awaitEpoch(0)
    waitForRateSourceTriggers(continuousExecution, 2)
    query.stop()

    val results = spark.read.table("noharness").collect()
    assert(results.toSet == Set(0, 1, 2, 3).map(Row(_)))
  }
}

class ContinuousStressSuite extends StreamTest {

  import testImplicits._

  // We need more than the default local[2] to be able to schedule all partitions simultaneously.
  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]",
      "continuous-stream-test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))

  private def waitForRateSourceTriggers(query: StreamExecution, numTriggers: Int): Unit = {
    query match {
      case s: ContinuousExecution =>
        assert(numTriggers >= 2, "must wait for at least 2 triggers to ensure query is initialized")
        val reader = s.lastExecution.executedPlan.collectFirst {
          case DataSourceV2ScanExec(_, r: ContinuousRateStreamReader) => r
        }.get

        assert(reader.lastStartTime != 0, "reader last start time not initialized yet")

        val deltaMs = (numTriggers - 1) * 1000 + 300
        while (System.currentTimeMillis < reader.lastStartTime + deltaMs) {
          Thread.sleep(reader.lastStartTime + deltaMs - System.currentTimeMillis)
        }
    }
  }

  // A continuous trigger that will only fire the initial time for the duration of a test.
  // This allows clean testing with manual epoch advancement.
  private val longContinuousTrigger = Trigger.Continuous("1 hour")

  test("only one epoch") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "1")
      .option("rowsPerSecond", "100")
      .load()
      .select('value)

    testStream(df, useV2Sink = true)(
      StartStream(longContinuousTrigger),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 200)),
      IncrementEpoch(),
      CheckAnswer(scala.Range(0, 20000): _*))
  }

  test("automatic epoch advancement") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "1")
      .option("rowsPerSecond", "100")
      .load()
      .select('value)

    testStream(df, useV2Sink = true)(
      StartStream(Trigger.Continuous(2012)),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 200)),
      IncrementEpoch(),
      Execute { query =>
        // Because we have automatic advancement, we can't reliably guarantee another trigger won't
        // commit more than the 20K rows we expect before we can check. So we simply ensure that:
        //  * the highest value committed was at least 20000 - 1
        //  * all values below the highest are present
        val data = query.sink.asInstanceOf[MemorySinkV2].allData
        val max = data.map(_.getLong(0)).max
        assert(max >= 19999)
        assert(data.toSet == scala.Range(0, max.toInt + 1).map(Row(_)).toSet)
      })
  }
}
