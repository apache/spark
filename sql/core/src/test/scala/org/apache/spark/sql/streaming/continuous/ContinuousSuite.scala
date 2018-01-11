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

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskStart}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.execution.streaming.sources.MemorySinkV2
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.test.TestSparkSession

class ContinuousSuiteBase extends StreamTest {
  // We need more than the default local[2] to be able to schedule all partitions simultaneously.
  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]",
      "continuous-stream-test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))

  protected def waitForRateSourceTriggers(query: StreamExecution, numTriggers: Int): Unit = {
    query match {
      case s: ContinuousExecution =>
        assert(numTriggers >= 2, "must wait for at least 2 triggers to ensure query is initialized")
        val reader = s.lastExecution.executedPlan.collectFirst {
          case DataSourceV2ScanExec(_, r: RateStreamContinuousReader) => r
        }.get

        val deltaMs = numTriggers * 1000 + 300
        while (System.currentTimeMillis < reader.creationTime + deltaMs) {
          Thread.sleep(reader.creationTime + deltaMs - System.currentTimeMillis)
        }
    }
  }

  // A continuous trigger that will only fire the initial time for the duration of a test.
  // This allows clean testing with manual epoch advancement.
  protected val longContinuousTrigger = Trigger.Continuous("1 hour")
}

class ContinuousSuite extends ContinuousSuiteBase {
  import testImplicits._

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
      CheckAnswerRowsContains(scala.Range(0, 10).map(Row(_))),
      StopStream,
      StartStream(longContinuousTrigger),
      AwaitEpoch(2),
      Execute(waitForRateSourceTriggers(_, 2)),
      IncrementEpoch(),
      CheckAnswerRowsContains(scala.Range(0, 20).map(Row(_))),
      StopStream)
  }

  test("map") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)
      .map(r => r.getLong(0) * 2)

    testStream(df, useV2Sink = true)(
      StartStream(longContinuousTrigger),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 2)),
      IncrementEpoch(),
      Execute(waitForRateSourceTriggers(_, 4)),
      IncrementEpoch(),
      CheckAnswerRowsContains(scala.Range(0, 40, 2).map(Row(_))))
  }

  test("flatMap") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)
      .flatMap(r => Seq(0, r.getLong(0), r.getLong(0) * 2))

    testStream(df, useV2Sink = true)(
      StartStream(longContinuousTrigger),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 2)),
      IncrementEpoch(),
      Execute(waitForRateSourceTriggers(_, 4)),
      IncrementEpoch(),
      CheckAnswerRowsContains(scala.Range(0, 20).flatMap(n => Seq(0, n, n * 2)).map(Row(_))))
  }

  test("filter") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)
      .where('value > 5)

    testStream(df, useV2Sink = true)(
      StartStream(longContinuousTrigger),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 2)),
      IncrementEpoch(),
      Execute(waitForRateSourceTriggers(_, 4)),
      IncrementEpoch(),
      CheckAnswerRowsContains(scala.Range(6, 20).map(Row(_))))
  }

  test("deduplicate") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)
      .dropDuplicates()

    val except = intercept[AnalysisException] {
      testStream(df, useV2Sink = true)(StartStream(longContinuousTrigger))
    }

    assert(except.message.contains(
      "Continuous processing does not support Deduplicate operations."))
  }

  test("timestamp") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "5")
      .load()
      .select(current_timestamp())

    val except = intercept[AnalysisException] {
      testStream(df, useV2Sink = true)(StartStream(longContinuousTrigger))
    }

    assert(except.message.contains(
      "Continuous processing does not support current time operations."))
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
      CheckAnswerRowsContains(scala.Range(0, 10).map(Row(_))),
      StopStream,
      StartStream(longContinuousTrigger),
      StopStream,
      StartStream(longContinuousTrigger),
      StopStream,
      StartStream(longContinuousTrigger),
      AwaitEpoch(2),
      Execute(waitForRateSourceTriggers(_, 2)),
      IncrementEpoch(),
      CheckAnswerRowsContains(scala.Range(0, 20).map(Row(_))),
      StopStream)
  }

  test("kill task") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)

    // Get an arbitrary task from this query to kill. It doesn't matter which one.
    var taskId: Long = -1
    val listener = new SparkListener() {
      override def onTaskStart(start: SparkListenerTaskStart): Unit = {
        taskId = start.taskInfo.taskId
      }
    }
    spark.sparkContext.addSparkListener(listener)

    testStream(df, useV2Sink = true)(
      StartStream(Trigger.Continuous(100)),
      Execute(waitForRateSourceTriggers(_, 2)),
      Execute { _ =>
        eventually(timeout(streamingTimeout)) { assert(taskId != -1) }
        spark.sparkContext.killTaskAttempt(taskId)
      },
      Execute(waitForRateSourceTriggers(_, 4)),
      IncrementEpoch(),
      CheckAnswerRowsContains(scala.Range(0, 20).map(Row(_))))

    spark.sparkContext.removeSparkListener(listener)
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
    assert(Set(0, 1, 2, 3).map(Row(_)).subsetOf(results.toSet))
  }
}

class ContinuousStressSuite extends ContinuousSuiteBase {
  import testImplicits._

  test("only one epoch") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "500")
      .load()
      .select('value)

    testStream(df, useV2Sink = true)(
      StartStream(longContinuousTrigger),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 201)),
      IncrementEpoch(),
      StopStream,
      CheckAnswerRowsContains(scala.Range(0, 25000).map(Row(_)))
    )
  }

  test("automatic epoch advancement") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "500")
      .load()
      .select('value)

    testStream(df, useV2Sink = true)(
      StartStream(Trigger.Continuous(2012)),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 201)),
      IncrementEpoch(),
      StopStream,
      CheckAnswerRowsContains(scala.Range(0, 25000).map(Row(_))))
  }

  test("restarts") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "500")
      .load()
      .select('value)

    testStream(df, useV2Sink = true)(
      StartStream(Trigger.Continuous(2012)),
      AwaitEpoch(10),
      StopStream,
      StartStream(Trigger.Continuous(2012)),
      AwaitEpoch(20),
      StopStream,
      StartStream(Trigger.Continuous(2012)),
      AwaitEpoch(21),
      StopStream,
      StartStream(Trigger.Continuous(2012)),
      AwaitEpoch(22),
      StopStream,
      StartStream(Trigger.Continuous(2012)),
      AwaitEpoch(25),
      StopStream,
      StartStream(Trigger.Continuous(2012)),
      StopStream,
      StartStream(Trigger.Continuous(2012)),
      AwaitEpoch(50),
      StopStream,
      CheckAnswerRowsContains(scala.Range(0, 25000).map(Row(_))))
  }
}
