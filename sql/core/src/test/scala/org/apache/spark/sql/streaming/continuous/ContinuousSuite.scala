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

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.v2.ContinuousScanExec
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf.CONTINUOUS_STREAMING_EPOCH_BACKLOG_QUEUE_SIZE
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
          case ContinuousScanExec(_, _, r: RateStreamContinuousStream, _) => r
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

  override protected val defaultTrigger = Trigger.Continuous(100)
}

class ContinuousSuite extends ContinuousSuiteBase {
  import testImplicits._

  test("basic") {
    val input = ContinuousMemoryStream[Int]

    testStream(input.toDF())(
      AddData(input, 0, 1, 2),
      CheckAnswer(0, 1, 2),
      StopStream,
      AddData(input, 3, 4, 5),
      StartStream(),
      CheckAnswer(0, 1, 2, 3, 4, 5))
  }

  test("map") {
    val input = ContinuousMemoryStream[Int]
    val df = input.toDF().map(_.getInt(0) * 2)

    testStream(df)(
      AddData(input, 0, 1),
      CheckAnswer(0, 2),
      StopStream,
      AddData(input, 2, 3, 4),
      StartStream(),
      CheckAnswer(0, 2, 4, 6, 8))
  }

  test("flatMap") {
    val input = ContinuousMemoryStream[Int]
    val df = input.toDF().flatMap(r => Seq(0, r.getInt(0), r.getInt(0) * 2))

    testStream(df)(
      AddData(input, 0, 1),
      CheckAnswer((0 to 1).flatMap(n => Seq(0, n, n * 2)): _*),
      StopStream,
      AddData(input, 2, 3, 4),
      StartStream(),
      CheckAnswer((0 to 4).flatMap(n => Seq(0, n, n * 2)): _*))
  }

  test("filter") {
    val input = ContinuousMemoryStream[Int]
    val df = input.toDF().where('value > 2)

    testStream(df)(
      AddData(input, 0, 1),
      CheckAnswer(),
      StopStream,
      AddData(input, 2, 3, 4),
      StartStream(),
      CheckAnswer(3, 4))
  }

  test("deduplicate") {
    val input = ContinuousMemoryStream[Int]
    val df = input.toDF().dropDuplicates()

    val except = intercept[AnalysisException] {
      testStream(df)(StartStream())
    }

    assert(except.message.contains(
      "Continuous processing does not support Deduplicate operations."))
  }

  test("timestamp") {
    val input = ContinuousMemoryStream[Int]
    val df = input.toDF().select(current_timestamp())

    val except = intercept[AnalysisException] {
      testStream(df)(StartStream())
    }

    assert(except.message.contains(
      "Continuous processing does not support current time operations."))
  }

  test("subquery alias") {
    val input = ContinuousMemoryStream[Int]
    input.toDF().createOrReplaceTempView("memory")
    val test = spark.sql("select value from memory where value > 2")

    testStream(test)(
      AddData(input, 0, 1),
      CheckAnswer(),
      StopStream,
      AddData(input, 2, 3, 4),
      StartStream(),
      CheckAnswer(3, 4))
  }

  test("repeatedly restart") {
    val input = ContinuousMemoryStream[Int]
    val df = input.toDF()

    testStream(df)(
      StartStream(),
      AddData(input, 0, 1),
      CheckAnswer(0, 1),
      StopStream,
      StartStream(),
      StopStream,
      StartStream(),
      StopStream,
      StartStream(),
      StopStream,
      AddData(input, 2, 3),
      StartStream(),
      CheckAnswer(0, 1, 2, 3),
      StopStream)
  }

  test("task failure kills the query") {
    val input = ContinuousMemoryStream[Int]
    val df = input.toDF()

    // Get an arbitrary task from this query to kill. It doesn't matter which one.
    var taskId: Long = -1
    val listener = new SparkListener() {
      override def onTaskStart(start: SparkListenerTaskStart): Unit = {
        taskId = start.taskInfo.taskId
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      testStream(df)(
        StartStream(Trigger.Continuous(100)),
        AddData(input, 0, 1, 2, 3),
        Execute { _ =>
          // Wait until a task is started, then kill its first attempt.
          eventually(timeout(streamingTimeout)) {
            assert(taskId != -1)
          }
          spark.sparkContext.killTaskAttempt(taskId)
        },
        ExpectFailure[SparkException] { e =>
          e.getCause != null && e.getCause.getCause.isInstanceOf[ContinuousTaskRetryException]
        })
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
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

    testStream(df)(
      StartStream(longContinuousTrigger),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 10)),
      IncrementEpoch(),
      StopStream,
      CheckAnswerRowsContains(scala.Range(0, 2500).map(Row(_)))
    )
  }

  test("automatic epoch advancement") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "500")
      .load()
      .select('value)

    testStream(df)(
      StartStream(Trigger.Continuous(2012)),
      AwaitEpoch(0),
      Execute(waitForRateSourceTriggers(_, 10)),
      IncrementEpoch(),
      StopStream,
      CheckAnswerRowsContains(scala.Range(0, 2500).map(Row(_))))
  }

  test("restarts") {
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "500")
      .load()
      .select('value)

    testStream(df)(
      StartStream(Trigger.Continuous(1012)),
      AwaitEpoch(2),
      StopStream,
      StartStream(Trigger.Continuous(1012)),
      AwaitEpoch(4),
      StopStream,
      StartStream(Trigger.Continuous(1012)),
      AwaitEpoch(5),
      StopStream,
      StartStream(Trigger.Continuous(1012)),
      AwaitEpoch(6),
      StopStream,
      StartStream(Trigger.Continuous(1012)),
      AwaitEpoch(8),
      StopStream,
      StartStream(Trigger.Continuous(1012)),
      StopStream,
      StartStream(Trigger.Continuous(1012)),
      AwaitEpoch(15),
      StopStream,
      CheckAnswerRowsContains(scala.Range(0, 2500).map(Row(_))))
  }
}

class ContinuousMetaSuite extends ContinuousSuiteBase {
  import testImplicits._

  // We need to specify spark.sql.streaming.minBatchesToRetain to do the following test.
  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]",
      "continuous-stream-test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")
        .set("spark.sql.streaming.minBatchesToRetain", "2")))

  test("SPARK-24351: check offsetLog/commitLog retained in the checkpoint directory") {
    withTempDir { checkpointDir =>
      val input = ContinuousMemoryStream[Int]
      val df = input.toDF().mapPartitions(iter => {
        // Sleep the task thread for 300 ms to make sure epoch processing time 3 times
        // longer than epoch creating interval. So the gap between last committed
        // epoch and currentBatchId grows over time.
        Thread.sleep(300)
        iter.map(row => row.getInt(0) * 2)
      })

      testStream(df)(
        StartStream(trigger = Trigger.Continuous(100),
          checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(input, 1),
        CheckAnswer(2),
        // Make sure epoch 2 has been committed before the following validation.
        AwaitEpoch(2),
        StopStream,
        AssertOnQuery(q => {
          q.commitLog.getLatest() match {
            case Some((latestEpochId, _)) =>
              val commitLogValidateResult = q.commitLog.get(latestEpochId - 1).isDefined &&
                q.commitLog.get(latestEpochId - 2).isEmpty
              val offsetLogValidateResult = q.offsetLog.get(latestEpochId - 1).isDefined &&
                q.offsetLog.get(latestEpochId - 2).isEmpty
              commitLogValidateResult && offsetLogValidateResult
            case None => false
          }
        })
      )
    }
  }
}

class ContinuousEpochBacklogSuite extends ContinuousSuiteBase {
  import testImplicits._

  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[1]",
      "continuous-stream-test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))

  // This test forces the backlog to overflow by not standing up enough executors for the query
  // to make progress.
  test("epoch backlog overflow") {
    withSQLConf((CONTINUOUS_STREAMING_EPOCH_BACKLOG_QUEUE_SIZE.key, "10")) {
      val df = spark.readStream
        .format("rate")
        .option("numPartitions", "2")
        .option("rowsPerSecond", "500")
        .load()
        .select('value)

      testStream(df)(
        StartStream(Trigger.Continuous(1)),
        ExpectFailure[IllegalStateException] { e =>
          e.getMessage.contains("queue has exceeded its maximum")
        }
      )
    }
  }
}
