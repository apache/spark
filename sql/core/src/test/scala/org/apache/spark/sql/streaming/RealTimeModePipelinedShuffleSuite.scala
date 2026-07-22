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

import java.io.{File, IOException}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}
import org.apache.spark.sql.execution.streaming.state.{FailureInjectionCheckpointFileManager, FailureInjectionFileSystem, FailureInjectionState}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf

/** Driver-side switch a UDF reads on executors to fail a task on demand (for fault-tolerance). */
object RealTimeModePipelinedShuffleSuite {
  @volatile var failTasks: Boolean = false
}

/**
 * Tests that a stateful Real-Time Mode query (streaming `dropDuplicates`) runs when its repartition
 * shuffle is a PipelinedShuffleDependency, so the source-scan producer stage and the dedup consumer
 * stage are co-scheduled and stream records through a transient shuffle instead of the consumer
 * waiting for the producer to fully materialize.
 *
 * The Real-Time Mode operator allowlist check is left enabled, so the tests also confirm that the
 * dedup plan's operators (StreamingDeduplicateExec, StateStoreRestore/Save, ShuffleExchangeExec)
 * are admitted. They drive the source with the standard `testStream` DSL, which advances the query
 * across multiple batches.
 */
class RealTimeModePipelinedShuffleSuite extends StreamRealTimeModeManualClockSuiteBase {
  import testImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach()
    RealTimeModePipelinedShuffleSuite.failTasks = false
  }

  test("stateful dedup runs in Real-Time Mode over a pipelined shuffle") {
    // Track, from the driver, whether the producer (source scan) and consumer (dedup) stages of the
    // pipelined group were ever RUNNING simultaneously. runningStages holds currently-running stage
    // ids (added on submit, removed on completion); maxConcurrentStages records the peak. A
    // sequential producer-then-consumer schedule never exceeds one running stage at a time.
    val runningStages = ConcurrentHashMap.newKeySet[Int]()
    val maxConcurrentStages = new AtomicInteger(0)
    val listener = new SparkListener {
      override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit = {
        runningStages.add(e.stageInfo.stageId)
        maxConcurrentStages.accumulateAndGet(runningStages.size(), Math.max)
      }
      override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
        runningStages.remove(e.stageInfo.stageId)
      }
    }
    spark.sparkContext.addSparkListener(listener)

    try {
      val inputData = LowLatencyMemoryStream[(String, Int)]

      // scan --shuffle(repartition by key)--> streaming dropDuplicates --> sink.
      // dropDuplicates on "key" forces a hash-partitioning ShuffleExchangeExec, which the
      // IncrementalExecution rule marks pipelined for a Real-Time Mode batch.
      val result = inputData
        .toDF()
        .select($"_1".as("key"))
        .dropDuplicates("key")
        .select($"key")

      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        // Batch 0: three distinct keys, each sent twice -> dedup emits each once. A Real-Time Mode
        // batch runs for a fixed duration and emits in real time, so CheckAnswerWithTimeout polls
        // the sink (rather than blocking for batch completion) and advanceRealTimeClock ends it.
        AddData(inputData, ("a", 1), ("b", 1), ("c", 1), ("a", 2), ("b", 2), ("c", 2)),
        StartStream(),
        CheckAnswerWithTimeout(60000, "a", "b", "c"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(0),
        // Batch 1: all duplicates of already-seen keys plus one new key -> only "d" is new.
        AddData(inputData, ("a", 3), ("b", 3), ("c", 3), ("d", 1)),
        CheckAnswerWithTimeout(60000, "a", "b", "c", "d"),
        // Every Real-Time Mode shuffle exchange in the executed plan is pipelined, and the producer
        // + consumer stages were genuinely co-scheduled (>= 2 stages ran at once).
        Execute { q =>
          val executedPlan = q.lastExecution.executedPlan
          val exchanges = executedPlan.collect { case s: ShuffleExchangeExec => s }
          assert(exchanges.nonEmpty, "expected a shuffle exchange in the dedup plan")
          assert(exchanges.forall(_.pipelined),
            "expected all Real-Time Mode shuffle exchanges to be pipelined, got: " +
              exchanges.map(e => s"pipelined=${e.pipelined}").mkString(", "))
          assert(maxConcurrentStages.get() >= 2,
            s"expected >= 2 stages running concurrently, saw max ${maxConcurrentStages.get()}")
        },
        StopStream
      )
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("dedup over a pipelined shuffle recovers from a task failure via checkpoint restart") {
    withTempDir { checkpointDir =>
      // A UDF that throws on demand, to fail a task mid-batch. Placed after the dedup so the
      // failure lands in the pipelined consumer stage while the query is running.
      val failUDF = udf { (key: String) =>
        if (RealTimeModePipelinedShuffleSuite.failTasks) {
          throw new RuntimeException(s"forced task failure on $key")
        }
        key
      }

      val inputData = LowLatencyMemoryStream[(String, Int)]
      val result = inputData
        .toDF()
        .select($"_1".as("key"))
        .dropDuplicates("key")
        .select(failUDF($"key").as("key"))

      // First run: dedup "a","b" (batch 0 commits), then a task failure fails the query. RTM does
      // not retry tasks, so a single task failure fails the whole batch/query.
      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, ("a", 1), ("b", 1), ("a", 2)),
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        CheckAnswerWithTimeout(60000, "a", "b"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(0),
        Execute { _ => RealTimeModePipelinedShuffleSuite.failTasks = true },
        AddData(inputData, ("c", 1)),
        advanceRealTimeClock,
        ExpectFailure[SparkException] { ex =>
          val msg = Option(ex.getCause).map(_.getMessage).getOrElse(ex.getMessage)
          assert(msg != null && msg.contains("forced task failure"),
            s"expected a forced task failure, got: $msg")
        }
      )

      // Restart from the same checkpoint. The dedup state from batch 0 must survive: "a" and "b"
      // are already seen and must NOT be re-emitted; only the genuinely-new "c","d" appear. This
      // proves recovery-to-last-committed-batch works when the shuffle is pipelined.
      RealTimeModePipelinedShuffleSuite.failTasks = false
      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, ("a", 3), ("b", 3), ("c", 3), ("d", 1)),
        CheckAnswerWithTimeout(60000, "c", "d"),
        advanceRealTimeClock,
        StopStream
      )
    }
  }

  /** Run `f` with a temp dir whose checkpoint file ops can be fault-injected via injectionState. */
  private def withTempDirAllowFailureInjection(f: (File, FailureInjectionState) => Unit): Unit = {
    withTempDir { dir =>
      val injectionState = FailureInjectionFileSystem.registerTempPath(dir.getPath)
      try {
        f(dir, injectionState)
      } finally {
        FailureInjectionFileSystem.removePathFromTempToInjectionState(dir.getPath)
      }
    }
  }

  test("dedup over a pipelined shuffle recovers from a commit-log write failure") {
    withSQLConf(
      SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key ->
        classOf[FailureInjectionCheckpointFileManager].getName) {
      withTempDirAllowFailureInjection { (checkpointDir, injectionState) =>
        val inputData = LowLatencyMemoryStream[(String, Int)]
        val result = inputData
          .toDF()
          .select($"_1".as("key"))
          .dropDuplicates("key")
          .select($"key")

        // Batch 0 dedups "a","b" and commits. Then fail the close() of batch 1's commit-log write,
        // so batch 1 cannot commit and the query fails after processing.
        injectionState.createAtomicDelayCloseRegex = Seq(".*/commits/1")

        testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          AddData(inputData, ("a", 1), ("b", 1)),
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          CheckAnswerWithTimeout(60000, "a", "b"),
          advanceRealTimeClock,
          WaitUntilBatchProcessed(0),
          AddData(inputData, ("c", 1)),
          CheckAnswerWithTimeout(60000, "a", "b", "c"),
          advanceRealTimeClock,
          // The injected close() failure surfaces as the underlying IOException failing the batch.
          ExpectFailure[IOException]()
        )

        // Clear the injection and restart from the same checkpoint. Batch 0's dedup state must
        // survive (a, b already seen); the uncommitted batch 1 is re-run, so its new key "c" is
        // still emitted, and a further new key "d" is added.
        injectionState.createAtomicDelayCloseRegex = Seq.empty
        testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, ("a", 2), ("b", 2), ("c", 2), ("d", 1)),
          CheckAnswerWithTimeout(60000, "c", "d"),
          advanceRealTimeClock,
          StopStream
        )
      }
    }
  }
}
