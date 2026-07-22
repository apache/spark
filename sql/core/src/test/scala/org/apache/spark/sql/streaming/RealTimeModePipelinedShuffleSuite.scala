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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}

/**
 * Tests that a stateful Real-Time Mode query (streaming `dropDuplicates`) runs when its repartition
 * shuffle is a PipelinedShuffleDependency, so the source-scan producer stage and the dedup consumer
 * stage are co-scheduled and stream records through a transient shuffle instead of the consumer
 * waiting for the producer to fully materialize.
 *
 * The Real-Time Mode operator allowlist check is left enabled, so the test also confirms that the
 * dedup plan's operators (StreamingDeduplicateExec, StateStoreRestore/Save, ShuffleExchangeExec)
 * are admitted. It drives the source with the standard `testStream` DSL, which advances the query
 * across multiple batches.
 */
class RealTimeModePipelinedShuffleSuite extends StreamRealTimeModeSuiteBase {
  import testImplicits._

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
        // Batch 0: three distinct keys, each sent twice -> dedup emits each once.
        AddData(inputData, ("a", 1), ("b", 1), ("c", 1), ("a", 2), ("b", 2), ("c", 2)),
        StartStream(),
        CheckAnswer("a", "b", "c"),
        // Batch 1: all duplicates of already-seen keys plus one new key -> only "d" is new.
        AddData(inputData, ("a", 3), ("b", 3), ("c", 3), ("d", 1)),
        CheckAnswer("a", "b", "c", "d"),
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
}
