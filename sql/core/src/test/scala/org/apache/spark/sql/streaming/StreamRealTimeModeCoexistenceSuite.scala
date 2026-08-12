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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.duration._

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamExecution,
  StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink,
  LowLatencyMemoryStream}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests that Real-Time Mode (RTM) and MicroBatch Mode (MBM) multi-stage queries can run in the
 * SAME cluster -- concurrently, in one SparkContext, sharing one set of executors.
 *
 * This is the coexistence property that makes RTM usable without dedicating a cluster to it. It
 * holds because a shuffle's implementation is chosen by DEPENDENCY TYPE, not by a cluster-wide
 * setting: `SparkEnv.shuffleManagerFor` routes a `PipelinedShuffleDependency` to the pipelined
 * manager (`spark.shuffle.manager.incremental`, the streaming shuffle) and every other
 * `ShuffleDependency` to the blocking manager (`spark.shuffle.manager`, sort shuffle). Both
 * managers are instantiated in the same JVM and neither query has to know about the other.
 *
 * Each test uses MULTI-STAGE queries on both sides, since a single-stage query has no shuffle and
 * so would not exercise routing at all. The RTM side is verified to be genuinely pipelined (rather
 * than merely running) by asserting on the `pipelined` flag of its exchanges, and the MBM side is
 * verified to be genuinely NOT pipelined -- a test that only checked both queries produced answers
 * would pass even if routing collapsed to a single manager.
 */
class StreamRealTimeModeCoexistenceSuite extends StreamRealTimeModeSuiteBase {

  import testImplicits._

  /** Every shuffle exchange in the query's last executed plan, with its `pipelined` flag. */
  private def exchangePipelinedFlags(q: StreamExecution): Seq[Boolean] =
    q.lastExecution.executedPlan.collect { case s: ShuffleExchangeExec => s.pipelined }

  /** Asserts the query has at least one shuffle and every one of them is pipelined. */
  private def assertAllExchangesPipelined(q: StreamExecution): Unit = {
    val flags = exchangePipelinedFlags(q)
    assert(flags.nonEmpty, "expected at least one shuffle exchange in the RTM plan")
    assert(flags.forall(identity),
      s"expected every RTM exchange to be pipelined, got: ${flags.mkString(", ")}")
  }

  /** Asserts the query has at least one shuffle and none of them is pipelined. */
  private def assertNoExchangePipelined(q: StreamExecution): Unit = {
    val flags = exchangePipelinedFlags(q)
    assert(flags.nonEmpty, "expected at least one shuffle exchange in the MBM plan")
    assert(!flags.exists(identity),
      s"expected no MBM exchange to be pipelined, got: ${flags.mkString(", ")}")
  }

  test("an RTM and an MBM multi-stage query run concurrently in the same cluster") {
    // Keep both queries' shuffles small: the RTM query's whole pipelined group is gang-admitted, so
    // its scan + dedup tasks and the MBM query's tasks must all fit the cluster's slots at once.
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val rtmInput = LowLatencyMemoryStream[(String, Int)]
      val mbmInput = MemoryStream[(String, Int)]

      // Both are multi-stage: a shuffle (repartition by key) feeding a stateful dedup.
      val rtmQuery = rtmInput.toDF().select($"_1".as("key")).dropDuplicates("key").select($"key")
      val mbmQuery = mbmInput.toDF().select($"_1".as("key")).dropDuplicates("key").select($"key")

      // Start the MBM query first and leave it running for the whole RTM test.
      val mbmHandle = mbmQuery.writeStream
        .format("memory")
        .queryName("coexistence_mbm")
        .outputMode(OutputMode.Update)
        .start()

      try {
        mbmInput.addData(("a", 1), ("b", 1), ("a", 2))
        mbmHandle.processAllAvailable()
        checkAnswer(spark.table("coexistence_mbm"), Seq(Row("a"), Row("b")))

        val mbmExec = mbmHandle.asInstanceOf[StreamingQueryWrapper].streamingQuery
        assertNoExchangePipelined(mbmExec)

        // With the MBM query still active, run an RTM query in the same context.
        testStream(rtmQuery, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          AddData(rtmInput, ("x", 1), ("y", 1), ("x", 2)),
          StartStream(),
          CheckAnswerWithTimeout(60000, "x", "y"),
          Execute { q =>
            assertAllExchangesPipelined(q)
            assert(mbmHandle.isActive, "the MBM query must still be running alongside RTM")
          },
          StopStream
        )

        // The MBM query must still make progress AFTER the RTM query has come and gone, proving the
        // pipelined shuffle did not disturb the blocking manager's state.
        mbmInput.addData(("c", 1), ("a", 3))
        mbmHandle.processAllAvailable()
        checkAnswer(spark.table("coexistence_mbm"), Seq(Row("a"), Row("b"), Row("c")))
        assertNoExchangePipelined(
          mbmHandle.asInstanceOf[StreamingQueryWrapper].streamingQuery)
      } finally {
        mbmHandle.stop()
        spark.sql("DROP TABLE IF EXISTS coexistence_mbm")
      }
    }
  }

  test("a batch query with a shuffle runs while an RTM query is active") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val rtmInput = LowLatencyMemoryStream[(String, Int)]
      val rtmQuery = rtmInput.toDF().select($"_1".as("key")).dropDuplicates("key").select($"key")

      // A multi-stage BATCH query: groupBy forces a blocking shuffle. Run it mid-RTM-batch.
      val batchResult = new AtomicReference[Seq[Row]](null)

      testStream(rtmQuery, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(rtmInput, ("x", 1), ("y", 1), ("x", 2)),
        StartStream(),
        CheckAnswerWithTimeout(60000, "x", "y"),
        Execute { q =>
          assertAllExchangesPipelined(q)
          // While the RTM batch is still open, a regular batch job with its own shuffle must run to
          // completion on the same executors, using the blocking shuffle manager.
          val df = spark.range(0, 100).selectExpr("id % 5 AS k").groupBy("k").agg(count("*"))
          batchResult.set(df.orderBy("k").collect().toSeq)
        },
        Execute { _ =>
          val rows = batchResult.get()
          assert(rows != null, "the batch query did not run")
          assert(rows.length == 5, s"expected 5 groups, got ${rows.length}")
          assert(rows.forall(_.getLong(1) == 20L), s"expected 20 rows per group, got $rows")
        },
        StopStream
      )
    }
  }

  test("two RTM queries run concurrently, each with its own pipelined group") {
    // Two independent pipelined groups must be admitted and co-scheduled at the same time. Keep the
    // partition counts low so both groups' gang demands fit the cluster together.
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val inputA = LowLatencyMemoryStream[(String, Int)]
      val inputB = LowLatencyMemoryStream[(String, Int)]

      val queryA = inputA.toDF().select($"_1".as("key")).dropDuplicates("key").select($"key")
      val queryB = inputB.toDF().select($"_1".as("key")).dropDuplicates("key").select($"key")

      // Track, from the driver, whether a stage of query A and a stage of query B were ever RUNNING
      // at the same time. Each query's long-running RTM stages are attributed to its query id (via
      // the job property StreamExecution tags), and `sawBothRunning` is set whenever both queries
      // have at least one stage running simultaneously -- which only happens if the two pipelined
      // groups are genuinely co-scheduled rather than one running after the other.
      val idA = new AtomicReference[String](null)
      val idB = new AtomicReference[String](null)
      val stagesA = ConcurrentHashMap.newKeySet[Int]()
      val stagesB = ConcurrentHashMap.newKeySet[Int]()
      val runningA = ConcurrentHashMap.newKeySet[Int]()
      val runningB = ConcurrentHashMap.newKeySet[Int]()
      val sawBothRunning = new AtomicBoolean(false)
      val listener = new SparkListener {
        override def onJobStart(e: SparkListenerJobStart): Unit = {
          val qid = Option(e.properties).map(_.getProperty(StreamExecution.QUERY_ID_KEY)).orNull
          if (qid != null && qid == idA.get()) e.stageIds.foreach(stagesA.add(_))
          else if (qid != null && qid == idB.get()) e.stageIds.foreach(stagesB.add(_))
        }
        override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit = {
          val sid = e.stageInfo.stageId
          if (stagesA.contains(sid)) runningA.add(sid)
          else if (stagesB.contains(sid)) runningB.add(sid)
          if (!runningA.isEmpty && !runningB.isEmpty) sawBothRunning.set(true)
        }
        override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
          val sid = e.stageInfo.stageId
          runningA.remove(sid)
          runningB.remove(sid)
        }
      }
      spark.sparkContext.addSparkListener(listener)

      // ForeachWriter is one of the sinks RTM allows (see RealTimeModeAllowlist.allowedSinks);
      // ForeachBatch is not, so it cannot be used to drive a second RTM query here.
      val handleB = queryB.writeStream
        .foreach(new ForeachWriter[Row] {
          override def open(partitionId: Long, epochId: Long): Boolean = true
          override def process(value: Row): Unit = ()
          override def close(errorOrNull: Throwable): Unit = ()
        })
        .queryName("coexistence_rtm_b")
        .outputMode(OutputMode.Update)
        .trigger(defaultTrigger)
        .start()

      try {
        idB.set(handleB.id.toString)
        eventually(timeout(60.seconds)) {
          assert(handleB.isActive, "second RTM query failed to start")
        }
        val execB = handleB.asInstanceOf[StreamingQueryWrapper].streamingQuery

        testStream(queryA, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          AddData(inputA, ("x", 1), ("y", 1), ("x", 2)),
          StartStream(),
          // StartStream only launches the query; its id is not known until it is running. Record it
          // here, then drive a SECOND batch below so the listener observes a batch of query A whose
          // jobs all start after idA is set (the first batch's jobs may fire before this and be
          // dropped from stagesA). This mirrors the mitigation in the sibling co-scheduling test.
          Execute(q => idA.set(q.id.toString)),
          CheckAnswerWithTimeout(60000, "x", "y"),
          AddData(inputA, ("z", 1), ("w", 1)),
          CheckAnswerWithTimeout(60000, "x", "y", "z", "w"),
          Execute { q =>
            assert(handleB.isActive, "the second RTM query must still be running")
            assert(handleB.exception.isEmpty,
              s"second RTM query failed: ${handleB.exception.map(_.getMessage).getOrElse("")}")
            // Query A's pipelined group is running now. Feed query B so it keeps scheduling its own
            // group (an idle RTM query can run empty batches), and wait until the listener has seen
            // a stage of A and a stage of B running at the same time.
            eventually(timeout(60.seconds)) {
              inputB.addData(("p", 1), ("q", 1))
              assert(sawBothRunning.get(),
                "expected a stage of query A and a stage of query B to run concurrently")
            }
            // Both groups are pipelined shuffles (not materialized).
            assertAllExchangesPipelined(q)
            assertAllExchangesPipelined(execB)
          },
          StopStream
        )
      } finally {
        spark.sparkContext.removeSparkListener(listener)
        handleB.stop()
      }
    }
  }
}
