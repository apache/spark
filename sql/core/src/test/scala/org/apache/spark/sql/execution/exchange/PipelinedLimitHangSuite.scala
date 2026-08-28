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

package org.apache.spark.sql.execution.exchange

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * SQL-level acceptance tests for LIMIT over a plan that CONTAINS a pipelined-eligible shuffle
 * (SPARK-57399).
 *
 * IMPORTANT -- what these tests do and do NOT cover. A LIMIT operator
 * (`CollectLimitExec` / `CollectTailExec` / `TakeOrderedAndProjectExec`) builds a hidden REGULAR
 * shuffle inside its `doExecute` (`prepareShuffleDependency` with `pipelined = false`) that no plan
 * walk can see. A pipelined exchange beneath it would sit under an unmaterialized regular boundary
 * and the job would hard-fail at submission, so `EnablePipelinedShuffle` /
 * `AQEEnablePipelinedShuffle` refuse to pipeline a plan containing such an operator above a
 * shuffle. Consequently these LIMIT shapes now run as REGULAR shuffles, and these tests assert
 * exactly that (fallback + correct results) -- they can no longer exercise the writer/reader
 * early-stop machinery.
 *
 * The early-stop and live-reduce-set machinery (`ChannelShuffleWriter.putUnlessAbandoned` /
 * `ChannelShuffleRendezvous.abandon` / `DAGScheduler.liveReduceSet`) is covered instead by the
 * RDD-level partial-read tests in `PipelinedChannelShuffleSuite` ("partial read ... completes"),
 * which drive a real partial read over a pipelined shuffle without a LIMIT operator in the way.
 *
 * Each test builds and stops its OWN SparkSession (withSession), so they do not share process
 * state and one test's timeout/cancel cannot affect the other.
 */
class PipelinedLimitHangSuite extends SparkFunSuite with AdaptiveSparkPlanHelper {

  private def withSession(aqe: Boolean)(body: SparkSession => Unit): Unit = {
    // Stop and clear any session an earlier sql/core suite left in this JVM, or getOrCreate()
    // would return it and silently ignore this harness's .config() (see PipelinedShuffleSqlSuite).
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val spark = SparkSession.builder()
      .master("local[16]")
      .appName("pipelined-limit-hang")
      .config("spark.shuffle.manager.incremental",
        "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager")
      .config("spark.sql.adaptive.enabled", aqe.toString)
      .config("spark.sql.pipelinedShuffle.enabled", "true")
      .config("spark.speculation", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    try body(spark) finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  /**
   * Runs `query(spark)` on a background thread under a deadline, returning true iff it both
   * COMPLETES (no writer hang) and produces 10 rows. Each query is a partial read (LIMIT) over one
   * Each shape asserts BOTH that the plan fell back to a regular shuffle (no exchange is
   * pipelined -- a LIMIT operator's hidden shuffle makes pipelining unsafe, see the class doc) and
   * that the query returns the right rows. The deadline stays so that a regression which DID
   * pipeline such a plan surfaces as a timeout (the hidden-shuffle hard-fail or a writer hang)
   * rather than a hung suite.
   */
  private def completesWithin(seconds: Int, aqe: Boolean)(
      query: SparkSession => Int): Boolean = {
    val pool = Executors.newSingleThreadExecutor()
    val fut = pool.submit(new Runnable {
      override def run(): Unit = withSession(aqe) { spark =>
        val rows = query(spark)
        require(rows == 10, s"expected 10 rows, got $rows")
      }
    })
    try {
      fut.get(seconds.toLong, TimeUnit.SECONDS)
      true
    } catch {
      case _: java.util.concurrent.TimeoutException =>
        fut.cancel(true)
        false
    } finally {
      pool.shutdownNow()
    }
  }

  /** Assert no exchange in `df`'s executed plan is pipelined (the LIMIT fallback). */
  private def requireNotPipelined(df: org.apache.spark.sql.DataFrame): Unit = {
    val pipelined = collect(df.queryExecution.executedPlan) {
      case s: ShuffleExchangeExec if s.pipelined => s
    }
    require(pipelined.isEmpty,
      s"a plan with a LIMIT operator above a shuffle must stay regular; plan:" +
        s"\n${df.queryExecution.executedPlan}")
  }

  // Many input rows per partition so a regression that pipelined this would really park a writer.
  private def repartitionLimit(spark: SparkSession): Int = {
    import spark.implicits._
    val df = spark.range(0, 5000000L, 1, 4).withColumn("k", ($"id" % 100))
      .repartition($"k").limit(10)
    val n = df.collect().length
    requireNotPipelined(df)
    n
  }

  // union() over two pipelined shuffles: the result RDD reaches each shuffle through a
  // RangeDependency whose getParents applies a per-branch offset, and it fans in over two
  // branches -- liveReduceSet must map the live set through both branches' offsets.
  private def unionLimit(spark: SparkSession): Int = {
    import spark.implicits._
    val a = spark.range(0, 2500000L, 1, 4).withColumn("k", ($"id" % 100)).repartition($"k")
    val b = spark.range(0, 2500000L, 1, 4).withColumn("k", ($"id" % 100)).repartition($"k")
    val df = a.union(b).limit(10)
    val n = df.collect().length
    requireNotPipelined(df)
    n
  }

  // join() over two pipelined shuffles: the result RDD fans in over a ZippedPartitionsRDD, one
  // OneToOne branch per side down to that side's shuffle -- liveReduceSet must contribute the live
  // set from every branch that reaches a given shuffle.
  private def joinLimit(spark: SparkSession): Int = {
    import spark.implicits._
    val a = spark.range(0, 2500000L, 1, 4).withColumn("k", ($"id" % 100))
    val b = spark.range(0, 2500000L, 1, 4).withColumn("k2", ($"id" % 100))
    val df = a.join(b, a("k") === b("k2")).limit(10)
    val n = df.collect().length
    requireNotPipelined(df)
    n
  }

  for (aqe <- Seq(false, true)) {
    val mode = if (aqe) "AQE on" else "AQE off"

    test(s"LIMIT over a shuffle falls back to regular and is correct ($mode)") {
      assert(completesWithin(90, aqe)(repartitionLimit),
        "LIMIT over a shuffle should fall back to regular and complete correctly")
    }

    test(s"union + LIMIT falls back to regular and is correct ($mode)") {
      assert(completesWithin(90, aqe)(unionLimit),
        "union + LIMIT should fall back to regular and complete correctly")
    }

    test(s"join + LIMIT falls back to regular and is correct ($mode)") {
      assert(completesWithin(90, aqe)(joinLimit),
        "join + LIMIT should fall back to regular and complete correctly")
    }
  }
}
