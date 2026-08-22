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
 * Acceptance test for the LIMIT early-stop fix (SPARK-57399). An
 * early-stopping reader over a pipelined channel shuffle used to hang the writer: the channel
 * queue is bounded (64 batches), so once a LIMIT's reduce task was satisfied and stopped
 * draining while the map task was still producing, the writer blocked forever on a full
 * queue's put() with no drainer. That is now fixed -- the reader marks its partitions abandoned
 * on task completion, and the writer stops feeding an abandoned partition (see
 * ChannelShuffleWriter.putUnlessAbandoned / ChannelShuffleRendezvous.abandon). regular shuffle
 * never had this: it materializes to disk first, so the writer finishes regardless of whether
 * the reader drains everything.
 *
 * Each test runs a partial read (LIMIT(10)) over one or more pipelined shuffles on a background
 * thread under a 90s deadline and asserts it both COMPLETES (no hang) and returns the correct row
 * count (rows == 10) in BOTH AQE modes. The shapes cover the narrow chains
 * DAGScheduler.liveReduceSet must map to the producer's live reduce-partition set: a plain
 * repartition (identity), union (two branches with per-branch offsets), and join (fan-in over a
 * ZippedPartitionsRDD, one narrow branch per side). A hang trips the TimeoutException path -> the
 * helper returns false -> the test fails; a wrong count throws out of the future -> the test
 * fails. Each test builds and stops its OWN SparkSession (withSession), so they do not share
 * process state and one test's timeout/cancel cannot affect the other.
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
   * or more pipelined shuffles through a narrow chain the producer must map to its live
   * reduce-partition set (see DAGScheduler.liveReduceSet); a wrong mapping either hangs the writer
   * (timeout -> false) or drops rows (the require fails -> the future throws -> false).
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

  // Many input rows per partition so the writer keeps producing well past the LIMIT.
  private def repartitionLimit(spark: SparkSession): Int = {
    import spark.implicits._
    spark.range(0, 5000000L, 1, 4).withColumn("k", ($"id" % 100))
      .repartition($"k").limit(10).collect().length
  }

  // union() over two pipelined shuffles: the result RDD reaches each shuffle through a
  // RangeDependency whose getParents applies a per-branch offset, and it fans in over two
  // branches -- liveReduceSet must map the live set through both branches' offsets.
  private def unionLimit(spark: SparkSession): Int = {
    import spark.implicits._
    val a = spark.range(0, 2500000L, 1, 4).withColumn("k", ($"id" % 100)).repartition($"k")
    val b = spark.range(0, 2500000L, 1, 4).withColumn("k", ($"id" % 100)).repartition($"k")
    a.union(b).limit(10).collect().length
  }

  // join() over two pipelined shuffles: the result RDD fans in over a ZippedPartitionsRDD, one
  // OneToOne branch per side down to that side's shuffle -- liveReduceSet must contribute the live
  // set from every branch that reaches a given shuffle.
  private def joinLimit(spark: SparkSession): Int = {
    import spark.implicits._
    val a = spark.range(0, 2500000L, 1, 4).withColumn("k", ($"id" % 100))
    val b = spark.range(0, 2500000L, 1, 4).withColumn("k2", ($"id" % 100))
    a.join(b, a("k") === b("k2")).limit(10).collect().length
  }

  for (aqe <- Seq(false, true)) {
    val mode = if (aqe) "AQE on" else "AQE off"

    test(s"LIMIT over a pipelined shuffle completes ($mode)") {
      assert(completesWithin(90, aqe)(repartitionLimit),
        "LIMIT over a pipelined shuffle should complete, but the writer hung on a full queue")
    }

    test(s"union + LIMIT over pipelined shuffles completes ($mode)") {
      assert(completesWithin(90, aqe)(unionLimit),
        "union + LIMIT over pipelined shuffles should complete, but the writer hung")
    }

    test(s"join + LIMIT over pipelined shuffles completes ($mode)") {
      assert(completesWithin(90, aqe)(joinLimit),
        "join + LIMIT over pipelined shuffles should complete, but the writer hung")
    }
  }
}
