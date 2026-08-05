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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * End-to-end SQL coverage of the local-repartition v2 path: a batch query whose hash
 * exchange is rewritten to a pipelined shuffle (EnablePipelinedShuffle) and served by the
 * in-process channel manager (PipelinedChannelShuffleManager), run through the
 * concurrent-stage scheduler on a single executor. Self-manages its SparkSession because the
 * shuffle manager and AQE-off gate are start-up configs.
 */
class PipelinedShuffleSqlSuite extends SparkFunSuite with AdaptiveSparkPlanHelper {

  private def withPipelinedSession(body: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      // High task-concurrency cap so the pipelined group's whole-group slot demand (the sum
      // of every concurrent stage's partitions) is admitted. This is a correctness harness,
      // not a perf one: on a smaller physical machine these logical slots oversubscribe the
      // cores, which is fine for verifying results but meaningless for timing.
      .master("local[16]")
      .appName("pipelined-shuffle-sql")
      .config("spark.shuffle.manager.incremental",
        "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager")
      .config("spark.sql.adaptive.enabled", "false")   // rule only sees exchanges with AQE off
      .config("spark.sql.pipelinedShuffle.enabled", "true")
      .config("spark.speculation", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    try {
      body(spark)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  test("batch repartition($k) runs end-to-end through the pipelined channel shuffle") {
    withPipelinedSession { spark =>
      import spark.implicits._
      val df = spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 10)).repartition($"k")

      // Single action only: a pipelined shuffle is single-shot, so collect exactly once and
      // derive everything from that one result.
      val rows = df.select($"id").as[Long].collect()
      val ids = rows.toSet

      // The rule fired and the exchange is pipelined.
      val pipelinedExchanges = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec if s.pipelined => s
      }
      assert(pipelinedExchanges.nonEmpty,
        s"expected a pipelined ShuffleExchangeExec; plan was:\n${df.queryExecution.executedPlan}")

      // Correctness: the same 1000 ids, repartitioned, all present exactly once.
      assert(rows.length === 1000, s"expected 1000 rows, got ${rows.length}")
      assert(ids === (0L until 1000L).toSet)
    }
  }

  test("a single keyed groupBy runs end-to-end through the pipelined channel shuffle") {
    withPipelinedSession { spark =>
      import spark.implicits._
      val df = spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 7))
        .groupBy($"k").count()

      // Single action only (pipelined shuffle is single-shot).
      val counts = df.as[(Long, Long)].collect().toMap
      val pipelined = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec if s.pipelined => s
      }
      assert(pipelined.nonEmpty,
        s"expected pipelined exchange; plan:\n${df.queryExecution.executedPlan}")
      // Each residue class 0..6 of 0..999.
      val expected = (0L until 1000L).groupBy(_ % 7).map { case (k, vs) => (k, vs.size.toLong) }
      assert(counts === expected)
    }
  }

  test("groupBy with ORDER BY (hash + single-partition exchanges) is all-pipelined") {
    // A trailing ORDER BY adds a SinglePartition exchange on top of the groupBy's hash
    // exchange. The relaxed rule pipelines BOTH (a mixed hash-pipelined / single-regular job
    // would be rejected); this confirms SinglePartition pipelines and the all-pipelined job
    // is admitted and correct.
    withPipelinedSession { spark =>
      import spark.implicits._
      val df = spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 7))
        .groupBy($"k").count().orderBy($"k")

      val rows = df.as[(Long, Long)].collect()
      val exchanges = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      assert(exchanges.nonEmpty && exchanges.forall(_.pipelined),
        s"every exchange should be pipelined; plan:\n${df.queryExecution.executedPlan}")
      // Result is correct AND globally ordered by k.
      val expected = (0L until 1000L).groupBy(_ % 7).map { case (k, vs) => (k, vs.size.toLong) }
        .toSeq.sortBy(_._1)
      assert(rows.toSeq === expected)
    }
  }

  test("sort-merge join (both sides hash-exchanged) is all-pipelined and correct") {
    // A shuffled join is the last TPC-DS transport shape not yet covered: both join inputs
    // get a hash ShuffleExchangeExec. Disable broadcast so the join is a SortMergeJoin with
    // two real shuffles; the relaxed rule pipelines both, and the concurrent-stage group
    // (two producers + the join stage) runs together.
    withPipelinedSession { spark =>
      import spark.implicits._
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      // Two structurally DIFFERENT inputs so exchange reuse does not collapse them into one
      // ReusedExchange (which the rule would skip). Different ranges + key expressions.
      val left = spark.range(0, 200, 1, 2).withColumn("k", ($"id" % 10))
        .select($"k", $"id".as("lv"))
      val right = spark.range(0, 120, 1, 2).withColumn("k", ($"id" % 6))
        .select($"k", $"id".as("rv"))
      val joined = left.join(right, "k")

      val rows = joined.select($"k", $"lv", $"rv").as[(Long, Long, Long)].collect()
      val exchanges = collect(joined.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      assert(exchanges.length >= 2 && exchanges.forall(_.pipelined),
        s"both join inputs should be pipelined; plan:\n${joined.queryExecution.executedPlan}")

      // Ground truth: an equi-join on k over the two relations.
      val l = (0L until 200L).map(i => (i % 10, i))
      val r = (0L until 120L).map(i => (i % 6, i))
      val expected = (for ((lk, lv) <- l; (rk, rv) <- r if lk == rk) yield (lk, lv, rv)).toSet
      assert(rows.toSet === expected)
    }
  }
}
