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

package org.apache.spark.sql.execution.adaptive

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.exchange.{PipelinedShuffleTestSession, ShuffleExchangeExec}

/**
 * End-to-end coverage of the pipelined channel path UNDER AQE: AQEEnablePipelinedShuffle
 * flips the final unmaterialized tail's exchanges to pipelined, exchanges below the tail
 * materialize as regular query stages (keeping full AQE treatment), and the final result job
 * runs the pipelined gang over the materialized prefix -- the scheduler shape admitted by
 * the materialized-prefix relaxation. Self-manages its SparkSession (shuffle manager is a
 * start-up config).
 */
class AQEPipelinedShuffleSuite extends SparkFunSuite
  with AdaptiveSparkPlanHelper with PipelinedShuffleTestSession {

  private def withAqePipelinedSession(body: SparkSession => Unit): Unit =
    withPipelinedSession("aqe-pipelined-shuffle", aqe = true)(body)

  private def pipelinedExchanges(plan: org.apache.spark.sql.execution.SparkPlan) =
    collect(plan) { case s: ShuffleExchangeExec if s.pipelined => s }

  private def materializedStages(plan: org.apache.spark.sql.execution.SparkPlan) =
    collect(plan) { case q: ShuffleQueryStageExec => q }

  test("single-exchange aggregate pipelines the whole final job under AQE") {
    withAqePipelinedSession { spark =>
      import spark.implicits._
      // Keep ONE Dataset for both execution and plan inspection: .as[...] creates a new
      // QueryExecution, and asserting on an unexecuted sibling would see the initial
      // adaptive plan (isFinalPlan=false), not the executed one.
      val ds = spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 7))
        .groupBy($"k").count().as[(Long, Long)]

      val counts = ds.collect().toMap
      val plan = ds.queryExecution.executedPlan
      assert(pipelinedExchanges(plan).nonEmpty,
        s"expected the sole exchange pipelined; plan:\n$plan")
      assert(materializedStages(plan).isEmpty,
        s"no exchange should have materialized as a stage; plan:\n$plan")
      val expected = (0L until 1000L).groupBy(_ % 7).map { case (k, vs) => (k, vs.size.toLong) }
      assert(counts === expected)
    }
  }

  test("groupBy + ORDER BY: materialized prefix + pipelined tail under AQE") {
    // The canonical AQE shape this feature targets: the groupBy's hash exchange sits BELOW
    // the sort's range exchange, so it stays regular and materializes as a query stage
    // (keeping AQE coalescing); the range exchange on top is free and flips. The final job
    // is the pipelined tail over the materialized prefix.
    withAqePipelinedSession { spark =>
      import spark.implicits._
      val ds = spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 7))
        .groupBy($"k").count().orderBy($"k").as[(Long, Long)]

      val rows = ds.collect()
      val plan = ds.queryExecution.executedPlan
      assert(pipelinedExchanges(plan).size === 1,
        s"expected exactly the top (range) exchange pipelined; plan:\n$plan")
      assert(materializedStages(plan).nonEmpty,
        s"expected the groupBy exchange materialized as a regular stage; plan:\n$plan")
      val expected = (0L until 1000L).groupBy(_ % 7).map { case (k, vs) => (k, vs.size.toLong) }
        .toSeq.sortBy(_._1)
      assert(rows.toSeq === expected)
    }
  }

  test("shuffled join inputs flip as a symmetric pair under AQE") {
    withAqePipelinedSession { spark =>
      import spark.implicits._
      // Keep the join a shuffled join: disable both static and adaptive broadcast switching.
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      val left = spark.range(0, 200, 1, 2).withColumn("k", ($"id" % 10))
        .select($"k", $"id".as("lv"))
      val right = spark.range(0, 120, 1, 2).withColumn("k", ($"id" % 6))
        .select($"k", $"id".as("rv"))
      val joined = left.join(right, "k")
        .select($"k", $"lv", $"rv").as[(Long, Long, Long)]

      val rows = joined.collect()
      val plan = joined.queryExecution.executedPlan
      assert(pipelinedExchanges(plan).size === 2,
        s"expected both join inputs pipelined as a pair; plan:\n$plan")

      val l = (0L until 200L).map(i => (i % 10, i))
      val r = (0L until 120L).map(i => (i % 6, i))
      val expected = (for ((lk, lv) <- l; (rk, rv) <- r if lk == rk) yield (lk, lv, rv)).toSet
      assert(rows.toSet === expected)
    }
  }

  test("an exchange wider than the task-concurrency limit fails loudly under AQE too") {
    // Same design decision as the non-AQE suite (viirya): no silent degrade; the explicit
    // slot-admission error reaches the user.
    withAqePipelinedSession { spark =>
      import spark.implicits._
      spark.conf.set("spark.sql.shuffle.partitions", "64")
      try {
        val ex = intercept[Exception] {
          spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 7))
            .groupBy($"k").count().collect()
        }
        val messages = Iterator.iterate(ex: Throwable)(_.getCause).takeWhile(_ != null)
          .map(t => Option(t.getMessage).getOrElse("")).mkString(" | ")
        assert(messages.contains("CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT") ||
          messages.contains("concurrent task slots"),
          s"expected the explicit slot-admission error, got: $messages")
      } finally {
        spark.conf.set("spark.sql.shuffle.partitions", "4")
      }
    }
  }

  test("results match the regular AQE baseline on the prefix + tail shape") {
    withAqePipelinedSession { spark =>
      import spark.implicits._
      def run(): Seq[(Long, Long)] = {
        spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 13))
          .groupBy($"k").count().orderBy($"k".desc)
          .as[(Long, Long)].collect().toSeq
      }
      spark.conf.set("spark.sql.shuffle.localPipelined.enabled", "false")
      val baseline = run()
      spark.conf.set("spark.sql.shuffle.localPipelined.enabled", "true")
      val pipelined = run()
      assert(pipelined === baseline)
    }
  }

  test("coalesce over a shuffle stays regular under AQE") {
    // A CoalesceExec reading from a shuffle drains several reduce partitions per task (a
    // CoalescedRDD over the ShuffledRowRDD), which the channel transport cannot serve without
    // deadlocking (see EnablePipelinedShuffle / ChannelShuffleReader). AQEEnablePipelinedShuffle
    // blocks below a CoalesceExec (it is stats-sensitive for this purpose), so the shuffle it
    // reads -- and everything deeper -- stays regular. The query still runs correctly. Guard with
    // a deadline so a regression (the shuffle went pipelined and the coalesced read hung) surfaces
    // as a failure rather than a hung suite.
    val pool = Executors.newSingleThreadExecutor()
    val fut = pool.submit(new Runnable {
      override def run(): Unit = withAqePipelinedSession { spark =>
        import spark.implicits._
        // Enough rows that a regression would fill a bounded queue and truly deadlock, not fit.
        val ds = spark.range(0, 2000000L, 1, 4).withColumn("k", ($"id" % 10))
          .groupBy($"k").count().coalesce(2).as[(Long, Long)]
        val n = ds.collect().length
        require(n == 10, s"expected 10 groups, got $n")
        val plan = ds.queryExecution.executedPlan
        require(pipelinedExchanges(plan).isEmpty,
          s"coalesce over a shuffle must stay regular under AQE; found a pipelined exchange:" +
            s"\n$plan")
      }
    })
    try {
      fut.get(90, TimeUnit.SECONDS)
    } catch {
      case _: java.util.concurrent.TimeoutException =>
        fut.cancel(true)
        fail("coalesce over a pipelined shuffle hung under AQE: the shuffle was pipelined " +
          "despite a coalesce reading it, and the coalesced read deadlocked the writer")
    } finally {
      pool.shutdownNow()
    }
  }
}
