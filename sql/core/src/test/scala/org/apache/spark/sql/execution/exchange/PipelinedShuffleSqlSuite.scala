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
 * End-to-end SQL coverage of the pipelined channel path: a batch query whose hash
 * exchange is rewritten to a pipelined shuffle (EnablePipelinedShuffle) and served by the
 * in-process channel manager (PipelinedChannelShuffleManager), run through the
 * concurrent-stage scheduler on a single executor. Self-manages its SparkSession because the
 * shuffle manager and AQE-off gate are start-up configs.
 */
class PipelinedShuffleSqlSuite extends SparkFunSuite with AdaptiveSparkPlanHelper {

  private def withPipelinedSession(body: SparkSession => Unit): Unit = {
    // sql/core suites share a JVM. If an earlier suite left an active/default SparkSession behind,
    // getOrCreate() below would return THAT session and silently ignore every .config() here
    // (spark.shuffle.manager.incremental, spark.sql.shuffle.localPipelined.enabled), so no exchange
    // would be flipped and the assertions would fail pointing nowhere near the cause. Stop and
    // clear any pre-existing session first so this harness gets a fresh one with its own configs.
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
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
      .config("spark.sql.shuffle.localPipelined.enabled", "true")
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

  test("groupBy with ORDER BY (hash + range exchanges) is all-pipelined") {
    // A trailing ORDER BY adds a RANGE exchange (global sort with 4 shuffle partitions ->
    // RangePartitioning) on top of the groupBy's hash exchange. The relaxed rule pipelines
    // BOTH (a mixed pipelined/regular job would be rejected). Range is the interesting case:
    // RangePartitioner construction runs a SAMPLE job over the exchange's child -- which here
    // reads the pipelined hash shuffle -- before the main job runs, so this also exercises
    // two successive jobs over the same single-shot pipelined producer.
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
      // Pin the partitioning shapes so this test can't silently stop covering range.
      val partitionings = exchanges.map(_.outputPartitioning.getClass.getSimpleName).sorted
      assert(exchanges.exists(_.outputPartitioning.isInstanceOf[
          org.apache.spark.sql.catalyst.plans.physical.RangePartitioning]),
        s"expected a RangePartitioning exchange, got: $partitionings; " +
          s"plan:\n${df.queryExecution.executedPlan}")
      // Result is correct AND globally ordered by k.
      val expected = (0L until 1000L).groupBy(_ % 7).map { case (k, vs) => (k, vs.size.toLong) }
        .toSeq.sortBy(_._1)
      assert(rows.toSeq === expected)
    }
  }

  test("repartitionByRange (pure range exchange) runs through the pipelined channel shuffle") {
    // A range exchange directly over the scan: RangePartitioner samples the scan (a job with
    // no shuffle at all), then the main job runs the pipelined range shuffle. Verifies the
    // channel transport is agnostic to the partitioner kind, and rows land range-partitioned.
    withPipelinedSession { spark =>
      import spark.implicits._
      val df = spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 100))
        .repartitionByRange($"k")
        // spark_partition_id() records which output partition each row landed in without
        // leaving the DataFrame API (Dataset.rdd would execute a separate QueryExecution).
        .select($"k", org.apache.spark.sql.functions.spark_partition_id().as("p"))

      val partitioned = df.as[(Long, Int)].collect().map { case (k, p) => (p, k) }
      val exchanges = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      assert(exchanges.nonEmpty && exchanges.forall(_.pipelined),
        s"expected a pipelined exchange; plan:\n${df.queryExecution.executedPlan}")
      assert(exchanges.exists(_.outputPartitioning.isInstanceOf[
          org.apache.spark.sql.catalyst.plans.physical.RangePartitioning]),
        s"expected RangePartitioning; plan:\n${df.queryExecution.executedPlan}")

      // No rows lost, and the partitioning is a genuine range split: key ranges of distinct
      // partitions must not overlap.
      assert(partitioned.length === 1000)
      val ranges = partitioned.groupBy(_._1).map { case (p, rows) =>
        (p, rows.map(_._2).min, rows.map(_._2).max)
      }.toSeq.sortBy(_._2)
      ranges.sliding(2).foreach {
        case Seq((p1, _, max1), (p2, min2, _)) =>
          assert(max1 <= min2, s"partitions $p1 and $p2 overlap: max($p1)=$max1 > min($p2)=$min2")
        case _ =>
      }
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

  test("global aggregate (single-partition exchange) runs through the pipelined channel") {
    // An ungrouped aggregate requires AllTuples, planned as a SinglePartition exchange: the
    // channel's numPartitions == 1 degenerate case (everything routes to queue 0).
    withPipelinedSession { spark =>
      import spark.implicits._
      val df = spark.range(0, 1000, 1, 2).agg(org.apache.spark.sql.functions.sum($"id"))

      val result = df.as[Long].collect()
      val exchanges = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      assert(exchanges.nonEmpty && exchanges.forall(_.pipelined),
        s"expected a pipelined exchange; plan:\n${df.queryExecution.executedPlan}")
      assert(exchanges.exists(_.outputPartitioning ==
          org.apache.spark.sql.catalyst.plans.physical.SinglePartition),
        s"expected a SinglePartition exchange; plan:\n${df.queryExecution.executedPlan}")
      assert(result.toSeq === Seq((0L until 1000L).sum))
    }
  }

  test("Dataset.rdd (shuffle unregistered before the job runs) loses no rows") {
    // Dataset.rdd builds the RDD inside a SQL execution scope that ENDS before any job runs;
    // with spark.sql.classic.shuffleDependency.fileCleanup.enabled (the default under
    // testing), the scope's end removes the shuffle from every manager -- BEFORE the collect
    // below executes it. An early manager version kept the per-shuffle map-task count in a
    // registry keyed by shuffleId; the unregister wiped it, the reader treated the missing
    // entry as numMaps = 0, stopped at the first end-of-stream marker, and silently dropped
    // whatever one writer had not yet enqueued (a race, ~2/3 reproducible). numMaps is now
    // derived from the handle's own dependency, which cannot be unregistered away.
    withPipelinedSession { spark =>
      import spark.implicits._
      val df = spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 100))
        .repartitionByRange($"k")
      val rows = df.rdd.mapPartitionsWithIndex { (idx, iter) =>
        iter.map(row => (idx, row.getLong(1)))
      }.collect()
      assert(rows.length === 1000)
    }
  }

  test("an exchange wider than the task-concurrency limit fails loudly at admission") {
    // Deliberate design decision (viirya): the rule does NOT cap flipped exchanges at the
    // local concurrency limit. The user opted in explicitly, so an over-wide plan surfaces
    // the scheduler's CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT error -- actionable and
    // explicit -- rather than silently degrading to a regular run.
    withPipelinedSession { spark =>
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

  test("cross-subquery exchange reuse cannot create a shared pipelined exchange") {
    // The no-reuse gate checks subquery plans too (collectWithSubqueries). Probing every
    // SQL route to a reused PIPELINED exchange showed each is closed by a different layer,
    // and this test pins the observed facts so a change in any layer surfaces here:
    //   1. Same-tree reuse: the gate skips the plan (also covered by the join/q68 shapes).
    //   2. Main-vs-subquery reuse: never fires -- each subquery runs its own preparation
    //      pass (PlanSubqueries -> prepareExecutedPlan, which includes
    //      EnablePipelinedShuffle), so its exchanges are already pipelined=true when the
    //      outer ReuseExchangeAndSubquery compares canonical forms against the outer
    //      not-yet-pipelined exchange: no match.
    //   3. Subquery-vs-subquery duplication: collapsed into ONE subquery by
    //      MergeScalarSubqueries / subquery reuse before exchange reuse is considered.
    withPipelinedSession { spark =>
      import spark.implicits._
      spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 7)).createOrReplaceTempView("t")

      // Main plan and subquery share an identical inner groupBy (route 2).
      val df = spark.sql("""
        SELECT k, COUNT(*) AS c FROM t GROUP BY k
        HAVING COUNT(*) > (SELECT AVG(c2) FROM (SELECT COUNT(*) AS c2 FROM t GROUP BY k) s)
      """)
      // Two DIFFERENT subqueries share an identical inner groupBy (routes 2 + 3).
      val df2 = spark.sql("""
        SELECT k, COUNT(*) AS c FROM t GROUP BY k
        HAVING COUNT(*) > (SELECT AVG(c2) FROM (SELECT COUNT(*) AS c2 FROM t GROUP BY k) a)
           AND COUNT(*) <= (SELECT MAX(c3) FROM (SELECT COUNT(*) AS c3 FROM t GROUP BY k) b)
      """)

      Seq(df, df2).foreach { d =>
        val plan = d.queryExecution.executedPlan
        // No reused exchange materializes anywhere (main tree or subqueries)...
        assert(plan.collectWithSubqueries { case r: ReusedExchangeExec => r }.isEmpty,
          s"unexpected reused exchange; plan:\n$plan")
        // ... so the gate does not fire and the plan (and its independently-prepared
        // subqueries) pipeline.
        assert(collect(plan) { case s: ShuffleExchangeExec if s.pipelined => s }.nonEmpty,
          s"main plan should be pipelined; plan:\n$plan")
        assert(plan.collectWithSubqueries {
            case s: ShuffleExchangeExec if s.pipelined => s
          }.size > collect(plan) { case s: ShuffleExchangeExec => s }.size,
          s"subquery exchanges should be pipelined by their own preparation; plan:\n$plan")
      }

      // Both execute correctly: 1000 = 7*142 + 6, so keys 0..5 have 143 rows (> avg
      // 142.86) and key 6 has 142.
      assert(df.collect().length === 6)
      assert(df2.collect().length === 6)
    }
  }

  test("coalesce over a shuffle falls back to a regular (non-pipelined) shuffle") {
    // A CoalesceExec (user .coalesce(n), a narrow no-shuffle partition reduction) reading from a
    // shuffle makes ONE reduce task drain SEVERAL reduce partitions sequentially -- a core
    // CoalescedRDD over the ShuffledRowRDD. The channel transport cannot serve that: the map-side
    // writer interleaves all partitions on one thread and parks on a full bounded queue, so a
    // reader draining partition `start` to completion before touching `start + 1` deadlocks the
    // parked writer, with no timeout escape. `coalesce`'s API contract is a narrow dependency that
    // merges adjacent partitions, which we cannot honor by re-hashing to `n` partitions either. So
    // EnablePipelinedShuffle leaves the WHOLE plan regular when any shuffle is read by a coalesce
    // (leaving only that exchange regular would put a pipelined exchange below a regular boundary,
    // which the scheduler rejects). The query still runs correctly, just not pipelined. Guard with
    // a deadline so a regression (a coalesce that DID go pipelined and hung) surfaces as a failure,
    // not a hung suite.
    val pool = Executors.newSingleThreadExecutor()
    val fut = pool.submit(new Runnable {
      override def run(): Unit = withPipelinedSession { spark =>
        import spark.implicits._
        // Enough rows that, had this gone pipelined, the writer would fill a bounded queue and
        // park -- so a regression is a real deadlock, not a too-small case that happens to fit.
        val df = spark.range(0, 2000000L, 1, 4).withColumn("k", ($"id" % 10))
          .groupBy($"k").count().coalesce(2)
        val n = df.collect().length
        require(n == 10, s"expected 10 groups, got $n")

        // The fallback fired: NO exchange in the plan is pipelined.
        val pipelined = collect(df.queryExecution.executedPlan) {
          case s: ShuffleExchangeExec if s.pipelined => s
        }
        require(pipelined.isEmpty,
          s"coalesce over a shuffle must leave the plan regular; found a pipelined exchange in:" +
            s"\n${df.queryExecution.executedPlan}")
      }
    })
    try {
      fut.get(90, TimeUnit.SECONDS)
    } catch {
      case _: java.util.concurrent.TimeoutException =>
        fut.cancel(true)
        fail("coalesce over a pipelined shuffle hung: the shuffle was pipelined despite a " +
          "coalesce reading it, and the coalesced multi-partition read deadlocked the writer")
    } finally {
      pool.shutdownNow()
    }
  }

  /**
   * Run `body` on a fresh pipelined session under a deadline, failing (not hanging the suite) on
   * timeout. Used by the unsupported-consumer fallback tests, where a regression is a deadlock or
   * a hard-fail rather than a wrong answer.
   */
  private def withDeadline(seconds: Int, onTimeout: String)(
      body: SparkSession => Unit): Unit = {
    val pool = Executors.newSingleThreadExecutor()
    val fut = pool.submit(new Runnable {
      override def run(): Unit = withPipelinedSession(body)
    })
    try {
      fut.get(seconds.toLong, TimeUnit.SECONDS)
    } catch {
      case _: java.util.concurrent.TimeoutException =>
        fut.cancel(true)
        fail(onTimeout)
    } finally {
      pool.shutdownNow()
    }
  }

  private def assertNotPipelined(df: org.apache.spark.sql.DataFrame, why: String): Unit = {
    val pipelined = collect(df.queryExecution.executedPlan) {
      case s: ShuffleExchangeExec if s.pipelined => s
    }
    require(pipelined.isEmpty, s"$why; found a pipelined exchange in:" +
      s"\n${df.queryExecution.executedPlan}")
  }

  test("coalesce over a union of shuffles falls back to regular (binary-child guard)") {
    // A CoalesceExec above a UnionExec (a BinaryExecNode) whose branches contain shuffles: the
    // guard walk must descend through the union's children, not only unary ones. With the old
    // unary-only walk both branch exchanges flip, and CoalescedRDD's task then drains SEVERAL
    // reduce partitions of the SAME pipelined shuffle in order while the single-threaded writer
    // parks on a later partition's full queue -- a hang with no timeout escape.
    //
    // Two details are load-bearing for this to reproduce the hazard at all (an earlier version of
    // this test had neither and passed even with the buggy guard):
    //   - the branches must be STRUCTURALLY DIFFERENT shuffles (groupBy vs repartition). Two
    //     identical branches canonicalize alike, exchange reuse collapses them, and the rule's
    //     reuse gate then bails out first -- leaving the plan regular for the wrong reason.
    //   - each reduce partition needs more rows than a queue holds (queueCapacity 64 batches x
    //     batchSize 1024 ~= 65K rows), or the writer never parks and nothing deadlocks.
    withDeadline(90,
      "coalesce over a union of pipelined shuffles hung: the guard missed the union's shuffle " +
        "children and the coalesced read deadlocked the writer") { spark =>
      import spark.implicits._
      val a = spark.range(0, 2000000L, 1, 4).withColumn("k", ($"id" % 1000))
        .groupBy($"k").count().select($"k")
      val b = spark.range(0, 2000000L, 1, 4).withColumn("k2", ($"id" % 1000))
        .repartition($"k2").select($"k2".as("k"))
      val df = a.union(b).coalesce(2)
      val n = df.count()
      require(n == 1000L + 2000000L, s"expected ${1000L + 2000000L} rows, got $n")
      assertNotPipelined(df, "coalesce over a union must leave the plan regular")
    }
  }

  test("crossJoin over a shuffle falls back to regular (N-to-1 cartesian read)") {
    // A CartesianProductExec reads its child once PER right partition, so N reduce tasks would mint
    // N readers on one rendezvous queue -- rows/markers split and the writer is abandoned mid-run.
    // The guard must leave a shuffle read by a cartesian product regular.
    withDeadline(90,
      "crossJoin over a pipelined shuffle hung or corrupted: the shuffle was pipelined and the " +
        "N-to-1 cartesian read split it across concurrent readers") { spark =>
      import spark.implicits._
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      val left = spark.range(0, 100000L, 1, 4).withColumn("k", ($"id" % 10)).repartition($"k")
      val right = spark.range(0, 20L, 1, 2)
      val df = left.crossJoin(right)
      val n = df.count()
      require(n == 100000L * 20L, s"expected ${100000L * 20L} rows, got $n")
      assertNotPipelined(df, "a shuffle read by a cartesian product must stay regular")
    }
  }

  test("a limit operator that builds a hidden shuffle in doExecute stays regular") {
    // CollectLimitExec/CollectTailExec/TakeOrderedAndProjectExec build a regular (pipelined=false)
    // shuffle inside doExecute, invisible to the plan walk. A flipped exchange below one of them
    // would sit under an unmaterialized regular boundary and the job would hard-fail at submission.
    // .collect() on a limit takes executeTake and dodges doExecute; a non-root position (feeding a
    // write) forces doExecute. Marking these operators blocking keeps the exchange below regular.
    withDeadline(90,
      "a limit operator's hidden shuffle made the job hard-fail: the exchange below it was " +
        "pipelined and landed under an unmaterialized regular boundary") { spark =>
      import spark.implicits._
      withTempDir { dir =>
        val out = new java.io.File(dir, "limit-out").getAbsolutePath
        val df = spark.range(0, 1000000L, 1, 4).withColumn("k", ($"id" % 100))
          .groupBy($"k").count().orderBy($"count").limit(5)
        // .write forces TakeOrderedAndProjectExec.doExecute (its hidden SinglePartition shuffle),
        // rather than the executeTake path .collect() would take.
        df.write.parquet(out)
        val readBack = spark.read.parquet(out).count()
        require(readBack == 5L, s"expected 5 rows written, got $readBack")
        assertNotPipelined(df, "an exchange below a hidden-shuffle limit op must stay regular")
      }
    }
  }
}
