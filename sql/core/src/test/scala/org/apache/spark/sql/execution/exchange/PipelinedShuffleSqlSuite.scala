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

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import org.apache.spark.{PipelinedShuffleDependency, SparkEnv, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.local.pipelined.ChannelShuffleRendezvous
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

/**
 * End-to-end SQL coverage of the pipelined channel path: a batch query whose hash
 * exchange is rewritten to a pipelined shuffle (EnablePipelinedShuffle) and served by the
 * in-process channel manager (PipelinedChannelShuffleManager), run through the
 * concurrent-stage scheduler on a single executor. Self-manages its SparkSession because the
 * shuffle manager and AQE-off gate are start-up configs.
 */
class PipelinedShuffleSqlSuite extends SparkFunSuite
  with AdaptiveSparkPlanHelper with PipelinedShuffleTestSession {

  private def withPipelinedSession(body: SparkSession => Unit): Unit =
    withPipelinedSession("pipelined-shuffle-sql", aqe = false)(body)

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

  private def assertRegularRDD(root: RDD[_]): Unit = {
    val visited = scala.collection.mutable.Set.empty[Int]
    val pending = scala.collection.mutable.Stack[RDD[_]](root)
    while (pending.nonEmpty) {
      val rdd = pending.pop()
      if (visited.add(rdd.id)) {
        rdd.dependencies.foreach { dep =>
          assert(!dep.isInstanceOf[PipelinedShuffleDependency[_, _, _]])
          pending.push(dep.rdd)
        }
      }
    }
  }

  for (aqe <- Seq(false, true)) {
    test(s"typed and Python RDD exports retain regular shuffle lineage with AQE=$aqe") {
      withPipelinedSession("pipelined-rdd-exports", aqe) { spark =>
        import spark.implicits._
        val df = spark.range(0, 4000, 1, 2).repartition(4)
        assert(df.collect().length === 4000)
        val typed = df.groupByKey(_ % 4).mapGroups { (key, rows) =>
          (key, rows.size.toLong)
        }
        val rdd = typed.rdd
        assertRegularRDD(rdd)
        assert(rdd.coalesce(1).collect().map(_._2).sum === 4000L)
        val pythonRDD = df.asInstanceOf[org.apache.spark.sql.classic.Dataset[Long]]
          .javaToPython.rdd
        assertRegularRDD(pythonRDD)
        assert(pythonRDD.coalesce(1).count() > 0)
        val fromRDD = spark.createDataFrame(rdd).repartition(4).rdd
        assertRegularRDD(fromRDD)
        assert(fromRDD.coalesce(1).count() === 4L)
      }
    }

    test(s"lazy checkpoints retain regular shuffle lineage with AQE=$aqe") {
      withPipelinedSession("pipelined-checkpoint", aqe) { spark =>
        withTempDir { dir =>
          spark.sparkContext.setCheckpointDir(dir.getCanonicalPath)
          for (reliable <- Seq(false, true)) {
            val df = spark.range(0, 4000, 1, 2).repartition(4)
            assert(df.collect().length === 4000)
            val checkpointed = if (reliable) {
              df.checkpoint(eager = false)
            } else {
              df.localCheckpoint(eager = false)
            }
            val rdd = checkpointed.rdd
            assertRegularRDD(rdd)
            assert(rdd.coalesce(1).count() === 4000L)
            assert(checkpointed.collect().sorted === (0L until 4000L).toArray)
          }
        }
      }
    }

    test(s"SQL cursor computes its producer once with AQE=$aqe") {
      withPipelinedSession("pipelined-cursor", aqe) { spark =>
        spark.conf.set("spark.sql.scripting.enabled", "true")
        spark.conf.set("spark.sql.scripting.cursorEnabled", "true")
        spark.conf.set("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "false")
        val evaluated = spark.sparkContext.longAccumulator("cursor producer rows")
        spark.udf.register("record_cursor_row", (id: Long) => {
          evaluated.add(1)
          id
        })
        val result = spark.sql(
          """BEGIN
            |  DECLARE v BIGINT;
            |  DECLARE total BIGINT DEFAULT 0;
            |  DECLARE i INT DEFAULT 0;
            |  DECLARE c CURSOR FOR
            |    SELECT /*+ REPARTITION(4) */ record_cursor_row(id) FROM range(100);
            |  OPEN c;
            |  WHILE i < 100 DO
            |    FETCH c INTO v;
            |    SET total = total + v;
            |    SET i = i + 1;
            |  END WHILE;
            |  CLOSE c;
            |  VALUES (total);
            |END""".stripMargin)
        assert(result.collect().head.getLong(0) === (0L until 100L).sum)
        assert(evaluated.value === 100L)
      }
    }

    test(s"Dataset.rdd supports narrow and shuffle consumers with AQE=$aqe") {
      withPipelinedSession("pipelined-rdd-boundary", aqe) { spark =>
        val rdd = spark.range(0, 4000, 1, 2).repartition(4).toDF().rdd
        // Assert eligibility before executing a shape that would hang with the channel.
        assertRegularRDD(rdd)
        assert(rdd.coalesce(2).count() === 4000L)
        assert(rdd.union(rdd).count() === 8000L)
        assert(rdd.zip(rdd).count() === 4000L)
        val grouped = rdd.map(r => (r.getLong(0) % 2, 1L)).reduceByKey(_ + _)
        assert(grouped.collect().toMap === Map(0L -> 2000L, 1L -> 2000L))
        assert(rdd.repartition(2).count() === 4000L)
      }
    }

    test(s"cache construction and partial eviction use regular shuffles with AQE=$aqe") {
      withPipelinedSession("pipelined-cache", aqe) { spark =>
        val df = spark.range(0, 4000, 1, 2).repartition(4).persist(StorageLevel.MEMORY_ONLY)
        try {
          val classicDf = df.asInstanceOf[org.apache.spark.sql.classic.Dataset[_]]
          val cached = spark.sharedState.cacheManager.lookupCachedData(classicDf).get
            .cachedRepresentation.cacheBuilder
          assert(collect(cached.cachedPlan) {
            case s: ShuffleExchangeExec if s.pipelined => s
          }.isEmpty)
          val expected = (0L until 4000L).toArray
          assert(df.collect().sorted === expected)
          val buffers = cached.cachedColumnBuffers
          assert(buffers.getNumPartitions > 1)
          SparkEnv.get.blockManager.removeBlock(RDDBlockId(buffers.id, 0))
          assert(df.collect().sorted === expected)
          assert(df.collect().sorted === expected)
          val consumer = df.repartition(2)
          assert(consumer.collect().sorted === expected)
          assert(collect(consumer.queryExecution.executedPlan) {
            case s: ShuffleExchangeExec if s.pipelined => s
          }.isEmpty)
        } finally {
          df.unpersist(blocking = true)
        }
      }
    }

    test(s"toLocalIterator uses regular output across jobs with AQE=$aqe") {
      withPipelinedSession("pipelined-local-iterator", aqe) { spark =>
        import scala.jdk.CollectionConverters._
        import spark.implicits._
        spark.conf.set("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "false")
        val expected = (0L until 1000L).toArray
        for (collectFirst <- Seq(false, true)) {
          val evaluated = spark.sparkContext.longAccumulator("producer rows")
          val recordEvaluation = org.apache.spark.sql.functions.udf { id: Long =>
            evaluated.add(1)
            id
          }
          val df = spark.range(0, 1000, 1, 2).select(recordEvaluation($"id").as("id"))
            .repartition(4).as[Long]
          val initialRuns = if (collectFirst) {
            assert(df.collect().sorted === expected)
            1
          } else {
            0
          }
          for (run <- 1 to 3) {
            val iterator = df.toLocalIterator()
            assert(spark.conf.get("spark.sql.shuffle.localPipelined.enabled") === "true")
            assert(iterator.asScala.toArray.sorted === expected)
            // Four output-partition jobs reuse regular shuffle output: the producer runs once.
            assert(evaluated.value === (initialRuns + run) * 1000L)
          }
          assert(df.collect().sorted === expected)
          assert(evaluated.value === (initialRuns + 4) * 1000L)
          val exchanges = collect(df.queryExecution.executedPlan) {
            case s: ShuffleExchangeExec if s.pipelined => s
          }
          assert(exchanges.nonEmpty, "collect must still use the original pipelined plan")
          exchanges.foreach { s =>
            assert(!ChannelShuffleRendezvous.holdsShuffle(s.shuffleDependency.shuffleId))
          }
        }
      }
    }

    test(s"capacity fallback preserves explicit repartition width with AQE=$aqe") {
      withPipelinedSession("pipelined-explicit-width", aqe, cores = 8) { spark =>
        val df = spark.range(0, 1000, 1, 2).repartition(200)
        assert(df.collect().sorted === (0L until 1000L).toArray)
        val exchanges = collect(df.queryExecution.executedPlan) { case s: ShuffleExchangeExec => s }
        assert(exchanges.nonEmpty)
        assert(exchanges.forall(!_.pipelined))
        assert(exchanges.exists(_.outputPartitioning.numPartitions == 200))
      }
    }

    test(s"capacity accounts for all stages of the group with AQE=$aqe") {
      withPipelinedSession("pipelined-group-width", aqe, cores = 8) { spark =>
        import spark.implicits._
        spark.conf.set("spark.sql.shuffle.partitions", "6")
        val grouped = spark.range(0, 1000, 1, 2).groupBy(($"id" % 7).as("k")).count()
        assert(grouped.collect().map(_.getLong(1)).sum === 1000L)
        assert(collect(grouped.queryExecution.executedPlan) {
          case s: ShuffleExchangeExec if s.pipelined => s
        }.nonEmpty, "two producer tasks plus six consumer tasks fit exactly")

        val total = grouped.agg(org.apache.spark.sql.functions.max("count"))
        assert(total.collect().head.getLong(0) === 143L)
        val exchanges = collect(total.queryExecution.executedPlan) {
          case s: ShuffleExchangeExec => s
        }
        assert(exchanges.size >= 2)
        assert(exchanges.forall(!_.pipelined), "the full group needs 2 + 6 + 1 slots")
      }
    }
  }

  test("concurrent non-AQE actions on one pipelined Dataset fail without affecting its owner") {
    withPipelinedSession { spark =>
      import spark.implicits._
      spark.conf.set("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "true")
      PipelinedShuffleSqlSuite.started = new CountDownLatch(1)
      PipelinedShuffleSqlSuite.release = new CountDownLatch(1)
      val waitForRelease = org.apache.spark.sql.functions.udf(
        PipelinedShuffleSqlSuite.waitForRelease _)
      val df = spark.range(0, 1000, 1, 2).select(waitForRelease($"id").as("id"))
        .repartition(4).as[Long]
      val pool = Executors.newSingleThreadExecutor()
      val first = pool.submit(new Callable[Array[Long]] {
        override def call(): Array[Long] = df.collect()
      })
      try {
        assert(PipelinedShuffleSqlSuite.started.await(30, TimeUnit.SECONDS))
        val error = intercept[Exception] { df.collect() }
        val messages = Iterator.iterate(error: Throwable)(_.getCause).takeWhile(_ != null)
          .map(_.getMessage).mkString(" ")
        assert(messages.contains("PIPELINED_SHUFFLE_CROSS_JOB_REUSE"))
        PipelinedShuffleSqlSuite.release.countDown()
        assert(first.get(30, TimeUnit.SECONDS).sorted === (0L until 1000L).toArray)
        assert(df.collect().sorted === (0L until 1000L).toArray)
      } finally {
        PipelinedShuffleSqlSuite.release.countDown()
        spark.sparkContext.cancelAllJobs()
        pool.shutdownNow()
      }
    }
  }

  test("default shuffle width falls back to regular execution in local mode") {
    withPipelinedSession("pipelined-default-width", aqe = false, cores = 8) { spark =>
      import spark.implicits._
      spark.conf.unset("spark.sql.shuffle.partitions")
      assert(spark.conf.get("spark.sql.shuffle.partitions") === "200")
      val df = spark.range(0, 1000, 1, 2).groupBy(($"id" % 7).as("k")).count()
      assert(df.collect().map(_.getLong(1)).sum === 1000L)
      assert(collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec if s.pipelined => s
      }.isEmpty)
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

  test("a reader RDD reports the reduce partition its index reads, not the index itself") {
    // The scheduler derives a pipelined producer's live reduce-partition set from the result
    // partitions a partial read runs, so it must know how a reader partition index maps to a REDUCE
    // index. Equal partition counts do not imply identity (a skew-split plus a coalesce has the
    // same
    // count and a different mapping), and a wrong answer here is not a hang but SILENTLY dropped
    // records. So the mapping is asked of the reader RDD. This pins the three spec shapes.
    withPipelinedSession { spark =>
      import org.apache.spark.sql.execution.{CoalescedPartitionSpec, PartialReducerPartitionSpec,
        ShuffledRowRDD}
      import spark.implicits._
      val df = spark.range(0, 1000, 1, 2).withColumn("k", ($"id" % 4)).repartition($"k")
      val exchange = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }.head
      val dep = exchange.shuffleDependency

      // width-1 coalesced spec: names exactly one reducer, in spec order (NOT the partition index).
      val widthOne = new ShuffledRowRDD(dep, exchange.metrics,
        Array(CoalescedPartitionSpec(3, 4), CoalescedPartitionSpec(1, 2)))
      assert(widthOne.reducePartitionIndex(0) === Some(3),
        "partition 0 reads reducer 3 here, so the mapping must not return the index")
      assert(widthOne.reducePartitionIndex(1) === Some(1))

      // a coalesced RANGE covers several reducers: no single reduce id -> uncomputable.
      val ranged = new ShuffledRowRDD(dep, exchange.metrics,
        Array(CoalescedPartitionSpec(0, 2), CoalescedPartitionSpec(2, 4)))
      assert(ranged.reducePartitionIndex(0).isEmpty,
        "a multi-reducer range has no single reduce partition")

      // a skew-split spec names its reducer explicitly.
      val split = new ShuffledRowRDD(dep, exchange.metrics,
        Array(PartialReducerPartitionSpec(2, 0, 1, 0L)))
      assert(split.reducePartitionIndex(0) === Some(2))

      // out-of-range index is uncomputable rather than an exception.
      assert(widthOne.reducePartitionIndex(99).isEmpty)
    }
  }

  test("a streaming query is left untouched in a feature-on session") {
    // IncrementalExecution.preparations inherits QueryExecution's list, so this batch-only rule
    // would otherwise run on streaming plans and flip every micro-batch exchange (state-store
    // shuffles, the static side of a stream-static join) to pipelined -- before
    // MarkPipelinedShuffleForRealTimeMode gets to make that decision, and against what it
    // deliberately does for the static side (leaving it regular so the gang does not demand slots
    // for stages that must finish first). The streaming engine owns that marking.
    withPipelinedSession { spark =>
      import spark.implicits._
      import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
      implicit val ctx = spark.sqlContext
      val input = MemoryStream[Int]
      val agg = input.toDF().selectExpr("value % 10 AS k").groupBy($"k").count()
      val q = agg.writeStream.format("memory").queryName("pipelined_stream_probe")
        .outputMode("complete").start()
      try {
        input.addData(1 to 200: _*)
        q.processAllAvailable()
        val plan = q.asInstanceOf[
            org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper]
          .streamingQuery.lastExecution.executedPlan
        val flipped = collect(plan) { case s: ShuffleExchangeExec if s.pipelined => s }
        val all = collect(plan) { case s: ShuffleExchangeExec => s }
        assert(all.nonEmpty,
          s"precondition: the streaming plan should contain a state-store shuffle; plan:\n$plan")
        assert(flipped.isEmpty,
          s"a streaming plan must not be rewritten by the batch rule; plan:\n$plan")
      } finally {
        q.stop()
        spark.sql("DROP VIEW IF EXISTS pipelined_stream_probe")
      }
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

private object PipelinedShuffleSqlSuite {
  @volatile var started = new CountDownLatch(1)
  @volatile var release = new CountDownLatch(1)

  def waitForRelease(id: Long): Long = {
    started.countDown()
    require(release.await(30, TimeUnit.SECONDS), "Timed out waiting for concurrent action")
    id
  }
}
