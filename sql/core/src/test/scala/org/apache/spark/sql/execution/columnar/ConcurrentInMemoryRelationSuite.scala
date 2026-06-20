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

package org.apache.spark.sql.execution.columnar

import java.io.File
import java.util.concurrent.CountDownLatch

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Regression test for SPARK-57547: concurrent first-touch of one cold table cache must not let
 * duplicate partition computes silently drop rows.
 *
 * AQE creates a separate `TableCacheQueryStageExec` for every reference to the same cache (table
 * cache stages are never reused), and each one submits its own build job over the shared cache RDD.
 * A query that references a cached relation several times therefore first-touches the cold cache
 * from several jobs at once. Spark has no global cross-executor "compute this partition once"
 * barrier, so the same partition can be computed by multiple executors. If the cache decided it was
 * "loaded" from a raw task-completion count (the legacy behavior), those duplicate completions
 * could push the count to the partition count while a row-producing partition was still being
 * computed, falsely marking the cache loaded with rowCount 0 -- which lets AQE propagate an empty
 * relation and silently lose rows.
 *
 * The fix counts the DISTINCT set of materialized partitions instead, so duplicate computes can no
 * longer mark the cache loaded early. These tests reproduce the race deterministically: a two-stage
 * gate holds the row-producing partition while the empty-output partition's duplicate cross-executor
 * completions accumulate. With distinct tracking the cache stays correctly not-loaded while a
 * partition is still building, so the consumer observes every row; were the loaded check to fall
 * back to a raw task-completion count it would latch the cache as loaded with rowCount 0 and let AQE
 * propagate an empty relation, losing rows (which the repro detects as a row-count mismatch). A
 * multi-executor `local-cluster` session is required so the duplicate computes land on different
 * executors.
 */
class ConcurrentInMemoryRelationSuite extends SparkFunSuite with LocalSparkContext with Eventually {

  private def cacheBuilderOf(ds: Dataset[_]): CachedRDDBuilder = {
    val relations = ds.queryExecution.withCachedData.collect { case i: InMemoryRelation => i }
    assert(relations.length == 1)
    relations.head.cacheBuilder
  }

  private def withSession(numExecutors: Int = 4)(
      f: SparkSession => Unit): Unit = {
    val conf = new SparkConf()
      .setMaster(s"local-cluster[$numExecutors,1,1024]")
      .setAppName("ConcurrentInMemoryRelationSuite")
    sc = new SparkContext(conf)
    try {
      // Wait for all executors to register so tasks spread one-per-executor as the tests assume.
      eventually(timeout(Span(60, Seconds)), interval(Span(200, Millis))) {
        assert(sc.getExecutorIds().size == numExecutors)
      }
      f(SparkSession.builder().sparkContext(sc).getOrCreate())
    } finally {
      resetSparkContext()
    }
  }

  /**
   * Drives the actual SPARK-57547 data loss deterministically.
   *
   * Caches a skewed join with two shuffle partitions: every partition has non-empty INPUT (so
   * neither is pruned as an empty task), but only the `skewKey` bucket produces OUTPUT rows -- so
   * one partition is row-producing and the other produces zero rows. A two-stage gate blocks every
   * partition's build inside `mapPartitions` until released. `numReferences` threads each submit
   * their own build job over the shared cache RDD (exactly as per-reference
   * `TableCacheQueryStageExec`s do); on `local-cluster[4,1,...]` (= numReferences x cachePartitions
   * task slots, one task per executor) the empty-output partition is computed by both references on
   * two distinct executors.
   *
   * Sequence: (1) the threads first-touch the cold cache, gating all `numReferences x
   * cachePartitions` tasks; (2) release only the empty-output partition, so its two cross-executor
   * completions land while the row-producing partition is still gated; (3) poll
   * `isCachedColumnBuffersLoaded` -- distinct-partition tracking keeps it false (a raw
   * task-completion count would instead reach cachePartitions here and latch a poisoned "loaded"
   * state with rowCount 0); (4) a consumer query (a GROUPED aggregate, where empty propagation could
   * collapse the result) sees the cache not-loaded and plans against the real rows -- had it been
   * poisoned, AQE would have propagated an empty relation and dropped rows; (5) release the
   * producing partition. Returns (rows the consumer observed, expected rows), equal unless poisoned.
   */
  private def runDataLossRepro(spark: SparkSession): (Long, Long) = {
    import spark.implicits._
    val numKeys = 64
    val skewKey = 42
    val rowsPerKey = 500
    val numReferences = 2
    val cachePartitions = 2 // one row-producing, one empty-output (see shuffle.partitions below)
    val expected = rowsPerKey.toLong * rowsPerKey.toLong // only the skewKey bucket joins

    // Exactly two shuffle partitions (one row-producing, one empty-output), no broadcast so the
    // join shuffles, and build the cache with AQE off so the skewed producing partition is not
    // rebalanced away (which would defuse the window). Consumers below run with AQE on so they go
    // through TableCacheQueryStageExec + empty-relation propagation.
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    val gateDir = Utils.createTempDir()
    def file(name: String) = new File(gateDir, name)
    val releaseEmpty = file("releaseEmpty").getAbsolutePath
    val releaseProducing = file("releaseProducing").getAbsolutePath
    val entryDir = gateDir.getAbsolutePath

    def side(matchSalt: Int, valueCol: String): DataFrame =
      spark.range(0, numKeys.toLong * rowsPerKey).select(
        ($"id" % numKeys).cast("int").as("k"),
        when(($"id" % numKeys) === skewKey, lit(0)).otherwise(lit(matchSalt)).as("salt"),
        $"id".as(valueCol))

    val joined = side(1, "lv")
      .join(side(2, "rv"), Seq("k", "salt"))
      .select($"k", $"lv").as[(Int, Long)]

    // Two-stage gate: every partition signals it has entered (past the block-existence check) and
    // waits for releaseEmpty; the row-producing partition (the only one with rows) waits longer.
    val gated = joined.mapPartitions { iter =>
      val buffered = iter.buffered
      val isProducing = buffered.hasNext
      file(s"entered-${java.util.UUID.randomUUID()}").createNewFile()
      def waitFor(path: String): Unit = {
        val deadline = System.currentTimeMillis() + 60000
        while (!new File(path).exists() && System.currentTimeMillis() < deadline) Thread.sleep(50)
      }
      waitFor(releaseEmpty)
      if (isProducing) waitFor(releaseProducing)
      buffered
    }.toDF("k", "lv")

    val cached = gated.cache()
    try {
      val builder = cacheBuilderOf(cached)
      // Cache plan captured (static, 2 partitions); consumers from here on use AQE.
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      // Every reference launches its own build job over the shared cache RDD (no dedup at this
      // layer), so the empty partition is computed by every reference: numReferences x
      // cachePartitions gated tasks.
      val expectedEntries = numReferences * cachePartitions
      val rdd = builder.cachedColumnBuffers
      val submitted = new CountDownLatch(numReferences)
      val pool = ThreadUtils.newDaemonFixedThreadPool(numReferences, "spark57547-dataloss")
      try {
        val firstTouch = (1 to numReferences).map { _ =>
          pool.submit(new java.util.concurrent.Callable[Unit] {
            override def call(): Unit = {
              val f = spark.sparkContext.submitJob(
                rdd,
                (_: Iterator[CachedBatch]) => (),
                0 until rdd.getNumPartitions,
                (_: Int, _: Unit) => (),
                ())
              submitted.countDown()
              ThreadUtils.awaitResult(f, 120.seconds)
            }
          })
        }
        assert(submitted.await(60, java.util.concurrent.TimeUnit.SECONDS))
        // Wait until every build task is parked at the gate (all have passed the block-existence
        // check), so releasing the empty partition forces its cross-executor completions to run.
        eventually(timeout(Span(60, Seconds)), interval(Span(100, Millis))) {
          val entered = new File(entryDir).listFiles().count(_.getName.startsWith("entered-"))
          assert(entered == expectedEntries, s"entered=$entered expected=$expectedEntries")
        }

        // Stage 1: release ONLY the empty-output partition; the producing partition stays gated.
        assert(new File(releaseEmpty).createNewFile())

        // Were the loaded check to fall back to a raw task-completion count, the empty partition's
        // duplicate cross-executor completions would push that count to cachePartitions even though
        // the producing partition has not run, latching the cache as "loaded" with rowCount 0. We
        // read it through the relation handle -- exactly what AQE's stats reads do in production --
        // and the one-way latch would make the poison permanent (the producing partition is still
        // gated when the consumer runs below). With distinct-partition accounting the cache stays
        // not loaded here, so this poll times out and we fall through to a normal (complete) build.
        val poisoned =
          try {
            eventually(timeout(Span(30, Seconds)), interval(Span(100, Millis))) {
              assert(builder.isCachedColumnBuffersLoaded)
            }
            true
          } catch {
            case _: org.scalatest.exceptions.TestFailedException => false
          }

        // A GROUPED aggregate (not a global count): AQE empty-relation propagation collapses the
        // whole result when the cache stage is (falsely) reported as a zero-row materialized stage;
        // a global aggregate over empty would still emit one row and mask the loss.
        val observed = if (poisoned) {
          // The cache lied (loaded with rowCount 0 while the producing partition is still gated and
          // unbuilt). The consumer plans against it and AQE propagates an empty relation, so the
          // rows silently vanish. The producing partition stays gated, so this is deterministic.
          val consumer = cached.groupBy("k").count()
          val rows = consumer.collect()
          assert(consumer.queryExecution.executedPlan.toString.contains("EmptyRelation"),
            "expected AQE to propagate an empty relation from the poisoned cache stage")
          assert(new File(releaseProducing).createNewFile()) // unblock the build for clean shutdown
          rows.map(_.getLong(1)).sum
        } else {
          // The cache is correctly not loaded, so let the producing partition finish and the
          // consumer observes every row.
          assert(new File(releaseProducing).createNewFile())
          cached.groupBy("k").count().collect().map(_.getLong(1)).sum
        }
        firstTouch.foreach(_.get(120, java.util.concurrent.TimeUnit.SECONDS))
        (observed, expected)
      } finally {
        pool.shutdown()
      }
    } finally {
      cached.unpersist(blocking = true)
      Utils.deleteRecursively(gateDir)
    }
  }

  /**
   * Builds a cold cache whose partitions all carry rows and first-touches it concurrently from
   * `numReferences` jobs with every partition gated, so each partition is computed once per
   * reference on a distinct executor (`numReferences` duplicate cross-executor computes per
   * partition). Returns (reported materialized row count, expected rows); with distinct-partition
   * tracking on, the keyed accumulator de-duplicates the duplicate computes so the count is exact.
   */
  private def runDuplicateComputeStats(spark: SparkSession): (Long, Long) = {
    import spark.implicits._
    val numReferences = 2
    val cachePartitions = 2
    val numRows = 200L // split evenly across the partitions; every partition is non-empty

    val gateDir = Utils.createTempDir()
    def file(name: String) = new File(gateDir, name)
    val release = file("release").getAbsolutePath
    val entryDir = gateDir.getAbsolutePath

    // Every partition has rows and blocks at the gate until released, so all references' build
    // tasks are in flight (past the block-existence check) before any completes -- forcing the
    // duplicate cross-executor computes that the per-batch accumulator would over-count.
    val cached = spark.range(0, numRows, 1, cachePartitions).as[Long].mapPartitions { iter =>
      file(s"entered-${java.util.UUID.randomUUID()}").createNewFile()
      val deadline = System.currentTimeMillis() + 60000
      while (!new File(release).exists() && System.currentTimeMillis() < deadline) Thread.sleep(50)
      iter
    }.cache()
    try {
      val builder = cacheBuilderOf(cached)
      val rdd = builder.cachedColumnBuffers
      val submitted = new CountDownLatch(numReferences)
      val pool = ThreadUtils.newDaemonFixedThreadPool(numReferences, "spark57547-stats")
      try {
        val futures = (1 to numReferences).map { _ =>
          pool.submit(new java.util.concurrent.Callable[Unit] {
            override def call(): Unit = {
              val f = spark.sparkContext.submitJob(
                rdd,
                (_: Iterator[CachedBatch]) => (),
                0 until rdd.getNumPartitions,
                (_: Int, _: Unit) => (),
                ())
              submitted.countDown()
              ThreadUtils.awaitResult(f, 120.seconds)
            }
          })
        }
        assert(submitted.await(60, java.util.concurrent.TimeUnit.SECONDS))
        // Wait until every reference's task for every partition is parked at the gate, then release
        // them so each partition is computed once per reference.
        eventually(timeout(Span(60, Seconds)), interval(Span(100, Millis))) {
          val entered = new File(entryDir).listFiles().count(_.getName.startsWith("entered-"))
          assert(entered == numReferences * cachePartitions, s"entered=$entered")
        }
        assert(new File(release).createNewFile())
        futures.foreach(_.get(120, java.util.concurrent.TimeUnit.SECONDS))
        assert(builder.isCachedColumnBuffersLoaded)
        (builder.materializedRowCount, numRows)
      } finally {
        pool.shutdown()
      }
    } finally {
      cached.unpersist(blocking = true)
      Utils.deleteRecursively(gateDir)
    }
  }

  test("SPARK-57547: concurrent first-touch of a cold cache does not lose rows") {
    withSession() { spark =>
      val (observed, expected) = runDataLossRepro(spark)
      assert(observed == expected, s"consumer observed $observed rows, expected $expected")
    }
  }

  test("SPARK-57547: cache statistics are exact under duplicate cross-executor computes") {
    // Every partition is computed by both references, so the partition-keyed accumulator sees a
    // duplicate `add` per partition. Last-write-wins de-duplication keeps the reported row count
    // exact -- a naive summing accumulator would over-count under these duplicate computes.
    withSession() { spark =>
      val (rowCount, expected) = runDuplicateComputeStats(spark)
      assert(rowCount == expected,
        s"partition-keyed accumulator should report exact row count $expected, got $rowCount")
    }
  }
}
