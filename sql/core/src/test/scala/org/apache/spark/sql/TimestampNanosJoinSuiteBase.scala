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

package org.apache.spark.sql

import java.time.{Instant, LocalDateTime}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end equi-JOIN correctness tests over the nanosecond-precision timestamp types
 * `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`). Joins over these types execute today
 * with no production change -- they ride on the nanos hash+equals (hash joins) and the nanos
 * ordering+equals (sort-merge join) the types already implement. There is currently no join
 * coverage organized by column type, so this per-type suite adds it.
 *
 * Headline risk: SUB-MICROSECOND join-key correctness. The carrier is
 * `TimestampNanosVal = (epochMicros: Long, nanosWithinMicro: Short in [0, 999])`. Every join key
 * in these tables shares the SAME epochMicros (1577836800000000, = 2020-01-01T00:00:00Z) and
 * differs ONLY in nanosWithinMicro. So the micro-level path alone cannot tell the keys apart -- a
 * correct join MUST be driven by the full nanos value:
 *   - keys equal in epochMicros but DIFFERENT in nanosWithinMicro must NOT join,
 *   - keys fully equal (incl. the sub-microsecond remainder) MUST join,
 *   - NULL keys must NOT match (NULL never equals NULL in an equi-join).
 * If the sub-microsecond remainder were ignored, every non-null left row would spuriously match
 * every non-null right row on the shared micro and these tests would fail loudly.
 *
 * Precision-safety: all sub-microsecond remainders are multiples of 100ns (200/300/500/700/900),
 * which are exact at every p in [7, 9] (createDataFrame floors nanosWithinMicro to (n/100)*100 at
 * p=7 and (n/10)*10 at p=8). So the SAME inputs and the SAME expected rows are valid verbatim at
 * all three precisions, and the five distinct remainders never collide even at the coarsest p=7.
 *
 * Each test forces a specific physical join strategy (broadcast-hash / sort-merge / shuffled-hash)
 * with AQE disabled, asserts that exact exec node fired, and runs under whole-stage codegen on and
 * off -- so the same sub-microsecond equal/distinct relationship is proven on the nanos hash path
 * AND the nanos ordering path, in both codegen modes, for NTZ and LTZ.
 *
 * The nanosecond timestamp types are gated behind a preview flag enabled by default under tests
 * (`Utils.isTesting`), so it is not set here. The session time zone is fixed so the
 * `TIMESTAMP_LTZ` (`Instant`) values render deterministically. The two subclasses run every test
 * with ANSI mode on and off.
 */
abstract class TimestampNanosJoinSuiteBase extends SharedSparkSession with AdaptiveSparkPlanHelper {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  // Whole-stage codegen on (CODEGEN_ONLY) vs off (NO_CODEGEN). The join exec class is identical in
  // both modes; only the WholeStageCodegenExec wrapper differs, and the recursive class-based
  // assertion below descends through that wrapper, so this toggle is orthogonal to the assertion.
  // Mirrors TimestampNanosFunctionsSuiteBase.
  protected val codegenModes: Seq[Seq[(String, String)]] = Seq(
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY"),
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN"))

  // The three equi-join strategies, each as (label, exec class to assert, SQLConf overrides that
  // force it). AQE is disabled in every entry so the executed plan is final and the node is
  // deterministically findable.
  //   - BroadcastHashJoin: huge broadcast threshold so the (tiny) build side broadcasts.
  //   - SortMergeJoin: broadcast disabled, prefer-SMJ on, no SHJ force flag.
  //   - ShuffledHashJoin: broadcast disabled, prefer-SMJ off, plus the testing-only force flag --
  //     the only deterministic SHJ for two tiny EQUAL-sized inputs (bypasses the muchSmaller
  //     heuristic). The flag is a hard-coded string key (no SQLConf constant) gated on
  //     Utils.isTesting, which is true in the test JVM.
  protected val joinStrategies: Seq[(String, Class[_ <: SparkPlan], Seq[(String, String)])] = Seq(
    ("BroadcastHashJoin", classOf[BroadcastHashJoinExec], Seq(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString)),
    ("SortMergeJoin", classOf[SortMergeJoinExec], Seq(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true")),
    ("ShuffledHashJoin", classOf[ShuffledHashJoinExec], Seq(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      "spark.sql.join.forceApplyShuffledHashJoin" -> "true")))

  /**
   * Asserts that exactly one join exec of `execClass` is present in the executed plan of `df`.
   * `collect` (mixed in from AdaptiveSparkPlanHelper) recurses through AdaptiveSparkPlanExec,
   * QueryStageExec and WholeStageCodegenExec, so the inner join node is matched in both codegen
   * modes and under either AQE setting. Forces materialization of the plan first.
   */
  protected def assertJoinUsed(df: DataFrame, execClass: Class[_ <: SparkPlan]): Unit = {
    val plan = df.queryExecution.executedPlan
    val matched = collect(plan) { case p if execClass.isInstance(p) => p }
    assert(matched.size == 1,
      s"Expected exactly one ${execClass.getSimpleName}, found ${matched.size}.\n" +
        s"Executed plan:\n$plan")
  }

  // ---- relation builders: key column "k" of the given nanos type + an Int id column ----

  private def ntzLeft(p: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000500"), 1),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000200"), 2),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000900"), 3),
        Row(null, 4))),
      new StructType().add("k", TimestampNTZNanosType(p)).add("lid", IntegerType))

  private def ntzRight(p: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000500"), 10),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000700"), 20),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000300"), 30),
        Row(null, 40))),
      new StructType().add("k", TimestampNTZNanosType(p)).add("rid", IntegerType))

  private def ltzLeft(p: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(Instant.parse("2020-01-01T00:00:00.000000500Z"), 1),
        Row(Instant.parse("2020-01-01T00:00:00.000000200Z"), 2),
        Row(Instant.parse("2020-01-01T00:00:00.000000900Z"), 3),
        Row(null, 4))),
      new StructType().add("k", TimestampLTZNanosType(p)).add("lid", IntegerType))

  private def ltzRight(p: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(Instant.parse("2020-01-01T00:00:00.000000500Z"), 10),
        Row(Instant.parse("2020-01-01T00:00:00.000000700Z"), 20),
        Row(Instant.parse("2020-01-01T00:00:00.000000300Z"), 30),
        Row(null, 40))),
      new StructType().add("k", TimestampLTZNanosType(p)).add("rid", IntegerType))

  // Expected join outputs (order-insensitive). Selected as (lid, rid) so each row is identifiable.
  // INNER: only the fully-equal sub-microsecond pair (500ns == 500ns).
  private val expectedInner: Seq[Row] = Seq(Row(1, 10))
  // LEFT OUTER: all left rows. lid=1 matched; lid=2 (sub-micro mismatch 200 vs 700), lid=3
  // (left-only 900) and lid=4 (NULL key) each get a NULL rid. lid=2's NULL rid proves the
  // sub-microsecond non-match is real (not a same-micro false hit); lid=4's proves NULL != NULL.
  private val expectedLeftOuter: Seq[Row] =
    Seq(Row(1, 10), Row(2, null), Row(3, null), Row(4, null))

  // ==========================================================================================
  // NTZ: inner + left-outer over a sub-microsecond key, every strategy x codegen mode x p.
  // ==========================================================================================
  for {
    (stratName, execClass, stratConf) <- joinStrategies
    cgConf <- codegenModes
  } {
    val cgName = if (cgConf.exists(_ == (SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true"))) {
      "codegen on"
    } else {
      "codegen off"
    }

    test(s"NTZ nanos join distinguishes the sub-microsecond remainder - " +
      s"$stratName - $cgName") {
      withSQLConf((stratConf ++ cgConf): _*) {
        Seq(7, 8, 9).foreach { p =>
          val left = ntzLeft(p)
          val right = ntzRight(p)

          val inner = left.join(right, left("k") === right("k"), "inner")
            .select(left("lid"), right("rid"))
          assertJoinUsed(inner, execClass)
          checkAnswer(inner, expectedInner)

          val leftOuter = left.join(right, left("k") === right("k"), "left_outer")
            .select(left("lid"), right("rid"))
          assertJoinUsed(leftOuter, execClass)
          checkAnswer(leftOuter, expectedLeftOuter)
        }
      }
    }
  }

  // ==========================================================================================
  // LTZ: inner + left-outer over a sub-microsecond key, every strategy x codegen mode x p.
  // ==========================================================================================
  for {
    (stratName, execClass, stratConf) <- joinStrategies
    cgConf <- codegenModes
  } {
    val cgName = if (cgConf.exists(_ == (SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true"))) {
      "codegen on"
    } else {
      "codegen off"
    }

    test(s"LTZ nanos join distinguishes the sub-microsecond remainder - " +
      s"$stratName - $cgName") {
      withSQLConf((stratConf ++ cgConf): _*) {
        Seq(7, 8, 9).foreach { p =>
          val left = ltzLeft(p)
          val right = ltzRight(p)

          val inner = left.join(right, left("k") === right("k"), "inner")
            .select(left("lid"), right("rid"))
          assertJoinUsed(inner, execClass)
          checkAnswer(inner, expectedInner)

          val leftOuter = left.join(right, left("k") === right("k"), "left_outer")
            .select(left("lid"), right("rid"))
          assertJoinUsed(leftOuter, execClass)
          checkAnswer(leftOuter, expectedLeftOuter)
        }
      }
    }
  }
}

// Runs the nanosecond timestamp join tests with ANSI mode enabled explicitly.
class TimestampNanosJoinAnsiOnSuite extends TimestampNanosJoinSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp join tests with ANSI mode disabled explicitly.
class TimestampNanosJoinAnsiOffSuite extends TimestampNanosJoinSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
