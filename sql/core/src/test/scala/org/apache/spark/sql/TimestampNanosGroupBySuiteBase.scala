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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end `GROUP BY` correctness tests over the nanosecond-precision timestamp types
 * `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`), part of the nanosecond timestamp
 * preview (SPARK-56822). Grouping over these types executes today with no production change -- it
 * rides on the nanos hashing and equality the types already implement (SPARK-57103 extended
 * `Murmur3Hash` / `XxHash64` / `HiveHash` to hash the carrier's two fields, `epochMicros: Long`
 * and `nanosWithinMicro: Short in [0, 999]`). There is no coverage organized around grouping by
 * column type, so this per-type suite adds it and locks the regression.
 *
 * Headline risk: SUB-MICROSECOND key correctness, mirroring the join suite. The carrier is
 * `TimestampNanosVal = (epochMicros: Long, nanosWithinMicro: Short in [0, 999])`. Every key in
 * these tables shares the SAME epochMicros (1577836800000000, = 2020-01-01T00:00:00Z) and differs
 * ONLY in nanosWithinMicro. So the micro-level path alone cannot tell the keys apart -- correct
 * grouping MUST be driven by the full nanos value:
 *   - two rows equal in epochMicros but DIFFERENT in nanosWithinMicro must NOT collapse into one
 *     group,
 *   - exact duplicates (equal incl. the sub-microsecond remainder) MUST collapse,
 *   - NULL keys, unlike an equi-join, DO group together -- all NULL rows land in a single group.
 * If the sub-microsecond remainder were ignored, the two distinct sub-microsecond keys here would
 * fold into one, dropping the group count from 3 to 2 and failing these tests loudly.
 *
 * Precision-safety: all sub-microsecond remainders are multiples of 100ns (100 / 900), which are
 * exact at every p in [7, 9] (`createDataFrame` floors nanosWithinMicro to (n/100)*100 at p=7 and
 * (n/10)*10 at p=8). So the SAME inputs and the SAME expected results are valid verbatim at all
 * three precisions, and the two distinct remainders never collide even at the coarsest p=7.
 *
 * Each test runs under whole-stage codegen on and off, so the same sub-microsecond grouping is
 * proven on the nanos hash path in both codegen modes, for NTZ and LTZ. The mixed-precision test
 * additionally pins that a `UNION` of two different nanos precisions widens the key to the higher
 * precision (`findWiderDateTimeType`) and preserves the distinction.
 *
 * The nanosecond timestamp types are gated behind a preview flag enabled by default under tests
 * (`Utils.isTesting`), so it is not set here. The session time zone is fixed so the
 * `TIMESTAMP_LTZ` (`Instant`) values are deterministic. The two subclasses run every test with
 * ANSI mode on and off.
 */
abstract class TimestampNanosGroupBySuiteBase extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  // Whole-stage codegen on (CODEGEN_ONLY) vs off (NO_CODEGEN). The hash-aggregate exec is the same
  // in both modes; only the WholeStageCodegenExec wrapper differs. Mirrors the join and functions
  // suites.
  protected val codegenModes: Seq[Seq[(String, String)]] = Seq(
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY"),
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN"))

  private def cgLabel(cgConf: Seq[(String, String)]): String =
    if (cgConf.contains(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true")) "codegen on"
    else "codegen off"

  // ---- relation builders: key column "k" of the given nanos type + an Int value column "v" ----
  //
  // Five rows, all sharing epochMicros (2020-01-01T00:00:00Z) and differing only within the
  // microsecond:
  //   - two rows with the 100ns key (an exact duplicate): must collapse into one group,
  //   - one row with the 900ns key: same microsecond, distinct sub-microsecond remainder,
  //   - two NULL-key rows: group together into a single NULL group.
  // The value column distinguishes the group signatures: sum(v) is 1+2=3 for the 100ns group,
  // 3 for the 900ns group, and 4+5=9 for the NULL group, so the three groups are all identifiable
  // by their (count, sum) pair even without inspecting the key.

  private def ntzDF(p: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000100"), 1),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000100"), 2),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000900"), 3),
        Row(null, 4),
        Row(null, 5))),
      new StructType().add("k", TimestampNTZNanosType(p)).add("v", IntegerType))

  private def ltzDF(p: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(Instant.parse("2020-01-01T00:00:00.000000100Z"), 1),
        Row(Instant.parse("2020-01-01T00:00:00.000000100Z"), 2),
        Row(Instant.parse("2020-01-01T00:00:00.000000900Z"), 3),
        Row(null, 4),
        Row(null, 5))),
      new StructType().add("k", TimestampLTZNanosType(p)).add("v", IntegerType))

  // Each family as (label, builder(p), widerType(p)). GROUP BY is type-agnostic, so the family only
  // picks the data builder and (for the mixed-precision widening assertion) the expected widened
  // key type.
  protected val families: Seq[(String, Int => DataFrame, Int => DataType)] = Seq(
    ("NTZ", ntzDF, (p: Int) => TimestampNTZNanosType(p)),
    ("LTZ", ltzDF, (p: Int) => TimestampLTZNanosType(p)))

  // Mixed-precision UNION pairs; the key widens to max(pl, pr).
  private val mixedPrecisionPairs: Seq[(Int, Int)] = Seq((7, 9), (7, 8), (8, 9))

  for {
    (family, builder, widerType) <- families
    cgConf <- codegenModes
  } {
    // ========================================================================================
    // GROUP BY a nanos key: exact duplicates collapse, but two keys sharing epochMicros and
    // differing only within the microsecond stay in separate groups. Aggregates (count, sum) are
    // computed per group. Expected three groups: 100ns (count 2, sum 3), 900ns (count 1, sum 3),
    // NULL (count 2, sum 9). If the sub-microsecond remainder were ignored, 100ns and 900ns would
    // merge into a single (count 3, sum 6) group and this checkAnswer would fail.
    // ========================================================================================
    test(s"$family nanos GROUP BY distinguishes the sub-microsecond remainder - " +
      s"${cgLabel(cgConf)}") {
      withSQLConf(cgConf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val grouped = builder(p).groupBy("k")
            .agg(count(lit(1)).as("c"), sum(col("v")).as("s"))
          checkAnswer(grouped.select("c", "s"),
            Seq(Row(2L, 3L), Row(1L, 3L), Row(2L, 9L)))
        }
      }
    }

    // ========================================================================================
    // GROUP BY over a mixed-precision key: a UNION of the same data at two different nanos
    // precisions widens the key to max(pl, pr) (findWiderDateTimeType). Grouping the widened column
    // still separates the two sub-microsecond keys (all remainders are multiples of 100ns, exact at
    // every p in [7, 9]). The union doubles every row, so counts/sums double: 100ns (4, 6),
    // 900ns (2, 6), NULL (4, 18).
    // ========================================================================================
    test(s"$family nanos GROUP BY across mixed precisions widens key - ${cgLabel(cgConf)}") {
      withSQLConf(cgConf: _*) {
        mixedPrecisionPairs.foreach { case (pl, pr) =>
          val union = builder(pl).union(builder(pr))
          assert(union.schema("k").dataType == widerType(math.max(pl, pr)),
            s"expected key widened to ${widerType(math.max(pl, pr))} for ($pl, $pr)")
          val grouped = union.groupBy("k")
            .agg(count(lit(1)).as("c"), sum(col("v")).as("s"))
          checkAnswer(grouped.select("c", "s"),
            Seq(Row(4L, 6L), Row(2L, 6L), Row(4L, 18L)))
        }
      }
    }

    // ========================================================================================
    // NULL grouping: unlike an equi-join (where NULL never matches NULL), GROUP BY treats NULL as a
    // single group. Both NULL-key rows land in one group (count 2).
    // ========================================================================================
    test(s"$family nanos GROUP BY collapses NULL keys into a single group - ${cgLabel(cgConf)}") {
      withSQLConf(cgConf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val nullGroup = builder(p).groupBy("k").count().filter(col("k").isNull)
          checkAnswer(nullGroup.select("count"), Seq(Row(2L)))
        }
      }
    }
  }
}

// Runs the nanosecond timestamp GROUP BY tests with ANSI mode enabled explicitly.
class TimestampNanosGroupByAnsiOnSuite extends TimestampNanosGroupBySuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp GROUP BY tests with ANSI mode disabled explicitly.
class TimestampNanosGroupByAnsiOffSuite extends TimestampNanosGroupBySuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
