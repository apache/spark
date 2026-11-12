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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end `DISTINCT` correctness tests over the nanosecond-precision timestamp types
 * `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`), part of the nanosecond timestamp
 * preview (SPARK-56822). Set-distinctness over these types executes today with no production
 * change -- it rides on the nanos hashing and equality the types already implement (SPARK-57103
 * extended `Murmur3Hash` / `XxHash64` / `HiveHash` to hash the carrier's two fields,
 * `epochMicros: Long` and `nanosWithinMicro: Short in [0, 999]`). There is no coverage organized
 * around distinctness by column type, so this per-type suite adds it and locks the regression.
 *
 * Headline risk: SUB-MICROSECOND distinctness, mirroring the join suite. The carrier is
 * `TimestampNanosVal = (epochMicros: Long, nanosWithinMicro: Short in [0, 999])`. Every value in
 * these tables shares the SAME epochMicros (1577836800000000, = 2020-01-01T00:00:00Z) and differs
 * ONLY in nanosWithinMicro. So the micro-level path alone cannot tell them apart -- correct
 * distinctness MUST be driven by the full nanos value:
 *   - two values equal in epochMicros but DIFFERENT in nanosWithinMicro must NOT be deduplicated,
 *   - exact duplicates (equal incl. the sub-microsecond remainder) MUST be deduplicated,
 *   - NULL, unlike an equi-join, IS a single distinct value -- `DISTINCT` keeps exactly one NULL.
 * If the sub-microsecond remainder were ignored, the two distinct sub-microsecond values here
 * would fold into one, dropping the distinct-row count from 3 to 2 and failing these tests loudly.
 *
 * Precision-safety: all sub-microsecond remainders are multiples of 100ns (100 / 900), which are
 * exact at every p in [7, 9] (`createDataFrame` floors nanosWithinMicro to (n/100)*100 at p=7 and
 * (n/10)*10 at p=8). So the SAME inputs and the SAME expected results are valid verbatim at all
 * three precisions, and the two distinct remainders never collide even at the coarsest p=7.
 *
 * Each test runs under whole-stage codegen on and off, so the same sub-microsecond distinctness is
 * proven on the nanos hash path in both codegen modes, for NTZ and LTZ. The mixed-precision test
 * additionally pins that a `UNION` of two different nanos precisions widens the column to the
 * higher precision (`findWiderDateTimeType`) and preserves the distinction.
 *
 * The nanosecond timestamp types are gated behind a preview flag enabled by default under tests
 * (`Utils.isTesting`), so it is not set here. The session time zone is fixed so the
 * `TIMESTAMP_LTZ` (`Instant`) values are deterministic. The two subclasses run every test with
 * ANSI mode on and off.
 */
abstract class TimestampNanosDistinctSuiteBase extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  // Whole-stage codegen on (CODEGEN_ONLY) vs off (NO_CODEGEN). The hash-aggregate exec backing
  // DISTINCT is the same in both modes; only the WholeStageCodegenExec wrapper differs. Mirrors the
  // join and functions suites.
  protected val codegenModes: Seq[Seq[(String, String)]] = Seq(
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY"),
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN"))

  private def cgLabel(cgConf: Seq[(String, String)]): String =
    if (cgConf.contains(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true")) "codegen on"
    else "codegen off"

  // ---- relation builders: single nanos column "k" ----
  //
  // Four rows, all sharing epochMicros (2020-01-01T00:00:00Z) and differing only within the
  // microsecond:
  //   - two rows with the 100ns value (an exact duplicate): collapse to one distinct row,
  //   - one row with the 900ns value: same microsecond, distinct sub-microsecond remainder,
  //   - one NULL row: a single distinct NULL.
  // Expected three distinct values: 100ns, 900ns, NULL.

  private def ntzDF(p: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000100")),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000100")),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000900")),
        Row(null))),
      new StructType().add("k", TimestampNTZNanosType(p)))

  private def ltzDF(p: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(Instant.parse("2020-01-01T00:00:00.000000100Z")),
        Row(Instant.parse("2020-01-01T00:00:00.000000100Z")),
        Row(Instant.parse("2020-01-01T00:00:00.000000900Z")),
        Row(null))),
      new StructType().add("k", TimestampLTZNanosType(p)))

  // The external nanosecond remainder of a collected value, whichever family it came back as
  // (NTZ -> LocalDateTime, LTZ -> Instant). Every value here shares epochMicros, so the remainder
  // alone identifies it.
  private def nanoOf(v: Any): Int = v match {
    case ldt: LocalDateTime => ldt.getNano
    case i: Instant => i.getNano
  }

  // Each family as (label, builder(p), widerType(p)). DISTINCT is type-agnostic, so the family only
  // picks the data builder and (for the mixed-precision widening assertion) the expected widened
  // column type.
  protected val families: Seq[(String, Int => DataFrame, Int => DataType)] = Seq(
    ("NTZ", ntzDF, (p: Int) => TimestampNTZNanosType(p)),
    ("LTZ", ltzDF, (p: Int) => TimestampLTZNanosType(p)))

  // Mixed-precision UNION pairs; the column widens to max(pl, pr).
  private val mixedPrecisionPairs: Seq[(Int, Int)] = Seq((7, 9), (7, 8), (8, 9))

  // Asserts that `distinct` holds exactly the three expected values: the 100ns and 900ns remainders
  // and a single NULL. Checking the actual remainders (not just the row count) pins that the
  // surviving rows carry the full sub-microsecond value.
  private def assertDistinctKeys(distinct: DataFrame, p: Int): Unit = {
    assert(distinct.count() == 3, s"expected 3 distinct values (100ns, 900ns, NULL) at p=$p")
    val rows = distinct.collect()
    assert(rows.count(_.isNullAt(0)) == 1, s"expected exactly one NULL at p=$p")
    val remainders = rows.filterNot(_.isNullAt(0)).map(r => nanoOf(r.get(0))).toSet
    assert(remainders == Set(100, 900),
      s"expected distinct remainders {100, 900} at p=$p, got $remainders")
  }

  for {
    (family, builder, widerType) <- families
    cgConf <- codegenModes
  } {
    // ========================================================================================
    // DISTINCT over a nanos column: exact duplicates are removed, sub-microsecond-distinct values
    // are kept, and NULL survives as exactly one row. Expected three distinct rows: 100ns, 900ns,
    // NULL. If the sub-microsecond remainder were ignored, 100ns and 900ns would collapse to one,
    // dropping the count to 2 and failing this assertion.
    // ========================================================================================
    test(s"$family nanos DISTINCT keeps sub-microsecond-distinct values - ${cgLabel(cgConf)}") {
      withSQLConf(cgConf: _*) {
        Seq(7, 8, 9).foreach { p =>
          assertDistinctKeys(builder(p).select("k").distinct(), p)
        }
      }
    }

    // ========================================================================================
    // DISTINCT over a mixed-precision column: a UNION of the same data at two different nanos
    // precisions widens the column to max(pl, pr) (findWiderDateTimeType). DISTINCT over the
    // widened column still separates the two sub-microsecond values and still collapses to three
    // rows (the union only adds more exact duplicates; all remainders are multiples of 100ns,
    // exact at every p in [7, 9]).
    // ========================================================================================
    test(s"$family nanos DISTINCT across mixed precisions widens column - ${cgLabel(cgConf)}") {
      withSQLConf(cgConf: _*) {
        mixedPrecisionPairs.foreach { case (pl, pr) =>
          val union = builder(pl).union(builder(pr))
          assert(union.schema("k").dataType == widerType(math.max(pl, pr)),
            s"expected column widened to ${widerType(math.max(pl, pr))} for ($pl, $pr)")
          assertDistinctKeys(union.distinct(), math.max(pl, pr))
        }
      }
    }
  }
}

// Runs the nanosecond timestamp DISTINCT tests with ANSI mode enabled explicitly.
class TimestampNanosDistinctAnsiOnSuite extends TimestampNanosDistinctSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp DISTINCT tests with ANSI mode disabled explicitly.
class TimestampNanosDistinctAnsiOffSuite extends TimestampNanosDistinctSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
