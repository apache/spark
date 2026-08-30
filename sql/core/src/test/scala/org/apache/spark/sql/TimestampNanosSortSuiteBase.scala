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
 * End-to-end SORT / ORDER BY tests over the nanosecond-precision timestamp types
 * `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`). These capabilities ride on the
 * orderability the nanosecond types already implement, so no production change is required.
 *
 * Scope note: per-type ordering correctness is already covered generically -- the nanos types are
 * in `DataTypeTestUtils.atomicTypes` (SPARK-57259), so `OrderingSuite` exercises interpreted vs
 * generated ordering for them (and SPARK-57103 added the sub-microsecond tie-break, Long-boundary,
 * pre-epoch, NULLS-first and precision-independence cases there), and `SortSuite` runs physical
 * `SortExec` (radix on and off) over those types. This suite therefore only covers the DataFrame /
 * SQL end-to-end scenarios NOT exercised by those generic suites:
 *   - a public-API `Dataset.orderBy` smoke test that also pins the sub-microsecond tie-break,
 *   - mixed-precision ORDER BY via `UNION ALL` (ordering after type-coercion widening),
 *   - the vectorized-ORC-read-then-sort columnar path, and
 *   - the cache-then-sort path (the in-memory columnar cache learned the nanos types in
 *     SPARK-57735, so caching a nanos column and sorting it now round-trips correctly).
 *
 * The nanosecond timestamp types are gated behind a preview flag enabled by default under tests
 * (`Utils.isTesting`), so it is not set here. The session time zone is fixed so the
 * `TIMESTAMP_LTZ` (`Instant`) values render deterministically. The two subclasses run every test
 * with ANSI mode on and off.
 *
 * NOTE on assertions: `checkAnswer` is order-INSENSITIVE (QueryTest sorts both sides), so it
 * cannot verify ORDER BY ordering. Ordering claims use `df.orderBy(...).collect().toSeq ===
 * <explicitly-ordered Seq[Row]>`; `checkAnswer` is used only as a value-set (multiset) cross-check.
 */
abstract class TimestampNanosSortSuiteBase extends SharedSparkSession {

  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  // Exercise both genComp arms: forced whole-stage codegen, then forced interpreted fallback.
  // Mirrors TimestampNanosFunctionsSuiteBase.scala.
  protected val codegenModes: Seq[Seq[(String, String)]] = Seq(
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY"),
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN"))

  // Single nanosecond TIMESTAMP_NTZ(p) column "c"; a null element becomes a NULL row.
  protected def ntzDF(values: Seq[String], precision: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        values.map(s => Row(if (s == null) null else LocalDateTime.parse(s)))),
      new StructType().add("c", TimestampNTZNanosType(precision)))

  // Single nanosecond TIMESTAMP_LTZ(p) column "c"; a null element becomes a NULL row.
  protected def ltzDF(values: Seq[String], precision: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        values.map(s => Row(if (s == null) null else Instant.parse(s)))),
      new StructType().add("c", TimestampLTZNanosType(precision)))

  // ==========================================================================================
  // Public-API ORDER BY smoke test, also pinning the sub-microsecond tie-break end to end.
  // ==========================================================================================
  // The first two non-null values share epochMicros (..00.000000001 and ..00.000000999 are both
  // inside micro 1577836800000000); the third (..00.000001000) rolls into the NEXT micro. The full
  // TimestampNanosVal.compareTo must order them 001 < 999 < 1000 through the Dataset.orderBy path,
  // on both the whole-stage codegen and interpreted comparison arms, for NTZ and LTZ.
  test("Dataset.orderBy over a nanosecond key tie-breaks on the sub-microsecond remainder") {
    val ntzVals = Seq(
      "2020-01-01T00:00:00.000001000",
      "2020-01-01T00:00:00.000000999",
      "2020-01-01T00:00:00.000000001",
      null)
    val ntzAsc = Seq(
      Row(LocalDateTime.parse("2020-01-01T00:00:00.000000001")),
      Row(LocalDateTime.parse("2020-01-01T00:00:00.000000999")),
      Row(LocalDateTime.parse("2020-01-01T00:00:00.000001000")))
    val ltzVals = Seq(
      "2020-01-01T00:00:00.000001000Z",
      "2020-01-01T00:00:00.000000999Z",
      "2020-01-01T00:00:00.000000001Z",
      null)
    val ltzAsc = Seq(
      Row(Instant.parse("2020-01-01T00:00:00.000000001Z")),
      Row(Instant.parse("2020-01-01T00:00:00.000000999Z")),
      Row(Instant.parse("2020-01-01T00:00:00.000001000Z")))

    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        // --- NTZ --- ASC default => NULLS FIRST; DESC default => NULLS LAST.
        val ntz = ntzDF(ntzVals, 9)
        assert(ntz.orderBy($"c".asc).collect().toSeq === (Row(null) +: ntzAsc))
        assert(ntz.orderBy($"c".desc).collect().toSeq === (ntzAsc.reverse :+ Row(null)))
        checkAnswer(ntz.filter($"c".isNotNull), ntzAsc)

        // --- LTZ ---
        val ltz = ltzDF(ltzVals, 9)
        assert(ltz.orderBy($"c".asc).collect().toSeq === (Row(null) +: ltzAsc))
        assert(ltz.orderBy($"c".desc).collect().toSeq === (ltzAsc.reverse :+ Row(null)))
        checkAnswer(ltz.filter($"c".isNotNull), ltzAsc)
      }
    }
  }

  // ==========================================================================================
  // Mixed-precision ORDER BY via UNION ALL (widens to the wider p, findWiderDateTimeType).
  // ==========================================================================================
  // TypeCoercionHelper.findWiderDateTimeType widens nanos by max precision within the (NTZ) family,
  // so p=7 UNION ALL p=9 -> TimestampNTZNanosType(9). The p=9 frame's ..001 remainder needs full
  // nanos; after widening the global order must be exact. (Remainders are 100ns multiples so the
  // p=7 frame's values are exact at precision 7.)
  test("ORDER BY over a UNION ALL of mixed-precision nanosecond timestamps orders correctly") {
    val p7 = ntzDF(Seq(
      "2020-01-01T00:00:00.000000200",
      "2020-01-01T00:00:00.000000800"), 7)
    val p9 = ntzDF(Seq(
      "2020-01-01T00:00:00.000000001",
      "2020-01-01T00:00:00.000000999"), 9)
    val unioned = p7.unionByName(p9)
    assert(unioned.schema("c").dataType === TimestampNTZNanosType(9))
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        assert(unioned.orderBy($"c".asc).collect().toSeq === Seq(
          Row(LocalDateTime.parse("2020-01-01T00:00:00.000000001")),
          Row(LocalDateTime.parse("2020-01-01T00:00:00.000000200")),
          Row(LocalDateTime.parse("2020-01-01T00:00:00.000000800")),
          Row(LocalDateTime.parse("2020-01-01T00:00:00.000000999"))))
      }
    }
  }

  // Same mixed-precision UNION ALL ordering, on the TIMESTAMP_LTZ family: findWiderDateTimeType
  // widens by max precision within the LTZ family too, so p=7 UNION ALL p=9 yields
  // TimestampLTZNanosType(9).
  test("ORDER BY over a UNION ALL of mixed-precision LTZ nanosecond timestamps orders correctly") {
    val p7 = ltzDF(Seq(
      "2020-01-01T00:00:00.000000200Z",
      "2020-01-01T00:00:00.000000800Z"), 7)
    val p9 = ltzDF(Seq(
      "2020-01-01T00:00:00.000000001Z",
      "2020-01-01T00:00:00.000000999Z"), 9)
    val unioned = p7.unionByName(p9)
    assert(unioned.schema("c").dataType === TimestampLTZNanosType(9))
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        assert(unioned.orderBy($"c".asc).collect().toSeq === Seq(
          Row(Instant.parse("2020-01-01T00:00:00.000000001Z")),
          Row(Instant.parse("2020-01-01T00:00:00.000000200Z")),
          Row(Instant.parse("2020-01-01T00:00:00.000000800Z")),
          Row(Instant.parse("2020-01-01T00:00:00.000000999Z"))))
      }
    }
  }

  // ==========================================================================================
  // POSITIVE columnar path: vectorized ORC read THEN sort -- executes and is correct today.
  // ==========================================================================================
  // ColumnarToRowExec reads nanos via the typed leaf getters getTimestampNTZNanos/
  // getTimestampLTZNanos (and via UnsafeProjection), never through ColumnarRow.copy()/get; the
  // vectorized ORC batch path is taken (OrcFileFormat accepts nanos; OrcAtomicColumnVector has a
  // nanos arm). Tie-break values share epochMicros and differ only in nanosWithinMicro.
  test("sort over a nanosecond column read back from vectorized ORC orders correctly") {
    Seq(7, 8, 9).foreach { p =>
      withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "true") {
        // --- NTZ --- exercises the getTimestampNTZNanos columnar leaf-getter arm.
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          val schema = new StructType().add("c", TimestampNTZNanosType(p))
          val data = Seq(
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000900")),
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000100")),
            Row(null))
          spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
            .write.mode("overwrite").orc(path)
          val read = spark.read.schema(schema).orc(path)
          // checkAnswer is order-INSENSITIVE (see suite NOTE), so it cannot prove the read-back
          // path SORTS correctly; observe the ordering with a collect-strict comparison against an
          // explicitly-ordered Seq (values are distinct, so collected order is the ordering proof).
          assert(read.orderBy($"c".asc_nulls_first).collect().toSeq === Seq(
            Row(null),
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000100")),
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000900"))))
        }
        // --- LTZ --- exercises the symmetric getTimestampLTZNanos columnar leaf-getter arm.
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          val schema = new StructType().add("c", TimestampLTZNanosType(p))
          val data = Seq(
            Row(Instant.parse("2020-01-01T00:00:00.000000900Z")),
            Row(Instant.parse("2020-01-01T00:00:00.000000100Z")),
            Row(null))
          spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
            .write.mode("overwrite").orc(path)
          val read = spark.read.schema(schema).orc(path)
          assert(read.orderBy($"c".asc_nulls_first).collect().toSeq === Seq(
            Row(null),
            Row(Instant.parse("2020-01-01T00:00:00.000000100Z")),
            Row(Instant.parse("2020-01-01T00:00:00.000000900Z"))))
        }
      }
    }
  }

  // ==========================================================================================
  // CACHE path: cache a nanosecond column, then sort -- round-trips through CachedBatch.
  // ==========================================================================================
  // SPARK-57735 taught the in-memory columnar cache the nanos types (a TIMESTAMP_NANOS ColumnType /
  // ColumnBuilder / ColumnAccessor / ColumnStats plus the GenerateColumnAccessor branch), so
  // .cache() now round-trips the 16-byte TimestampNanosVal payload (epochMicros + nanosWithinMicro)
  // through CachedBatch instead of throwing. Nanos are non-primitive for the default cache
  // (DefaultCachedBatchSerializer.supportsColumnarOutput is primitive-only), so the read takes the
  // row path. The action below drives cache-WRITE (ColumnBuilder) + cache-READ (ColumnAccessor) +
  // sort end to end; values share epochMicros and differ only in nanosWithinMicro, so an exact
  // ordered read-back proves the sub-microsecond remainder survives the cache round trip.
  test("caching a nanosecond-precision timestamp column then sorting round-trips and orders") {
    Seq(7, 8, 9).foreach { p =>
      Seq[(DataType, Seq[Row], Seq[Row])](
        (TimestampNTZNanosType(p),
          Seq(
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000900")),
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000100")),
            Row(null)),
          Seq(
            Row(null),
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000100")),
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000900")))),
        (TimestampLTZNanosType(p),
          Seq(
            Row(Instant.parse("2020-01-01T00:00:00.000000900Z")),
            Row(Instant.parse("2020-01-01T00:00:00.000000100Z")),
            Row(null)),
          Seq(
            Row(null),
            Row(Instant.parse("2020-01-01T00:00:00.000000100Z")),
            Row(Instant.parse("2020-01-01T00:00:00.000000900Z"))))
      ).foreach { case (dt, data, expectedAsc) =>
        val schema = new StructType().add("c", dt)
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).cache()
        try {
          assert(df.orderBy($"c".asc_nulls_first).collect().toSeq === expectedAsc)
        } finally {
          df.unpersist()
        }
      }
    }
  }
}

// Runs the nanosecond timestamp sort tests with ANSI mode enabled explicitly.
class TimestampNanosSortAnsiOnSuite extends TimestampNanosSortSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp sort tests with ANSI mode disabled explicitly.
class TimestampNanosSortAnsiOffSuite extends TimestampNanosSortSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
