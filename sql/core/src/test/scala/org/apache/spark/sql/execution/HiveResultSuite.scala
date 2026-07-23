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

package org.apache.spark.sql.execution

import java.time.{Duration, LocalDateTime, Period, Year, ZoneOffset}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, YearUDT}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution.HiveResult._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.types.{StructField, StructType, TimestampLTZNanosType, TimestampNTZNanosType, YearMonthIntervalType, YearMonthIntervalType => YM}


class HiveResultSuite extends SharedSparkSession {
  import testImplicits._

  test("date formatting in hive result") {
    DateTimeTestUtils.outstandingTimezonesIds.foreach { zoneId =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zoneId) {
        val dates = Seq("2018-12-28", "1582-10-03", "1582-10-04", "1582-10-15")
        val df = dates.toDF("a").selectExpr("cast(a as date) as b")
        val executedPlan1 = df.queryExecution.executedPlan
        val result = hiveResultString(executedPlan1)
        assert(result == dates)
        val executedPlan2 = df.selectExpr("array(b)").queryExecution.executedPlan
        val result2 = hiveResultString(executedPlan2)
        assert(result2 == dates.map(x => s"[$x]"))
      }
    }
  }

  test("time formatting in hive result") {
    val times = Seq(
      "00:00:00",
      "00:01:02.003004",
      "12:13:14.999999",
      "23:59:59.1234")
    val df = times.toDF("a").selectExpr("cast(a as time) as b")
    val executedPlan1 = df.queryExecution.executedPlan
    val result = hiveResultString(executedPlan1)
    assert(result == times)
    val executedPlan2 = df.selectExpr("array(b)").queryExecution.executedPlan
    val result2 = hiveResultString(executedPlan2)
    assert(result2 == times.map(x => s"[$x]"))
  }

  test("timestamp formatting in hive result") {
    val timestamps = Seq(
      "2018-12-28 01:02:03",
      "1582-10-03 01:02:03",
      "1582-10-04 01:02:03",
      "1582-10-15 01:02:03")
    val df = timestamps.toDF("a").selectExpr("cast(a as timestamp) as b")
    val executedPlan1 = df.queryExecution.executedPlan
    val result = hiveResultString(executedPlan1)
    assert(result == timestamps)
    val executedPlan2 = df.selectExpr("array(b)").queryExecution.executedPlan
    val result2 = hiveResultString(executedPlan2)
    assert(result2 == timestamps.map(x => s"[$x]"))
  }

  test("SPARK-57257: nanosecond timestamp formatting in hive result") {
    // Each input fraction maps to the expected rendered fraction at precision 7, 8, 9. Sub-`p`
    // digits are floored and trailing zeros trimmed, so an all-zero fraction renders as no
    // fraction at all (e.g. ".000000001" at p=7/8). The flooring/trimming is independent of the
    // epoch sign, so the pre-1970 base (negative epoch micros + positive nanosWithinMicro)
    // shares the same expected fractions.
    val bases = Seq("2020-01-01 00:00:00", "1960-01-01 00:00:00")
    val cases = Seq(
      ".123456789" -> Seq(".1234567", ".12345678", ".123456789"),
      ".999999999" -> Seq(".9999999", ".99999999", ".999999999"),
      ".999999000" -> Seq(".999999", ".999999", ".999999"),
      ".000000001" -> Seq("", "", ".000000001"),
      ".000000999" -> Seq(".0000009", ".00000099", ".000000999"))
    // Render LTZ in a fixed zone so the wall-clock fields round-trip from the cast.
    withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      Seq(7, 8, 9).zipWithIndex.foreach { case (p, idx) =>
        bases.foreach { base =>
          cases.foreach { case (frac, expectedByPrecision) =>
            val input = base + frac
            val expected = base + expectedByPrecision(idx)
            Seq("timestamp_ltz", "timestamp_ntz").foreach { typeName =>
              val df = spark.sql(s"SELECT CAST('$input' AS $typeName($p)) AS b")
              assert(hiveResultString(df.queryExecution.executedPlan) === Seq(expected),
                s"type = $typeName($p), input = $input")
              val nested = spark.sql(s"SELECT array(CAST('$input' AS $typeName($p))) AS b")
              assert(hiveResultString(nested.queryExecution.executedPlan) === Seq(s"[$expected]"),
                s"nested type = $typeName($p), input = $input")
            }
          }
        }
      }

      // NULL values: handled by the generic `(null, _)` branch in `toHiveString` (before the
      // type-specific cases), so the path is type-agnostic. Verify top-level and nested NULLs.
      Seq("timestamp_ltz(9)", "timestamp_ntz(9)").foreach { typeName =>
        val nullCast = s"CAST(NULL AS $typeName)"
        val topLevel = spark.sql(s"SELECT $nullCast AS b")
        assert(hiveResultString(topLevel.queryExecution.executedPlan) === Seq("NULL"),
          s"top-level NULL of $typeName")
        val inArray = spark.sql(s"SELECT array($nullCast) AS b")
        assert(hiveResultString(inArray.queryExecution.executedPlan) === Seq("[null]"),
          s"array NULL of $typeName")
        val inMap = spark.sql(s"SELECT map('k', $nullCast) AS b")
        assert(hiveResultString(inMap.queryExecution.executedPlan) === Seq("{\"k\":null}"),
          s"map NULL of $typeName")
        val inStruct = spark.sql(s"SELECT named_struct('f', $nullCast) AS b")
        assert(hiveResultString(inStruct.queryExecution.executedPlan) === Seq("{\"f\":null}"),
          s"struct NULL of $typeName")
      }
    }
  }

  test("SPARK-57257: LTZ nanos timestamp honors session time zone, NTZ is zone-independent") {
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      // A fixed instant and the matching local date-time at UTC.
      val ldt = LocalDateTime.of(2020, 1, 1, 12, 0, 0, 123456789)
      val instant = ldt.toInstant(ZoneOffset.UTC)
      val ltzDf = spark.createDataFrame(
        Seq(Row(instant)).asJava,
        StructType(Seq(StructField("b", TimestampLTZNanosType(9)))))
      val ntzDf = spark.createDataFrame(
        Seq(Row(ldt)).asJava,
        StructType(Seq(StructField("b", TimestampNTZNanosType(9)))))

      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        assert(hiveResultString(ltzDf.queryExecution.executedPlan) ===
          Seq("2020-01-01 12:00:00.123456789"))
        assert(hiveResultString(ntzDf.queryExecution.executedPlan) ===
          Seq("2020-01-01 12:00:00.123456789"))
      }
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
        // LTZ shifts with the session zone (UTC-08:00 on this date) ...
        assert(hiveResultString(ltzDf.queryExecution.executedPlan) ===
          Seq("2020-01-01 04:00:00.123456789"))
        // ... while NTZ stays the same wall-clock value.
        assert(hiveResultString(ntzDf.queryExecution.executedPlan) ===
          Seq("2020-01-01 12:00:00.123456789"))
      }
    }
  }

  test("toHiveString correctly handles UDTs") {
    val point = new ExamplePoint(50.0, 50.0)
    val tpe = new ExamplePointUDT()
    assert(toHiveString((point, tpe), false, getTimeFormatters, getBinaryFormatter) ===
      "(50.0, 50.0)")
  }

  test("decimal formatting in hive result") {
    val df = Seq(new java.math.BigDecimal("1")).toDS()
    Seq(2, 6, 18).foreach { scala =>
      val executedPlan =
        df.selectExpr(s"CAST(value AS decimal(38, $scala))").queryExecution.executedPlan
      val result = hiveResultString(executedPlan)
      assert(result.head.split("\\.").last.length === scala)
    }

    val executedPlan = Seq(java.math.BigDecimal.ZERO).toDS()
      .selectExpr(s"CAST(value AS decimal(38, 8))").queryExecution.executedPlan
    val result = hiveResultString(executedPlan)
    assert(result.head === "0.00000000")
  }

  test("SHOW TABLES in hive result") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      Seq(("testcat.ns", "tbl", "foo"), ("spark_catalog.default", "tbl", "csv")).foreach {
        case (ns, tbl, source) =>
          withTable(s"$ns.$tbl") {
            spark.sql(s"CREATE TABLE $ns.$tbl (id bigint) USING $source")
            val df = spark.sql(s"SHOW TABLES FROM $ns")
            val executedPlan = df.queryExecution.executedPlan
            assert(hiveResultString(executedPlan).head == tbl)
          }
      }
    }
  }

  test("DESCRIBE TABLE in hive result") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      Seq(("testcat.ns", "tbl", "foo"), ("spark_catalog.default", "tbl", "csv")).foreach {
        case (ns, tbl, source) =>
          withTable(s"$ns.$tbl") {
            spark.sql(s"CREATE TABLE $ns.$tbl (id bigint COMMENT 'col1') USING $source")
            val df = spark.sql(s"DESCRIBE $ns.$tbl")
            val executedPlan = df.queryExecution.executedPlan
            val expected = "id                  " +
              "\tbigint              " +
              "\tcol1                "
            assert(hiveResultString(executedPlan).head == expected)
          }
      }
    }
  }

  test("SPARK-34984, SPARK-35016: year-month interval formatting in hive result") {
    val df = Seq(Period.ofYears(-10).minusMonths(1)).toDF("i")
    val plan1 = df.queryExecution.executedPlan
    assert(hiveResultString(plan1) === Seq("-10-1"))
    val plan2 = df.selectExpr("array(i)").queryExecution.executedPlan
    assert(hiveResultString(plan2) === Seq("[-10-1]"))
  }

  test("SPARK-49208: negative month intervals") {
    Seq(
      "0-0" -> (11, YM.YEAR, YM.YEAR),
      "0-0" -> (-11, YM.YEAR, YM.YEAR),
      "0-11" -> (11, YM.YEAR, YM.MONTH),
      "-0-11" -> (-11, YM.YEAR, YM.MONTH),
      "0-11" -> (11, YM.MONTH, YM.MONTH),
      "-0-11" -> (-11, YM.MONTH, YM.MONTH),
      "1-0" -> (12, YM.YEAR, YM.YEAR),
      "-1-0" -> (-12, YM.YEAR, YM.YEAR),
      "1-0" -> (12, YM.YEAR, YM.MONTH),
      "-1-0" -> (-12, YM.YEAR, YM.MONTH),
      "1-0" -> (12, YM.MONTH, YM.MONTH),
      "-1-0" -> (-12, YM.MONTH, YM.MONTH),
      "1-0" -> (13, YM.YEAR, YM.YEAR),
      "-1-0" -> (-13, YM.YEAR, YM.YEAR),
      "1-1" -> (13, YM.YEAR, YM.MONTH),
      "-1-1" -> (-13, YM.YEAR, YM.MONTH),
      "1-1" -> (13, YM.MONTH, YM.MONTH),
      "-1-1" -> (-13, YM.MONTH, YM.MONTH)
    ).foreach { case (hiveString, (months, startField, endField)) =>
      assert(toHiveString((Period.ofMonths(months), YearMonthIntervalType(startField, endField)),
        false,
        getTimeFormatters,
        getBinaryFormatter) === hiveString)
    }
  }

  test("SPARK-34984, SPARK-35016: day-time interval formatting in hive result") {
    val df = Seq(Duration.ofDays(5).plusMillis(10)).toDF("i")
    val plan1 = df.queryExecution.executedPlan
    assert(hiveResultString(plan1) === Seq("5 00:00:00.010000000"))
    val plan2 = df.selectExpr("array(i)").queryExecution.executedPlan
    assert(hiveResultString(plan2) === Seq("[5 00:00:00.010000000]"))
  }

  test("geometry formatting in hive result") {
    // Geometry with no SRID.
    val df = spark.sql(
      "SELECT ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040')")
    val result = hiveResultString(df.queryExecution.executedPlan)
    assert(result === Seq("POINT(1 2)"))
    // Geometry with SRID 0.
    val dfSrid0 = spark.sql(
      "SELECT ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040', 0)")
    val resultSrid0 = hiveResultString(dfSrid0.queryExecution.executedPlan)
    assert(resultSrid0 === Seq("POINT(1 2)"))
    // Geometry with SRID 4326.
    val dfSrid4326 = spark.sql(
      "SELECT ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040', 4326)")
    val resultSrid4326 = hiveResultString(dfSrid4326.queryExecution.executedPlan)
    assert(resultSrid4326 === Seq("SRID=4326;POINT(1 2)"))
  }

  test("nested geometry formatting in hive result") {
    // Geometry with no SRID.
    val df = spark.sql(
      "SELECT array(ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040'))")
    val result = hiveResultString(df.queryExecution.executedPlan)
    assert(result === Seq("[\"POINT(1 2)\"]"))
    // Geometry with SRID 4326.
    val dfSrid4326 = spark.sql(
      "SELECT array(ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040', 4326))")
    val resultSrid4326 = hiveResultString(dfSrid4326.queryExecution.executedPlan)
    assert(resultSrid4326 === Seq("[\"SRID=4326;POINT(1 2)\"]"))
  }

  test("geography formatting in hive result") {
    val df = spark.sql(
      "SELECT ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040')")
    val result = hiveResultString(df.queryExecution.executedPlan)
    assert(result === Seq("SRID=4326;POINT(1 2)"))
  }

  test("nested geography formatting in hive result") {
    val df = spark.sql(
      "SELECT array(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040'))")
    val result = hiveResultString(df.queryExecution.executedPlan)
    assert(result === Seq("[\"SRID=4326;POINT(1 2)\"]"))
  }

  test("SPARK-52650: Use stringifyValue to get UDT string representation") {
    val year = Year.of(18)
    val tpe = new YearUDT()
    assert(toHiveString((year, tpe),
      nested = false, getTimeFormatters, getBinaryFormatter) === "18")
    val tpe2 = new YearUDT() {
      override def stringifyValue(obj: Any): String = {
        f"${obj.asInstanceOf[Year].getValue}%04d"
      }
    }
    assert(toHiveString((year, tpe2),
      nested = false, getTimeFormatters, getBinaryFormatter) === "0018")
  }
}
