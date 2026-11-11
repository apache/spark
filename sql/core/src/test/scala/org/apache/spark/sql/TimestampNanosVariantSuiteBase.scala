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
 * Nanosecond timestamps and VARIANT. Spark's variant has no nanosecond primitive, so writing a
 * `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` value into a variant fails at analysis, while reading a
 * variant as one works at precision `p`. The subclasses run every test with ANSI mode on and off.
 */
abstract class TimestampNanosVariantSuiteBase extends SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  // Non-foldable NTZ and LTZ columns of precision `p`.
  private def nanosDF(p: Int): DataFrame = {
    val schema = new StructType()
      .add("ntz", TimestampNTZNanosType(p))
      .add("ltz", TimestampLTZNanosType(p))
    val data = Seq(Row(
      LocalDateTime.parse("2020-01-01T00:00:00.123456789"),
      Instant.parse("2020-01-01T08:00:00.123456789Z")))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  private def checkWriteError(
      df: => DataFrame,
      srcType: String,
      targetType: String = "VARIANT"): Unit = {
    val e = intercept[AnalysisException](df)
    assert(e.getCondition == "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION")
    assert(e.getMessageParameters.get("srcType") == s""""$srcType"""")
    assert(e.getMessageParameters.get("targetType") == s""""$targetType"""")
  }

  test("writing a nanosecond timestamp into a VARIANT fails at analysis") {
    checkWriteError(
      sql("SELECT CAST(TIMESTAMP_NTZ '2020-01-01 00:00:00.123456789' AS VARIANT)"),
      "TIMESTAMP_NTZ(9)")
    checkWriteError(
      sql("SELECT CAST(TIMESTAMP_LTZ '2020-01-01 00:00:00.1234567 UTC' AS VARIANT)"),
      "TIMESTAMP_LTZ(7)")
    Seq(7, 8, 9).foreach { p =>
      val df = nanosDF(p)
      Seq("ntz" -> s"TIMESTAMP_NTZ($p)", "ltz" -> s"TIMESTAMP_LTZ($p)").foreach { case (c, t) =>
        Seq(
          s"CAST($c AS VARIANT)" -> t,
          s"try_cast($c AS VARIANT)" -> t,
          s"CAST(array(array($c)) AS VARIANT)" -> s"ARRAY<ARRAY<$t>>",
          s"to_variant_object(named_struct('t', $c))" -> s"STRUCT<t: $t>",
          s"to_variant_object(map('k', array(named_struct('t', $c))))" ->
            s"MAP<STRING, ARRAY<STRUCT<t: $t>>>",
          s"variant_from_arrays(array('k'), array($c))" -> t,
          s"variant_from_entries(array(named_struct('k', 'k', 'v', $c)))" -> t,
          s"variant_insert(parse_json('{}'), '$$.a', $c)" -> t,
          s"try_variant_insert(parse_json('{}'), '$$.a', $c)" -> t,
          s"variant_set(parse_json('{}'), '$$.a', $c)" -> t,
          s"try_variant_set(parse_json('{}'), '$$.a', $c)" -> t,
          s"variant_array_append(parse_json('[]'), '$$', $c)" -> t,
          s"try_variant_array_append(parse_json('[]'), '$$', $c)" -> t
        ).foreach { case (expr, srcType) =>
          checkWriteError(df.selectExpr(expr), srcType)
        }
        checkWriteError(
          df.selectExpr(s"CAST(named_struct('t', $c) AS STRUCT<t: VARIANT>)"),
          s"STRUCT<t: $t>", "STRUCT<t: VARIANT>")
        checkWriteError(df.select(col(c).cast(VariantType)), t)
        checkWriteError(df.select(col(c).try_cast(VariantType)), t)
        checkWriteError(df.select(to_variant_object(struct(col(c)))), s"STRUCT<$c: $t>")
      }
    }
  }

  test("inserting a nanosecond timestamp into a VARIANT column fails at analysis") {
    withTable("t") {
      withTempView("src") {
        sql("CREATE TABLE t (v VARIANT) USING parquet")
        nanosDF(9).createOrReplaceTempView("src")
        Seq(
          "ANSI" -> "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          "STRICT" -> "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          "LEGACY" -> "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION"
        ).foreach { case (policy, condition) =>
          withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy) {
            Seq("ntz" -> "TIMESTAMP_NTZ(9)", "ltz" -> "TIMESTAMP_LTZ(9)").foreach { case (c, t) =>
              val e = intercept[AnalysisException](sql(s"INSERT INTO t SELECT $c FROM src"))
              assert(e.getCondition == condition, policy)
              assert(e.getMessageParameters.get("srcType") == s""""$t"""", policy)
            }
          }
        }
        checkAnswer(spark.table("t"), Seq.empty)
      }
    }
  }

  test("TIME cannot be written to a VARIANT either") {
    checkWriteError(sql("SELECT CAST(CAST('12:34:56' AS TIME(6)) AS VARIANT)"), "TIME(6)")
    checkWriteError(
      sql("SELECT to_variant_object(array(CAST('12:34:56' AS TIME(6))))"), "ARRAY<TIME(6)>")
    checkAnswer(
      sql("""SELECT CAST(variant_get(parse_json('"12:34:56.123456"'), '$', 'TIME(6)')
            |  AS STRING)""".stripMargin),
      Row("12:34:56.123456"))
  }

  test("reading a nanosecond timestamp from a VARIANT at the target precision") {
    val v = """parse_json('{"t": "2020-01-01 00:00:00.123456789",
      |"a": ["2020-01-01 00:00:00.123456789"]}')""".stripMargin
    Seq(7 -> "1234567", 8 -> "12345678", 9 -> "123456789").foreach { case (p, fraction) =>
      Seq(s"TIMESTAMP_NTZ($p)" -> s"timestamp_ntz($p)",
          s"TIMESTAMP_LTZ($p)" -> s"timestamp_ltz($p)").foreach { case (t, typeName) =>
        val expected = s"2020-01-01 00:00:00.$fraction"
        checkAnswer(
          sql(s"""SELECT CAST(variant_get($v, '$$.t', '$t') AS STRING),
                 |  CAST(try_variant_get($v, '$$.t', '$t') AS STRING),
                 |  CAST(CAST(variant_get($v, '$$.t') AS $t) AS STRING),
                 |  CAST(variant_get($v, '$$.a', 'ARRAY<$t>')[0] AS STRING),
                 |  typeof(variant_get($v, '$$.t', '$t'))""".stripMargin),
          Row(expected, expected, expected, expected, typeName))
        checkAnswer(
          spark.range(1).select(
            variant_get(parse_json(lit("\"2020-01-01 00:00:00.123456789\"")), "$", t)
              .cast(StringType)),
          Row(expected))
      }
      // A microsecond variant primitive widens without loss.
      checkAnswer(
        sql(s"""SELECT CAST(variant_get(CAST(TIMESTAMP_NTZ '2020-01-01 00:00:00.123456' AS
               |  VARIANT), '$$', 'TIMESTAMP_NTZ($p)') AS STRING),
               |  CAST(variant_get(CAST(TIMESTAMP_LTZ '2020-01-01 08:00:00.123456 UTC' AS
               |  VARIANT), '$$', 'TIMESTAMP_LTZ($p)') AS STRING)""".stripMargin),
        Row("2020-01-01 00:00:00.123456", "2020-01-01 00:00:00.123456"))
    }
  }
}

class TimestampNanosVariantAnsiOnSuite extends TimestampNanosVariantSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

class TimestampNanosVariantAnsiOffSuite extends TimestampNanosVariantSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
