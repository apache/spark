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

import java.text.SimpleDateFormat
import java.time.{Duration, Period}
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.{DAY, HOUR, MINUTE, SECOND}
import org.apache.spark.sql.types.YearMonthIntervalType.{MONTH, YEAR}

class CsvFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("from_csv with empty options") {
    val df = Seq("1").toDS()
    val schema = "a int"

    checkAnswer(
      df.select(from_csv($"value", lit(schema), Map[String, String]().asJava)),
      Row(Row(1)) :: Nil)
  }

  test("from_csv with option") {
    val df = Seq("26/08/2015 18:00").toDS()
    val schema = new StructType().add("time", TimestampType)
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm")

    checkAnswer(
      df.select(from_csv($"value", schema, options)),
      Row(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0"))))
  }

  test("checking the columnNameOfCorruptRecord option") {
    val columnNameOfCorruptRecord = "_unparsed"
    val df = Seq("0,2013-111-11 12:13:14", "1,1983-08-04").toDS()
    val schema = new StructType().add("a", IntegerType).add("b", DateType)
    val schemaWithCorrField1 = schema.add(columnNameOfCorruptRecord, StringType)
    val df2 = df
      .select(from_csv($"value", schemaWithCorrField1, Map(
        "mode" -> "Permissive", "columnNameOfCorruptRecord" -> columnNameOfCorruptRecord)))
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "corrected") {
      checkAnswer(df2, Seq(
        Row(Row(0, null, "0,2013-111-11 12:13:14")),
        Row(Row(1, java.sql.Date.valueOf("1983-08-04"), null))))
    }
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "legacy") {
      checkAnswer(df2, Seq(
        Row(Row(0, java.sql.Date.valueOf("2022-03-11"), null)),
        Row(Row(1, java.sql.Date.valueOf("1983-08-04"), null))))
    }
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "exception") {
      val msg = intercept[SparkException] {
        df2.collect()
      }.getCause.getMessage
      assert(msg.contains("Fail to parse"))
    }
  }

  test("schema_of_csv - infers schemas") {
    checkAnswer(
      spark.range(1).select(schema_of_csv(lit("0.1,1"))),
      Seq(Row("STRUCT<`_c0`: DOUBLE, `_c1`: INT>")))
    checkAnswer(
      spark.range(1).select(schema_of_csv("0.1,1")),
      Seq(Row("STRUCT<`_c0`: DOUBLE, `_c1`: INT>")))
  }

  test("schema_of_csv - infers schemas using options") {
    val df = spark.range(1)
      .select(schema_of_csv(lit("0.1 1"), Map("sep" -> " ").asJava))
    checkAnswer(df, Seq(Row("STRUCT<`_c0`: DOUBLE, `_c1`: INT>")))
  }

  test("to_csv - struct") {
    val df = Seq(Tuple1(Tuple1(1))).toDF("a")

    checkAnswer(df.select(to_csv($"a")), Row("1") :: Nil)
  }

  test("to_csv with option") {
    val df = Seq(Tuple1(Tuple1(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0")))).toDF("a")
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm").asJava

    checkAnswer(df.select(to_csv($"a", options)), Row("26/08/2015 18:00") :: Nil)
  }

  test("from_csv invalid csv - check modes") {
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      val schema = new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("_unparsed", StringType)
      val badRec = "\""
      val df = Seq(badRec, "2,12").toDS()

      checkAnswer(
        df.select(from_csv($"value", schema, Map("mode" -> "PERMISSIVE"))),
        Row(Row(null, null, badRec)) :: Row(Row(2, 12, null)) :: Nil)

      val exception1 = intercept[SparkException] {
        df.select(from_csv($"value", schema, Map("mode" -> "FAILFAST"))).collect()
      }.getMessage
      assert(exception1.contains(
        "Malformed records are detected in record parsing. Parse Mode: FAILFAST."))

      val exception2 = intercept[SparkException] {
        df.select(from_csv($"value", schema, Map("mode" -> "DROPMALFORMED")))
          .collect()
      }.getMessage
      assert(exception2.contains(
        "from_csv() doesn't support the DROPMALFORMED mode. " +
          "Acceptable modes are PERMISSIVE and FAILFAST."))
    }
  }

  test("from_csv uses DDL strings for defining a schema - java") {
    val df = Seq("""1,"haa"""").toDS()
    checkAnswer(
      df.select(
        from_csv($"value", lit("a INT, b STRING"), new java.util.HashMap[String, String]())),
      Row(Row(1, "haa")) :: Nil)
  }

  test("roundtrip to_csv -> from_csv") {
    val df = Seq(Tuple1(Tuple1(1)), Tuple1(null)).toDF("struct")
    val schema = df.schema(0).dataType.asInstanceOf[StructType]
    val options = Map.empty[String, String]
    val readback = df.select(to_csv($"struct").as("csv"))
      .select(from_csv($"csv", schema, options).as("struct"))

    checkAnswer(df, readback)
  }

  test("roundtrip from_csv -> to_csv") {
    val df = Seq(Some("1"), None).toDF("csv")
    val schema = new StructType().add("a", IntegerType)
    val options = Map.empty[String, String]
    val readback = df.select(from_csv($"csv", schema, options).as("struct"))
      .select(to_csv($"struct").as("csv"))

    checkAnswer(df, readback)
  }

  test("infers schemas of a CSV string and pass to to from_csv") {
    val in = Seq("""0.123456789,987654321,"San Francisco"""").toDS()
    val options = Map.empty[String, String].asJava
    val out = in.select(from_csv($"value", schema_of_csv("0.1,1,a"), options) as "parsed")
    val expected = StructType(Seq(StructField(
      "parsed",
      StructType(Seq(
        StructField("_c0", DoubleType, true),
        StructField("_c1", IntegerType, true),
        StructField("_c2", StringType, true))))))

    assert(out.schema == expected)
  }

  test("Support to_csv in SQL") {
    val df1 = Seq(Tuple1(Tuple1(1))).toDF("a")
    checkAnswer(df1.selectExpr("to_csv(a)"), Row("1") :: Nil)
  }

  test("parse timestamps with locale") {
    Seq("en-US", "ko-KR", "zh-CN", "ru-RU").foreach { langTag =>
      val locale = Locale.forLanguageTag(langTag)
      val ts = new SimpleDateFormat("dd/MM/yyyy HH:mm").parse("06/11/2018 18:00")
      val timestampFormat = "dd MMM yyyy HH:mm"
      val sdf = new SimpleDateFormat(timestampFormat, locale)
      val input = Seq(s"""${sdf.format(ts)}""").toDS()
      val options = Map("timestampFormat" -> timestampFormat, "locale" -> langTag)
      val df = input.select(from_csv($"value", lit("time timestamp"), options.asJava))

      checkAnswer(df, Row(Row(java.sql.Timestamp.valueOf("2018-11-06 18:00:00.0"))))
    }
  }

  test("support foldable schema by from_csv") {
    val options = Map[String, String]().asJava
    val schema = concat_ws(",", lit("i int"), lit("s string"))
    checkAnswer(
      Seq("""1,"a"""").toDS().select(from_csv($"value", schema, options)),
      Row(Row(1, "a")))

    val errMsg = intercept[AnalysisException] {
      Seq(("1", "i int")).toDF("csv", "schema")
        .select(from_csv($"csv", $"schema", options)).collect()
    }.getMessage
    assert(errMsg.contains("Schema should be specified in DDL format as a string literal"))

    val errMsg2 = intercept[AnalysisException] {
      Seq("1").toDF("csv").select(from_csv($"csv", lit(1), options)).collect()
    }.getMessage
    assert(errMsg2.contains("The expression '1' is not a valid schema string"))
  }

  test("schema_of_csv - infers the schema of foldable CSV string") {
    val input = concat_ws(",", lit(0.1), lit(1))
    checkAnswer(
      spark.range(1).select(schema_of_csv(input)),
      Seq(Row("STRUCT<`_c0`: DOUBLE, `_c1`: INT>")))
  }

  test("optional datetime parser does not affect csv time formatting") {
    val s = "2015-08-26 12:34:46"
    def toDF(p: String): DataFrame = sql(
      s"""
         |SELECT
         | to_csv(
         |   named_struct('time', timestamp'$s'), map('timestampFormat', "$p")
         | )
         | """.stripMargin)
    checkAnswer(toDF("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), toDF("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"))
  }

  test("SPARK-32968: Pruning csv field should not change result") {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(SQLConf.CSV_EXPRESSION_OPTIMIZATION.key -> enabled) {
        val df1 = sparkContext.parallelize(Seq("a,b")).toDF("csv")
          .selectExpr("from_csv(csv, 'a string, b string', map('mode', 'failfast')) as parsed")
        checkAnswer(df1.selectExpr("parsed.a"), Seq(Row("a")))
        checkAnswer(df1.selectExpr("parsed.b"), Seq(Row("b")))

        val df2 = sparkContext.parallelize(Seq("a,b")).toDF("csv")
          .selectExpr("from_csv(csv, 'a string, b string') as parsed")
        checkAnswer(df2.selectExpr("parsed.a"), Seq(Row("a")))
        checkAnswer(df2.selectExpr("parsed.b"), Seq(Row("b")))
      }
    }
  }

  test("SPARK-32968: bad csv input with csv pruning optimization") {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(SQLConf.CSV_EXPRESSION_OPTIMIZATION.key -> enabled) {
        val df = sparkContext.parallelize(Seq("1,\u0001\u0000\u0001234")).toDF("csv")
          .selectExpr("from_csv(csv, 'a int, b int', map('mode', 'failfast')) as parsed")

        val err1 = intercept[SparkException] {
          df.selectExpr("parsed.a").collect
        }

        val err2 = intercept[SparkException] {
          df.selectExpr("parsed.b").collect
        }

        assert(err1.getMessage.contains("Malformed records are detected in record parsing"))
        assert(err2.getMessage.contains("Malformed records are detected in record parsing"))
      }
    }
  }

  test("SPARK-32968: csv pruning optimization with corrupt record field") {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(SQLConf.CSV_EXPRESSION_OPTIMIZATION.key -> enabled) {
        val df = sparkContext.parallelize(Seq("a,b,c,d")).toDF("csv")
          .selectExpr("from_csv(csv, 'a string, b string, _corrupt_record string') as parsed")
          .selectExpr("parsed._corrupt_record")

        checkAnswer(df, Seq(Row("a,b,c,d")))
      }
    }
  }

  test("SPARK-35998: Make from_csv/to_csv to handle year-month intervals properly") {
    val ymDF = Seq(Period.of(1, 2, 0)).toDF
    Seq(
      (YearMonthIntervalType(), "INTERVAL '1-2' YEAR TO MONTH", Period.of(1, 2, 0)),
      (YearMonthIntervalType(YEAR), "INTERVAL '1' YEAR", Period.of(1, 0, 0)),
      (YearMonthIntervalType(MONTH), "INTERVAL '14' MONTH", Period.of(1, 2, 0))
    ).foreach { case (toCsvDtype, toCsvExpected, fromCsvExpected) =>
      val toCsvDF = ymDF.select(to_csv(struct($"value" cast toCsvDtype)) as "csv")
      checkAnswer(toCsvDF, Row(toCsvExpected))

      DataTypeTestUtils.yearMonthIntervalTypes.foreach { fromCsvDtype =>
        val fromCsvDF = toCsvDF
          .select(
            from_csv(
              $"csv",
              StructType(StructField("a", fromCsvDtype) :: Nil),
              Map.empty[String, String]) as "value")
          .selectExpr("value.a")
        if (toCsvDtype == fromCsvDtype) {
          checkAnswer(fromCsvDF, Row(fromCsvExpected))
        } else {
          checkAnswer(fromCsvDF, Row(null))
        }
      }
    }
  }

  test("SPARK-35999: Make from_csv/to_csv to handle day-time intervals properly") {
    val dtDF = Seq(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)).toDF
    Seq(
      (DayTimeIntervalType(), "INTERVAL '1 02:03:04' DAY TO SECOND",
        Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)),
      (DayTimeIntervalType(DAY, MINUTE), "INTERVAL '1 02:03' DAY TO MINUTE",
        Duration.ofDays(1).plusHours(2).plusMinutes(3)),
      (DayTimeIntervalType(DAY, HOUR), "INTERVAL '1 02' DAY TO HOUR",
        Duration.ofDays(1).plusHours(2)),
      (DayTimeIntervalType(DAY), "INTERVAL '1' DAY",
        Duration.ofDays(1)),
      (DayTimeIntervalType(HOUR, SECOND), "INTERVAL '26:03:04' HOUR TO SECOND",
        Duration.ofHours(26).plusMinutes(3).plusSeconds(4)),
      (DayTimeIntervalType(HOUR, MINUTE), "INTERVAL '26:03' HOUR TO MINUTE",
        Duration.ofHours(26).plusMinutes(3)),
      (DayTimeIntervalType(HOUR), "INTERVAL '26' HOUR",
        Duration.ofHours(26)),
      (DayTimeIntervalType(MINUTE, SECOND), "INTERVAL '1563:04' MINUTE TO SECOND",
        Duration.ofMinutes(1563).plusSeconds(4)),
      (DayTimeIntervalType(MINUTE), "INTERVAL '1563' MINUTE",
        Duration.ofMinutes(1563)),
      (DayTimeIntervalType(SECOND), "INTERVAL '93784' SECOND",
        Duration.ofSeconds(93784))
    ).foreach { case (toCsvDtype, toCsvExpected, fromCsvExpected) =>
      val toCsvDF = dtDF.select(to_csv(struct($"value" cast toCsvDtype)) as "csv")
      checkAnswer(toCsvDF, Row(toCsvExpected))

      DataTypeTestUtils.dayTimeIntervalTypes.foreach { fromCsvDtype =>
        val fromCsvDF = toCsvDF
          .select(
            from_csv(
              $"csv",
              StructType(StructField("a", fromCsvDtype) :: Nil),
              Map.empty[String, String]) as "value")
          .selectExpr("value.a")
        if (toCsvDtype == fromCsvDtype) {
          checkAnswer(fromCsvDF, Row(fromCsvExpected))
        } else {
          checkAnswer(fromCsvDF, Row(null))
        }
      }
    }
  }
}
