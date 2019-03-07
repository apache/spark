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
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class CsvFunctionsSuite extends QueryTest with SharedSQLContext {
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

    checkAnswer(df2, Seq(
      Row(Row(0, null, "0,2013-111-11 12:13:14")),
      Row(Row(1, java.sql.Date.valueOf("1983-08-04"), null))))
  }

  test("schema_of_csv - infers schemas") {
    checkAnswer(
      spark.range(1).select(schema_of_csv(lit("0.1,1"))),
      Seq(Row("struct<_c0:double,_c1:int>")))
    checkAnswer(
      spark.range(1).select(schema_of_csv("0.1,1")),
      Seq(Row("struct<_c0:double,_c1:int>")))
  }

  test("schema_of_csv - infers schemas using options") {
    val df = spark.range(1)
      .select(schema_of_csv(lit("0.1 1"), Map("sep" -> " ").asJava))
    checkAnswer(df, Seq(Row("struct<_c0:double,_c1:int>")))
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
    val out = in.select(from_csv('value, schema_of_csv("0.1,1,a"), options) as "parsed")
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
}
