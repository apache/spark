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
import java.time.{Duration, LocalDateTime, Period}
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.{DAY, HOUR, MINUTE, SECOND}
import org.apache.spark.sql.types.YearMonthIntervalType.{MONTH, YEAR}

class XmlFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._


  test("from_xml") {
    val df = Seq("""<ROW><a>1</a></ROW>""").toDS()
    val schema = new StructType().add("a", IntegerType)

    checkAnswer(
      df.select(from_xml($"value", schema)),
      Row(Row(1)) :: Nil)
  }

  test("from_xml with option (timestampFormat)") {
    val df = Seq("""<ROW><time>26/08/2015 18:00</time></ROW>""").toDS()
    val schema = new StructType().add("time", TimestampType)
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm").asJava

    checkAnswer(
      df.select(from_xml($"value", schema, options)),
      Row(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0"))))
  }

  test("from_xml with option (rowTag)") {
    val df = Seq("""<foo><a>1</a></foo>""").toDS()
    val schema = new StructType().add("a", IntegerType)
    val options = Map("rowTag" -> "foo").asJava

    checkAnswer(
      df.select(from_xml($"value", schema)),
      Row(Row(1)) :: Nil)
  }

  test("from_xml with option (dateFormat)") {
    val df = Seq("""<ROW><time>26/08/2015</time></ROW>""").toDS()
    val schema = new StructType().add("time", DateType)
    val options = Map("dateFormat" -> "dd/MM/yyyy").asJava

    checkAnswer(
      df.select(from_xml($"value", schema, options)),
      Row(Row(java.sql.Date.valueOf("2015-08-26"))))
  }

  test("from_xml missing columns") {
    val df = Seq("""<ROW><a>1</a></ROW>""").toDS()
    val schema = new StructType().add("b", IntegerType)

    checkAnswer(
      df.select(from_xml($"value", schema)),
      Row(Row(null)) :: Nil)
  }

  test("from_xml invalid xml") {
    val df = Seq("""<ROW><a>1</ROW>""").toDS()
    val schema = new StructType().add("a", IntegerType)

    checkAnswer(
      df.select(from_xml($"value", schema)),
      Row(Row(null)) :: Nil)
  }

  test("from_xml - xml doesn't conform to the array type") {
    val df = Seq("""<ROW><a>1</ROW>""").toDS()
    val schema = StructType(StructField("a", ArrayType(IntegerType)) :: Nil)

    checkAnswer(df.select(from_xml($"value", schema)), Row(Row(null))) // yhosny: strange result
  }

  test("from_xml array support") {
    val df = Seq(s"""<ROW> <a>1</a> <a>2</a> </ROW>""".stripMargin).toDS()
    val schema = StructType(StructField("a", ArrayType(IntegerType)) :: Nil)

    checkAnswer(
      df.select(from_xml($"value", schema)),
      Row(Row(Array(1, 2))))
  }

  test("from_xml uses DDL strings for defining a schema - java") {
    val df = Seq("""<ROW> <a>1</a> <b>haa</b> </ROW>""").toDS()
    checkAnswer(
      df.select(from_xml($"value", "a INT, b STRING", new java.util.HashMap[String, String]())),
      Row(Row(1, "haa")) :: Nil)
  }

  test("to_xml - struct") {
    val schema = StructType(StructField("a", IntegerType, nullable = false) :: Nil)
    val data = Seq(Row(1))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .withColumn("a", struct($"a"))

    val expected =
      s"""|<ROW>
          |    <a>1</a>
          |</ROW>""".stripMargin
    checkAnswer(
      df.select(to_xml($"a")),
      Row(expected) :: Nil)
  }

  test("to_xml with option (timestampFormat)") {
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm")
    val schema = StructType(StructField("a", TimestampType, nullable = false) :: Nil)
    val data = Seq(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0")))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .withColumn("a", struct($"a"))

    val expected =
      s"""|<ROW>
          |    <a>26/08/2015 18:00</a>
          |</ROW>""".stripMargin
    checkAnswer(
      df.select(to_xml($"a", options.asJava)),
      Row(expected) :: Nil)
  }

  test("to_xml with option (dateFormat)") {
    val options = Map("dateFormat" -> "dd/MM/yyyy")
    val schema = StructType(StructField("a", DateType, nullable = false) :: Nil)
    val data = Seq(Row(java.sql.Date.valueOf("2015-08-26")))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .withColumn("a", struct($"a"))

    val expected =
      s"""|<ROW>
          |    <a>26/08/2015</a>
          |</ROW>""".stripMargin
    checkAnswer(
      df.select(to_xml($"a", options.asJava)),
      Row(expected) :: Nil)
  }

  test("roundtrip in to_xml and from_xml - struct") {
    val schemaOne = StructType(StructField("a", IntegerType, nullable = false) :: Nil)
    val dataOne = Seq(Row(1, 2, 3))
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(dataOne), schemaOne)
      .withColumn("a", struct($"a"))
    val readBackOne = df1.select(to_xml($"a").as("xml"))
      .select(from_xml($"xml", schemaOne).as("a"))
    checkAnswer(df1, readBackOne)

    val xml =
      s"""|<ROW>
          |    <a>1</a>
          |</ROW>""".stripMargin
    val schemaTwo = new StructType().add("a", IntegerType)
    val dfTwo = Seq(Some(xml), None).toDF("xml")
    val readBackTwo = dfTwo.select(from_xml($"xml", schemaTwo).as("struct"))
      .select(to_xml($"struct").as("xml"))
    checkAnswer(dfTwo, readBackTwo)
  }

  test("roundtrip in to_xml and from_xml - array") {
    val schemaOne = StructType(StructField("a", ArrayType(IntegerType), nullable = false) :: Nil)
    val dataOne = Seq(Row(Array(1, 2, 3)))
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(dataOne), schemaOne)
      .withColumn("a", struct($"a"))
    val readBackOne = df1.select(to_xml($"a").as("xml"))
      .select(from_xml($"xml", schemaOne).as("a"))
    checkAnswer(df1, readBackOne)

    val xml =
      s"""|<ROW>
          |    <a>1</a>
          |    <a>2</a>
          |</ROW>""".stripMargin
    val schemaTwo = new StructType().add("a", ArrayType(IntegerType))
    val dfTwo = Seq(Some(xml), None).toDF("xml")
    val readBackTwo = dfTwo.select(from_xml($"xml", schemaTwo).as("struct"))
      .select(to_xml($"struct").as("xml"))
    checkAnswer(dfTwo, readBackTwo)
  }

  test("Support to_xml in SQL") {
    val schemaOne = StructType(StructField("a", IntegerType, nullable = false) :: Nil)
    val dataOne = Seq(Row(1))
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(dataOne), schemaOne)
      .withColumn("a", struct($"a"))
    val xml =
      s"""|<ROW>
          |    <a>1</a>
          |</ROW>""".stripMargin
    checkAnswer (
      df1.selectExpr("to_xml(a)"),
      Row(xml) :: Nil)

    val xml2 =
      s"""|<ROW>
          |    <a>26/08/2015 18:00</a>
          |</ROW>""".stripMargin
    val schema2 = StructType(StructField("a", TimestampType, nullable = false) :: Nil)
    val dataTwo = Seq(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0")))
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(dataTwo), schema2)
      .withColumn("a", struct($"a"))
    checkAnswer(
      df2.selectExpr("to_xml(a, map('timestampFormat', 'dd/MM/yyyy HH:mm'))"),
      Row(xml2) :: Nil)

    checkError(
      exception = intercept[AnalysisException] {
        df2.selectExpr("to_xml(a, named_struct('a', 1))")
      },
      errorClass = "INVALID_OPTIONS.NON_MAP_FUNCTION",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "to_xml(a, named_struct('a', 1))",
        start = 0,
        stop = 30
      )
    )

    checkError(
      exception = intercept[AnalysisException] {
        df2.selectExpr("to_xml(a, map('a', 1))")
      },
      errorClass = "INVALID_OPTIONS.NON_STRING_TYPE",
      parameters = Map("mapType" -> "\"MAP<STRING, INT>\""),
      context = ExpectedContext(
        fragment = "to_xml(a, map('a', 1))",
        start = 0,
        stop = 21
      )
    )
  }

  test("Support from_xml in SQL") {
    val xml =
      s"""|<ROW>
          |    <a>1</a>
          |</ROW>""".stripMargin
    val df1 = Seq(xml).toDS()
    checkAnswer(
      df1.selectExpr("from_xml(value, 'a INT')"),
      Row(Row(1)) :: Nil)

    val xml2 =
      s"""|<ROW>
          |    <c0>a</c0>
          |    <c1>1</c1>
          |    <c2>
          |        <c20>
          |            3.8
          |        </c20>
          |        <c21>
          |            8
          |        </c21>
          |    </c2>
          |</ROW>""".stripMargin
    val df2 = Seq(xml2).toDS()
    checkAnswer(
      df2.selectExpr("from_xml(value, 'c0 STRING, c1 INT, c2 STRUCT<c20: DOUBLE, c21: INT>')"),
      Row(Row("a", 1, Row(3.8, 8))) :: Nil)

    val xml3 =
      s"""|<ROW>
          |    <time>26/08/2015 18:00</time>
          |</ROW>""".stripMargin
    val df3 = Seq(xml3).toDS()
    checkAnswer(
      df3.selectExpr(
        "from_xml(value, 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy HH:mm'))"),
      Row(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0"))))

    checkError(
      exception = intercept[AnalysisException] {
        df3.selectExpr("from_xml(value, 1)")
      },
      errorClass = "INVALID_SCHEMA.NON_STRING_LITERAL",
      parameters = Map("inputSchema" -> "\"1\""),
      context = ExpectedContext(
        fragment = "from_xml(value, 1)",
        start = 0,
        stop = 17
      )
    )
    checkError(
      exception = intercept[AnalysisException] {
        df3.selectExpr("""from_xml(value, 'time InvalidType')""")
      },
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map(
        "error" -> "'InvalidType'",
        "hint" -> ": extra input 'InvalidType'"
      ),
      context = ExpectedContext(
        fragment = "from_xml(value, 'time InvalidType')",
        start = 0,
        stop = 34
      )
    )
    checkError(
      exception = intercept[AnalysisException] {
        df3.selectExpr("from_xml(value, 'time Timestamp', named_struct('a', 1))")
      },
      errorClass = "INVALID_OPTIONS.NON_MAP_FUNCTION",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "from_xml(value, 'time Timestamp', named_struct('a', 1))",
        start = 0,
        stop = 54
      )
    )
    checkError(
      exception = intercept[AnalysisException] {
        df3.selectExpr("from_xml(value, 'time Timestamp', map('a', 1))")
      },
      errorClass = "INVALID_OPTIONS.NON_STRING_TYPE",
      parameters = Map("mapType" -> "\"MAP<STRING, INT>\""),
      context = ExpectedContext(
        fragment = "from_xml(value, 'time Timestamp', map('a', 1))",
        start = 0,
        stop = 45
      )
    )
  }

  test("SPARK-24709: infers schemas of json strings and pass them to from_json") {
    val xml =
      s"""|<ROW>
          |    <a>1</a>
          |    <a>2</a>
          |    <a>3</a>
          |</ROW>""".stripMargin
    val in = Seq(xml).toDS()
    val out = in.select(from_xml($"value", schema_of_xml(xml)) as "parsed")
    val expected = StructType(StructField(
      "parsed",
      StructType(StructField(
        "a",
        ArrayType(LongType, true), true) :: Nil),
      true) :: Nil)

    assert(out.schema == expected)
  }

  test("infers schemas using options") {
    val df = spark.range(1)
      .select(schema_of_xml(lit("<ROW><a>1</a></ROW>"),
              Map("allowUnquotedFieldNames" -> "true").asJava))
    checkAnswer(df, Seq(Row("STRUCT<a: BIGINT>")))
  }

  test("from_json invalid json - check modes") {
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      val schema = new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("_unparsed", StringType)
      val badRec = """{"a" 1, "b": 11}"""
      val df = Seq(badRec, """{"a": 2, "b": 12}""").toDS()

      checkAnswer(
        df.select(from_json($"value", schema, Map("mode" -> "PERMISSIVE"))),
        Row(Row(null, null, badRec)) :: Row(Row(2, 12, null)) :: Nil)

      checkError(
        exception = intercept[SparkException] {
          df.select(from_json($"value", schema, Map("mode" -> "FAILFAST"))).collect()
        },
        errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
        parameters = Map(
          "badRecord" -> "[null,null,{\"a\" 1, \"b\": 11}]",
          "failFastMode" -> "FAILFAST")
      )

      val exception2 = intercept[AnalysisException] {
        df.select(from_json($"value", schema, Map("mode" -> "DROPMALFORMED")))
          .collect()
      }.getMessage
      assert(exception2.contains(
        "from_json() doesn't support the DROPMALFORMED mode. " +
          "Acceptable modes are PERMISSIVE and FAILFAST."))
    }
  }

  test("corrupt record column in the middle") {
    val schema = new StructType()
      .add("a", IntegerType)
      .add("_unparsed", StringType)
      .add("b", IntegerType)
    val badRec = """<row><a>1</a><b>2</row>"""
    val df = Seq(badRec, """<row><a>1</a><b>12</b></row>""").toDS()

    checkAnswer(
      df.select(from_xml($"value", schema, Map("columnNameOfCorruptRecord" -> "_unparsed").asJava)),
      Row(Row(null, badRec, null)) :: Row(Row(1, null, null)) :: Nil)
  }

  test("parse timestamps with locale") {
    Seq("en-US", "ko-KR", "zh-CN", "ru-RU").foreach { langTag =>
      val locale = Locale.forLanguageTag(langTag)
      val ts = new SimpleDateFormat("dd/MM/yyyy HH:mm").parse("06/11/2018 18:00")
      val timestampFormat = "dd MMM yyyy HH:mm"
      val sdf = new SimpleDateFormat(timestampFormat, locale)
      val input = Seq(s"""<row><time>${sdf.format(ts)}</time></row>""").toDS()
      val options = Map("timestampFormat" -> timestampFormat, "locale" -> langTag).asJava
      val df = input.select(from_xml($"value", "time timestamp", options))

      checkAnswer(df, Row(Row(java.sql.Timestamp.valueOf("2018-11-06 18:00:00.0"))))
    }
  }

  test("from_json - timestamp in micros") {
    val df = Seq("""<row><time>1970-01-01T00:00:00.123456</time></row>""").toDS()
    val schema = new StructType().add("time", TimestampType)
    val options = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.123456"))))
  }

  test("to_json - timestamp in micros") {
    val s = "2019-11-18 11:56:00.123456"
    val df = Seq(java.sql.Timestamp.valueOf(s)).toDF("t").select(
      to_json(struct($"t"), Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSSSS")))
    checkAnswer(df, Row(s"""{"t":"$s"}"""))
  }

  test("json_tuple - do not truncate results") {
    Seq(2000, 2800, 8000 - 1, 8000, 8000 + 1, 65535).foreach { len =>
      val str = Array.tabulate(len)(_ => "a").mkString
      val json_tuple_result = Seq(s"""{"test":"$str"}""").toDF("json")
        .withColumn("result", json_tuple($"json", "test"))
        .select($"result")
        .as[String].head().length
      assert(json_tuple_result === len)
    }
  }

  test("support foldable schema by from_json") {
    val options = Map[String, String]().asJava
    val schema = regexp_replace(lit("dpt_org_id INT, dpt_org_city STRING"), "dpt_org_", "")
    checkAnswer(
      Seq("""{"id":1,"city":"Moscow"}""").toDS().select(from_json($"value", schema, options)),
      Row(Row(1, "Moscow")))

    checkError(
      exception = intercept[AnalysisException] {
        Seq(("""{"i":1}""", "i int")).toDF("json", "schema")
          .select(from_json($"json", $"schema", options)).collect()
      },
      errorClass = "INVALID_SCHEMA.NON_STRING_LITERAL",
      parameters = Map("inputSchema" -> "\"schema\""),
      context = ExpectedContext(fragment = "from_json", getCurrentClassCallSitePattern)
    )
  }

  test("schema_of_json - infers the schema of foldable JSON string") {
    val input = regexp_replace(lit("""{"item_id": 1, "item_price": 0.1}"""), "item_", "")
    checkAnswer(
      spark.range(1).select(schema_of_json(input)),
      Seq(Row("STRUCT<id: BIGINT, price: DOUBLE>")))
  }

  test("SPARK-31065: schema_of_json - null and empty strings as strings") {
    Seq("""{"id": null}""", """{"id": ""}""").foreach { input =>
      checkAnswer(
        spark.range(1).select(schema_of_json(input)),
        Seq(Row("STRUCT<id: STRING>")))
    }
  }

  test("SPARK-31065: schema_of_json - 'dropFieldIfAllNull' option") {
    val options = Map("dropFieldIfAllNull" -> "true")
    // Structs
    checkAnswer(
      spark.range(1).select(
        schema_of_json(
          lit("""{"id": "a", "drop": {"drop": null}}"""),
          options.asJava)),
      Seq(Row("STRUCT<id: STRING>")))

    // Array of structs
    checkAnswer(
      spark.range(1).select(
        schema_of_json(
          lit("""[{"id": "a", "drop": {"drop": null}}]"""),
          options.asJava)),
      Seq(Row("ARRAY<STRUCT<id: STRING>>")))

    // Other types are not affected.
    checkAnswer(
      spark.range(1).select(
        schema_of_json(
          lit("""null"""),
          options.asJava)),
      Seq(Row("STRING")))
  }

  test("optional datetime parser does not affect json time formatting") {
    val s = "2015-08-26 12:34:46"
    def toDF(p: String): DataFrame = sql(
      s"""
         |SELECT
         | to_json(
         |   named_struct('time', timestamp'$s'), map('timestampFormat', "$p")
         | )
         | """.stripMargin)
    checkAnswer(toDF("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), toDF("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"))
  }

  test("SPARK-33134: return partial results only for root JSON objects") {
    val st = new StructType()
      .add("c1", LongType)
      .add("c2", ArrayType(new StructType().add("c3", LongType).add("c4", StringType)))
    val df1 = Seq("""{"c2": [19], "c1": 123456}""").toDF("c0")
    checkAnswer(df1.select(from_json($"c0", st)), Row(Row(123456, null)))

    val df2 = Seq("""{"data": {"c2": [19], "c1": 123456}}""").toDF("c0")
    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "true") {
      checkAnswer(
        df2.select(from_json($"c0", new StructType().add("data", st))),
        Row(Row(Row(123456, null)))
      )
    }
    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "false") {
      checkAnswer(df2.select(from_json($"c0", new StructType().add("data", st))), Row(Row(null)))
    }

    val df3 = Seq("""[{"c2": [19], "c1": 123456}]""").toDF("c0")
    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "true") {
      checkAnswer(df3.select(from_json($"c0", ArrayType(st))), Row(Array(Row(123456, null))))
    }
    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "false") {
      checkAnswer(df3.select(from_json($"c0", ArrayType(st))), Row(null))
    }

    val df4 = Seq("""{"c2": [19]}""").toDF("c0")
    checkAnswer(df4.select(from_json($"c0", MapType(StringType, st))), Row(null))
  }

  test("SPARK-40646: return partial results for JSON arrays with objects") {
    val st = new StructType()
      .add("c1", StringType)
      .add("c2", ArrayType(new StructType().add("a", LongType)))

    // "c2" is expected to be an array of structs but it is a struct in the data.
    val df = Seq("""[{"c2": {"a": 1}, "c1": "abc"}]""").toDF("c0")

    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "true") {
      checkAnswer(
        df.select(from_json($"c0", ArrayType(st))),
        Row(Array(Row("abc", null)))
      )
    }

    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "false") {
      checkAnswer(
        df.select(from_json($"c0", ArrayType(st))),
        Row(null)
      )
    }
  }

  test("SPARK-40646: return partial results for JSON maps") {
    val st = new StructType()
      .add("c1", MapType(StringType, IntegerType))
      .add("c2", StringType)

    // Map "c2" has "k2" key that is a string, not an integer.
    val df = Seq("""{"c1": {"k1": 1, "k2": "A", "k3": 3}, "c2": "abc"}""").toDF("c0")

    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "true") {
      checkAnswer(
        df.select(from_json($"c0", st)),
        Row(Row(null, "abc"))
      )
    }

    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "false") {
      checkAnswer(
        df.select(from_json($"c0", st)),
        Row(Row(null, null))
      )
    }
  }

  test("SPARK-40646: return partial results for JSON arrays") {
    val st = new StructType()
      .add("c", ArrayType(IntegerType))

    // Values in the array are strings instead of integers.
    val df = Seq("""["a", "b", "c"]""").toDF("c0")
    checkAnswer(
      df.select(from_json($"c0", ArrayType(st))),
      Row(null)
    )
  }

  test("SPARK-40646: return partial results for nested JSON arrays") {
    val st = new StructType()
      .add("c", ArrayType(ArrayType(IntegerType)))

    // The second array contains a string instead of an integer.
    val df = Seq("""[[1], ["2"]]""").toDF("c0")
    checkAnswer(
      df.select(from_json($"c0", ArrayType(st))),
      Row(null)
    )
  }

  test("SPARK-40646: return partial results for objects with values as JSON arrays") {
    val st = new StructType()
      .add("c1",
        ArrayType(
          StructType(
            StructField("c2", ArrayType(IntegerType)) ::
            Nil
          )
        )
      )

    // Value "a" cannot be parsed as an integer, c2 value is null.
    val df = Seq("""[{"c1": [{"c2": ["a"]}]}]""").toDF("c0")

    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "true") {
      checkAnswer(
        df.select(from_json($"c0", ArrayType(st))),
        Row(Array(Row(Seq(Row(null)))))
      )
    }

    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "false") {
      checkAnswer(
        df.select(from_json($"c0", ArrayType(st))),
        Row(null)
      )
    }
  }

  test("SPARK-33270: infers schema for JSON field with spaces and pass them to from_json") {
    val in = Seq("""{"a b": 1}""").toDS()
    val out = in.select(from_json($"value", schema_of_json("""{"a b": 100}""")) as "parsed")
    val expected = new StructType().add("parsed", new StructType().add("a b", LongType))
    assert(out.schema == expected)
  }

  test("SPARK-33286: from_json - combined error messages") {
    val df = Seq("""{"a":1}""").toDF("json")
    val invalidJsonSchema = """{"fields": [{"a":123}], "type": "struct"}"""
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        df.select(from_json($"json", invalidJsonSchema, Map.empty[String, String])).collect()
      },
      errorClass = "_LEGACY_ERROR_TEMP_3250",
      parameters = Map("other" -> """{"a":123}"""))

    val invalidDataType = "MAP<INT, cow>"
    val invalidDataTypeReason = "Unrecognized token 'MAP': " +
      "was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')\n " +
      "at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); " +
      "line: 1, column: 4]"
    checkError(
      exception = intercept[AnalysisException] {
        df.select(from_json($"json", invalidDataType, Map.empty[String, String])).collect()
      },
      errorClass = "INVALID_SCHEMA.PARSE_ERROR",
      parameters = Map(
        "inputSchema" -> "\"MAP<INT, cow>\"",
        "reason" -> invalidDataTypeReason
      )
    )

    val invalidTableSchema = "x INT, a cow"
    val invalidTableSchemaReason = "Unrecognized token 'x': " +
      "was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')\n" +
      " at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); " +
      "line: 1, column: 2]"
    checkError(
      exception = intercept[AnalysisException] {
        df.select(from_json($"json", invalidTableSchema, Map.empty[String, String])).collect()
      },
      errorClass = "INVALID_SCHEMA.PARSE_ERROR",
      parameters = Map(
        "inputSchema" -> "\"x INT, a cow\"",
        "reason" -> invalidTableSchemaReason
      )
    )
  }

  test("SPARK-33907: bad json input with json pruning optimization: GetStructField") {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> enabled) {
        val schema = new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
        val badRec = """{"a" 1, "b": 11}"""
        val df = Seq(badRec, """{"a": 2, "b": 12}""").toDS()

        checkError(
          exception = intercept[SparkException] {
            df.select(from_json($"value", schema, Map("mode" -> "FAILFAST"))("b")).collect()
          },
          errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
          parameters = Map(
            "badRecord" -> "[null,null]",
            "failFastMode" -> "FAILFAST")
        )

        checkError(
          exception = intercept[SparkException] {
            df.select(from_json($"value", schema, Map("mode" -> "FAILFAST"))("a")).collect()
          },
          errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
          parameters = Map(
            "badRecord" -> "[null,null]",
            "failFastMode" -> "FAILFAST")
        )
      }
    }
  }

  test("SPARK-33907: bad json input with json pruning optimization: GetArrayStructFields") {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> enabled) {
        val schema = ArrayType(new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))
        val badRec = """{"a" 1, "b": 11}"""
        val df = Seq(s"""[$badRec, {"a": 2, "b": 12}]""").toDS()

        checkError(
          exception = intercept[SparkException] {
            df.select(from_json($"value", schema, Map("mode" -> "FAILFAST"))("b")).collect()
          },
          errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
          parameters = Map(
            "badRecord" -> "[null]",
            "failFastMode" -> "FAILFAST")
        )

        checkError(
          exception = intercept[SparkException] {
            df.select(from_json($"value", schema, Map("mode" -> "FAILFAST"))("a")).collect()
          },
          errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
          parameters = Map(
            "badRecord" -> "[null]",
            "failFastMode" -> "FAILFAST")
        )
      }
    }
  }

  test("SPARK-33907: json pruning optimization with corrupt record field") {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> enabled) {
        val schema = new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
        val badRec = """{"a" 1, "b": 11}"""

        val df = Seq(badRec, """{"a": 2, "b": 12}""").toDS()
          .selectExpr("from_json(value, 'a int, b int, _corrupt_record string') as parsed")
          .selectExpr("parsed._corrupt_record")

        checkAnswer(df, Seq(Row("""{"a" 1, "b": 11}"""), Row(null)))
      }
    }
  }

  test("SPARK-35982: from_json/to_json for map types where value types are year-month intervals") {
    val ymDF = Seq(Period.of(1, 2, 0)).toDF()
    Seq(
      (YearMonthIntervalType(), """{"key":"INTERVAL '1-2' YEAR TO MONTH"}""", Period.of(1, 2, 0)),
      (YearMonthIntervalType(YEAR), """{"key":"INTERVAL '1' YEAR"}""", Period.of(1, 0, 0)),
      (YearMonthIntervalType(MONTH), """{"key":"INTERVAL '14' MONTH"}""", Period.of(1, 2, 0))
    ).foreach { case (toJsonDtype, toJsonExpected, fromJsonExpected) =>
      val toJsonDF = ymDF.select(to_json(map(lit("key"), $"value" cast toJsonDtype)) as "json")
      checkAnswer(toJsonDF, Row(toJsonExpected))

      DataTypeTestUtils.yearMonthIntervalTypes.foreach { fromJsonDtype =>
        val fromJsonDF = toJsonDF
          .select(
            from_json($"json", StructType(StructField("key", fromJsonDtype) :: Nil)) as "value")
          .selectExpr("value['key']")
        if (toJsonDtype == fromJsonDtype) {
          checkAnswer(fromJsonDF, Row(fromJsonExpected))
        } else {
          checkAnswer(fromJsonDF, Row(null))
        }
      }
    }
  }

  test("SPARK-35983: from_json/to_json for map types where value types are day-time intervals") {
    val dtDF = Seq(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)).toDF()
    Seq(
      (DayTimeIntervalType(), """{"key":"INTERVAL '1 02:03:04' DAY TO SECOND"}""",
        Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)),
      (DayTimeIntervalType(DAY, MINUTE), """{"key":"INTERVAL '1 02:03' DAY TO MINUTE"}""",
        Duration.ofDays(1).plusHours(2).plusMinutes(3)),
      (DayTimeIntervalType(DAY, HOUR), """{"key":"INTERVAL '1 02' DAY TO HOUR"}""",
        Duration.ofDays(1).plusHours(2)),
      (DayTimeIntervalType(DAY), """{"key":"INTERVAL '1' DAY"}""",
        Duration.ofDays(1)),
      (DayTimeIntervalType(HOUR, SECOND), """{"key":"INTERVAL '26:03:04' HOUR TO SECOND"}""",
        Duration.ofHours(26).plusMinutes(3).plusSeconds(4)),
      (DayTimeIntervalType(HOUR, MINUTE), """{"key":"INTERVAL '26:03' HOUR TO MINUTE"}""",
        Duration.ofHours(26).plusMinutes(3)),
      (DayTimeIntervalType(HOUR), """{"key":"INTERVAL '26' HOUR"}""",
        Duration.ofHours(26)),
      (DayTimeIntervalType(MINUTE, SECOND), """{"key":"INTERVAL '1563:04' MINUTE TO SECOND"}""",
        Duration.ofMinutes(1563).plusSeconds(4)),
      (DayTimeIntervalType(MINUTE), """{"key":"INTERVAL '1563' MINUTE"}""",
        Duration.ofMinutes(1563)),
      (DayTimeIntervalType(SECOND), """{"key":"INTERVAL '93784' SECOND"}""",
        Duration.ofSeconds(93784))
    ).foreach { case (toJsonDtype, toJsonExpected, fromJsonExpected) =>
      val toJsonDF = dtDF.select(to_json(map(lit("key"), $"value" cast toJsonDtype)) as "json")
      checkAnswer(toJsonDF, Row(toJsonExpected))

      DataTypeTestUtils.dayTimeIntervalTypes.foreach { fromJsonDtype =>
        val fromJsonDF = toJsonDF
          .select(
            from_json($"json", StructType(StructField("key", fromJsonDtype) :: Nil)) as "value")
          .selectExpr("value['key']")
        if (toJsonDtype == fromJsonDtype) {
          checkAnswer(fromJsonDF, Row(fromJsonExpected))
        } else {
          checkAnswer(fromJsonDF, Row(null))
        }
      }
    }
  }

  test("SPARK-36491: Make from_json/to_json to handle timestamp_ntz type properly") {
    val localDT = LocalDateTime.parse("2021-08-12T15:16:23")
    val df = Seq(localDT).toDF()
    val toJsonDF = df.select(to_json(map(lit("key"), $"value")) as "json")
    checkAnswer(toJsonDF, Row("""{"key":"2021-08-12T15:16:23.000"}"""))
    val fromJsonDF = toJsonDF
      .select(
        from_json($"json", StructType(StructField("key", TimestampNTZType) :: Nil)) as "value")
      .selectExpr("value['key']")
    checkAnswer(fromJsonDF, Row(localDT))
  }

}
