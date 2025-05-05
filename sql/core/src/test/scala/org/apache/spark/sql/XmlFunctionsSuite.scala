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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class XmlFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("from_xml") {
    val df = Seq("""<ROW><a>1</a></ROW>""").toDS()
    val schema = new StructType().add("a", IntegerType)

    checkAnswer(
      df.select(from_xml($"value", schema)),
      Row(Row(1)) :: Nil)
  }

  test("SPARK-48300: from_xml - Codegen Support") {
    withTempView("XmlToStructsTable") {
      val dataDF = Seq("""<ROW><a>1</a></ROW>""").toDF("value")
      dataDF.createOrReplaceTempView("XmlToStructsTable")
      val df = sql("SELECT from_xml(value, 'a INT') FROM XmlToStructsTable")
      assert(df.queryExecution.executedPlan.isInstanceOf[WholeStageCodegenExec])
      checkAnswer(df, Row(Row(1)) :: Nil)
    }
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
      df.select(from_xml($"value", schema, options)),
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

    checkAnswer(df.select(from_xml($"value", schema)), Row(Row(null)))
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

  test("SPARK-48363: from_xml with non struct schema") {
    checkError(
      exception = intercept[AnalysisException] {
        Seq("1").toDS().select(from_xml($"value", lit("ARRAY<int>"), Map[String, String]().asJava))
      },
      condition = "DATATYPE_MISMATCH.INVALID_XML_SCHEMA",
      parameters = Map(
        "schema" -> "\"ARRAY<INT>\"",
        "sqlExpr" -> "\"from_xml(value)\""
      ),
      context = ExpectedContext(fragment = "from_xml", getCurrentClassCallSitePattern)
    )

    checkError(
      exception = intercept[AnalysisException] {
        Seq("1").toDF("xml").selectExpr(s"from_xml(xml, 'ARRAY<int>')")
      },
      condition = "DATATYPE_MISMATCH.INVALID_XML_SCHEMA",
      parameters = Map(
        "schema" -> "\"ARRAY<INT>\"",
        "sqlExpr" -> "\"from_xml(xml)\""
      ),
      context = ExpectedContext(
        fragment = "from_xml(xml, 'ARRAY<int>')",
        start = 0,
        stop = 26
      )
    )
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

  test("to_xml ISO default - old dates") {
    withSQLConf("spark.sql.session.timeZone" -> "America/Los_Angeles") {
      val schema = StructType(StructField("a", TimestampType, nullable = false) :: Nil)
      val data = Seq(Row(java.sql.Timestamp.valueOf("1800-01-01 00:00:00.0")))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .withColumn("a", struct($"a"))

      val expected =
        s"""|<ROW>
            |    <a>1800-01-01T00:00:00.000-07:52:58</a>
            |</ROW>""".stripMargin
      checkAnswer(
        df.select(to_xml($"a")),
        Row(expected) :: Nil)
    }
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
      condition = "INVALID_OPTIONS.NON_MAP_FUNCTION",
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
      condition = "INVALID_OPTIONS.NON_STRING_TYPE",
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
      condition = "INVALID_SCHEMA.NON_STRING_LITERAL",
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
      condition = "PARSE_SYNTAX_ERROR",
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
      condition = "INVALID_OPTIONS.NON_MAP_FUNCTION",
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
      condition = "INVALID_OPTIONS.NON_STRING_TYPE",
      parameters = Map("mapType" -> "\"MAP<STRING, INT>\""),
      context = ExpectedContext(
        fragment = "from_xml(value, 'time Timestamp', map('a', 1))",
        start = 0,
        stop = 45
      )
    )
  }

  test("infers schemas of xml strings and pass them to from_xml") {
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

  test("from_xml invalid xml - check modes") {
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      val schema = new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("_unparsed", StringType)
      val badRec = """<ROW><a>1<b><2></b></ROW>"""
      val df = Seq(badRec, """<ROW><a>2</a><b>12</b></ROW>""").toDS()

      checkAnswer(
        df.select(from_xml($"value", schema, Map("mode" -> "PERMISSIVE").asJava)),
        Row(Row(null, null, badRec)) :: Row(Row(2, 12, null)) :: Nil)
    }
  }

  test("SPARK-48296: to_xml - Codegen Support") {
    withTempView("StructsToXmlTable") {
      val schema = StructType(StructField("a", IntegerType, nullable = false) :: Nil)
      val dataDF = spark.createDataFrame(Seq(Row(1)).asJava, schema).withColumn("a", struct($"a"))
      dataDF.createOrReplaceTempView("StructsToXmlTable")
      val df = sql("SELECT to_xml(a) FROM StructsToXmlTable")
      val plan = df.queryExecution.executedPlan
      assert(plan.isInstanceOf[WholeStageCodegenExec])
      val expected =
        s"""|<ROW>
            |    <a>1</a>
            |</ROW>""".stripMargin
      checkAnswer(df, Seq(Row(expected)))
    }
  }

  test("corrupt record column in the middle") {
    val schema = new StructType()
      .add("a", IntegerType)
      .add("_unparsed", StringType)
      .add("b", IntegerType)
    val badRec = """<ROW><a>1</a><b>2</ROW>"""
    val df = Seq(badRec, """<ROW><a>1</a><b>12</b></ROW>""").toDS()

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
      val input = Seq(s"""<ROW><time>${sdf.format(ts)}</time></ROW>""").toDS()
      val options = Map("timestampFormat" -> timestampFormat, "locale" -> langTag).asJava
      val df = input.select(from_xml($"value", "time timestamp", options))

      checkAnswer(df, Row(Row(java.sql.Timestamp.valueOf("2018-11-06 18:00:00.0"))))
    }
  }

  test("from_xml - timestamp in micros") {
    val df = Seq("""<ROW><time>1970-01-01T00:00:00.123456</time></ROW>""").toDS()
    val schema = new StructType().add("time", TimestampType)
    val options = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").asJava

    checkAnswer(
      df.select(from_xml($"value", schema, options)),
      Row(Row(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.123456"))))
  }

  test("to_xml - timestamp in micros") {
    val s = "2019-11-18 11:56:00.123456"
    val xml =
      s"""|<ROW>
          |    <t>$s</t>
          |</ROW>""".stripMargin
    val df = Seq(java.sql.Timestamp.valueOf(s)).toDF("t").select(
      to_xml(struct($"t"), Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSSSS").asJava))
    checkAnswer(df, Row(xml))
  }

  test("support foldable schema by from_xml") {
    val options = Map[String, String]().asJava
    val schema = regexp_replace(lit("dpt_org_id INT, dpt_org_city STRING"), "dpt_org_", "")
    checkAnswer(
      Seq("""<ROW><id>1</id><city>Moscow</city></ROW>""").toDS()
        .select(from_xml($"value", schema, options)),
      Row(Row(1, "Moscow")))

    checkError(
      exception = intercept[AnalysisException] {
        Seq(("""<ROW><i>1</i></ROW>""", "i int")).toDF("xml", "schema")
          .select(from_xml($"xml", $"schema", options)).collect()
      },
      condition = "INVALID_SCHEMA.NON_STRING_LITERAL",
      parameters = Map("inputSchema" -> "\"schema\""),
      context = ExpectedContext(fragment = "from_xml", getCurrentClassCallSitePattern)
    )
  }

  test("schema_of_xml - infers the schema of foldable XML string") {
    val input = regexp_replace(
      lit("""<ROW><item_id>1</item_id><item_price>0.1</item_price></ROW>"""), "item_", "")
    checkAnswer(
      spark.range(1).select(schema_of_xml(input)),
      Seq(Row("STRUCT<id: BIGINT, price: DOUBLE>")))
  }

  test("schema_of_xml - empty string as string") {
    Seq("""<ROW><id></id></ROW>""").foreach { input =>
      checkAnswer(
        spark.range(1).select(schema_of_xml(input)),
        Seq(Row("STRUCT<id: STRING>")))
    }
  }

  test("optional datetime parser does not affect xml time formatting") {
    val s = "2015-08-26 12:34:46"
    def toDF(p: String): DataFrame = sql(
      s"""
         |SELECT
         | to_xml(
         |   named_struct('time', timestamp'$s'), map('timestampFormat', "$p")
         | )
         | """.stripMargin)
    checkAnswer(toDF("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), toDF("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"))
  }
}
