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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Random

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.expressions.{CodegenObjectFactoryMode, ExpressionEvalHelper, Literal}
import org.apache.spark.sql.catalyst.expressions.variant.{VariantExpressionEvalUtils, VariantGet}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}
import org.apache.spark.util.ArrayImplicits._

class VariantSuite extends QueryTest with SharedSparkSession with ExpressionEvalHelper {
  import testImplicits._

  test("basic tests") {
    def verifyResult(df: DataFrame): Unit = {
      val result = df.collect()
        .map(_.get(0).asInstanceOf[VariantVal].toString)
        .sorted
        .toSeq
      val expected = (1 until 10).map(id => "1" * id)
      assert(result == expected)
    }

    val query = spark.sql("select parse_json(repeat('1', id)) as v from range(1, 10)")
    verifyResult(query)

    // Write into and read from Parquet.
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      query.write.parquet(tempDir)
      verifyResult(spark.read.parquet(tempDir))
    }
  }

  test("basic try_parse_json alias") {
    val df = spark.createDataFrame(Seq(Row("""{ "a" : 1 }"""), Row("""{ a : 1 }""")).asJava,
      new StructType().add("json", StringType))
    val actual = df.select(to_json(try_parse_json(col("json")))).collect()

    assert(actual(0)(0) == """{"a":1}""")
    assert(actual(1)(0) == null)
  }

  test("basic parse_json alias") {
    val df = spark.createDataFrame(Seq(Row("""{ "a" : 1 }""")).asJava,
      new StructType().add("json", StringType))
    val actual = df.select(
      to_json(parse_json(col("json"))),
      to_json(parse_json(lit("""{"b": [{"c": "str2"}]}""")))).collect().head

    assert(actual.getString(0) == """{"a":1}""")
    assert(actual.getString(1) == """{"b":[{"c":"str2"}]}""")
  }

  test("expression alias") {
    val df = Seq("""{ "a" : 1 }""", """{ "b" : 2 }""").toDF("json")
    val v = parse_json(col("json"))

    def rows(results: Any*): Seq[Row] = results.map(Row(_))

    checkAnswer(df.select(is_variant_null(v)), rows(false, false))
    checkAnswer(df.select(schema_of_variant(v)), rows("STRUCT<a: BIGINT>", "STRUCT<b: BIGINT>"))
    checkAnswer(df.select(schema_of_variant_agg(v)), rows("STRUCT<a: BIGINT, b: BIGINT>"))

    checkAnswer(df.select(variant_get(v, "$.a", "int")), rows(1, null))
    checkAnswer(df.select(variant_get(v, "$.b", "int")), rows(null, 2))
    checkAnswer(df.select(variant_get(v, "$.a", "double")), rows(1.0, null))
    checkError(
      exception = intercept[SparkRuntimeException] {
        df.select(variant_get(v, "$.a", "binary")).collect()
      },
      errorClass = "INVALID_VARIANT_CAST",
      parameters = Map("value" -> "1", "dataType" -> "\"BINARY\"")
    )

    checkAnswer(df.select(try_variant_get(v, "$.a", "int")), rows(1, null))
    checkAnswer(df.select(try_variant_get(v, "$.b", "int")), rows(null, 2))
    checkAnswer(df.select(try_variant_get(v, "$.a", "double")), rows(1.0, null))
    checkAnswer(df.select(try_variant_get(v, "$.a", "binary")), rows(null, null))
  }

  test("round trip tests") {
    val rand = new Random(42)
    val input = Seq.fill(50) {
      if (rand.nextInt(10) == 0) {
        null
      } else {
        val value = new Array[Byte](rand.nextInt(50))
        rand.nextBytes(value)
        val metadata = new Array[Byte](rand.nextInt(50))
        rand.nextBytes(metadata)
        new VariantVal(value, metadata)
      }
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(input.map(Row(_))),
      StructType.fromDDL("v variant")
    )
    val result = df.collect().map(_.get(0).asInstanceOf[VariantVal])

    def prepareAnswer(values: Seq[VariantVal]): Seq[String] = {
      values.map(v => if (v == null) "null" else v.debugString()).sorted
    }
    assert(prepareAnswer(input) == prepareAnswer(result.toImmutableArraySeq))

    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      df.write.parquet(tempDir)
      val readResult = spark.read.parquet(tempDir).collect().map(_.get(0).asInstanceOf[VariantVal])
      assert(prepareAnswer(input) == prepareAnswer(readResult.toImmutableArraySeq))
    }
  }

  test("array of variant") {
    val rand = new Random(42)
    val input = Seq.fill(3) {
      if (rand.nextInt(10) == 0) {
        null
      } else {
        val value = new Array[Byte](rand.nextInt(50))
        rand.nextBytes(value)
        val metadata = new Array[Byte](rand.nextInt(50))
        rand.nextBytes(metadata)
        val numElements = 3 // rand.nextInt(10)
        Seq.fill(numElements)(new VariantVal(value, metadata))
      }
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(input.map { v =>
        Row.fromSeq(Seq(v))
      }),
      StructType.fromDDL("v array<variant>")
    )

    def prepareAnswer(values: Seq[Row]): Seq[String] = {
      values.map(_.get(0)).map { v =>
        if (v == null) {
          "null"
        } else {
          v.asInstanceOf[mutable.ArraySeq[Any]]
           .map(_.asInstanceOf[VariantVal].debugString()).mkString(",")
        }
      }.sorted
    }

    // Test conversion to UnsafeRow in both codegen and interpreted code paths.
    val codegenModes = Seq(CodegenObjectFactoryMode.NO_CODEGEN.toString,
                           CodegenObjectFactoryMode.FALLBACK.toString)
    codegenModes.foreach { codegen =>
      withTempDir { dir =>
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
          val tempDir = new File(dir, "files").getCanonicalPath
          df.write.parquet(tempDir)
          Seq(false, true).foreach { vectorizedReader =>
            withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key ->
                vectorizedReader.toString) {
              val readResult = spark.read.parquet(tempDir).collect().toSeq
              assert(prepareAnswer(df.collect().toSeq) == prepareAnswer(readResult))
            }
          }
        }
      }
    }
  }

  test("write partitioned file") {
    def verifyResult(df: DataFrame): Unit = {
      val result = df.selectExpr("v").collect()
        .map(_.get(0).asInstanceOf[VariantVal].toString)
        .sorted
        .toSeq
      val expected = (1 until 10).map(id => "1" * id)
      assert(result == expected)
    }

    // At this point, JSON parsing logic is not really implemented. We just construct some number
    // inputs that are also valid JSON. This exercises passing VariantVal throughout the system.
    val queryString = "select id, parse_json(repeat('1', id)) as v from range(1, 10)"
    val query = spark.sql(queryString)
    verifyResult(query)

    // Partition by another column should work.
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      query.write.partitionBy("id").parquet(tempDir)
      verifyResult(spark.read.parquet(tempDir))
    }

    // Partitioning by Variant column is not allowed.
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      intercept[AnalysisException] {
        query.write.partitionBy("v").parquet(tempDir)
      }
    }

    // Same as above, using saveAsTable
    withTable("t") {
      query.write.partitionBy("id").saveAsTable("t")
      verifyResult(spark.sql("select * from t"))
    }

    withTable("t") {
      intercept[AnalysisException] {
        query.write.partitionBy("v").saveAsTable("t")
      }
    }

    // Same as above, using SQL CTAS
    withTable("t") {
      spark.sql(s"CREATE TABLE t USING PARQUET PARTITIONED BY (id) AS $queryString")
      verifyResult(spark.sql("select * from t"))
    }

    withTable("t") {
      intercept[AnalysisException] {
        spark.sql(s"CREATE TABLE t USING PARQUET PARTITIONED BY (v) AS $queryString")
      }
    }
  }

  test("SPARK-47546: invalid variant binary") {
    // Write a struct-of-binary that looks like a Variant, but with minor variations that may make
    // it invalid to read.
    // Test cases:
    // 1) A binary that is almost correct, but contains an extra field "paths"
    // 2,3) A binary with incorrect field names
    // 4) Incorrect data typea
    // 5,6) Nullable value or metdata

    // Binary value of empty metadata
    val m = "X'010000'"
    // Binary value of a literal "false"
    val v = "X'8'"
    val cases = Seq(
        s"named_struct('value', $v, 'metadata', $m, 'paths', $v)",
        s"named_struct('value', $v, 'dictionary', $m)",
        s"named_struct('val', $v, 'metadata', $m)",
        s"named_struct('value', 8, 'metadata', $m)",
        s"named_struct('value', cast(null as binary), 'metadata', $m)",
        s"named_struct('value', $v, 'metadata', cast(null as binary))"
    )
    cases.foreach { structDef =>
      Seq(false, true).foreach { vectorizedReader =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key ->
                vectorizedReader.toString) {
          withTempDir { dir =>
            val file = new File(dir, "dir").getCanonicalPath
            val df = spark.sql(s"select $structDef as v from range(10)")
            df.write.parquet(file)
            val schema = StructType(Seq(StructField("v", VariantType)))
            val result = spark.read.schema(schema).parquet(file).selectExpr("to_json(v)")
            val e = intercept[org.apache.spark.SparkException](result.collect())
            assert(e.getCause.isInstanceOf[AnalysisException], e.printStackTrace)
          }
        }
      }
    }
  }

  test("SPARK-47546: valid variant binary") {
    // Test valid struct-of-binary formats. We don't expect anybody to construct a Variant in this
    // way, but it lets us validate slight variations that could be produced by a different writer.

    // Binary value of empty metadata
    val m = "X'010000'"
    // Binary value of a literal "false"
    val v = "X'8'"
    val cases = Seq(
        s"named_struct('value', $v, 'metadata', $m)",
        s"named_struct('metadata', $m, 'value', $v)"
    )
    cases.foreach { structDef =>
      withTempDir { dir =>
        val file = new File(dir, "dir").getCanonicalPath
        val df = spark.sql(s"select $structDef as v from range(10)")
        df.write.parquet(file)
        val schema = StructType(Seq(StructField("v", VariantType)))
        val result = spark.read.schema(schema).parquet(file)
            .selectExpr("to_json(v)")
        checkAnswer(result, Seq.fill(10)(Row("false")))
      }
    }
  }

  test("json option constraints") {
    withTempDir { dir =>
      val file = new File(dir, "file.json")
      Files.write(file.toPath, "0".getBytes(StandardCharsets.UTF_8))

      // Ensure that we get an error when setting the singleVariantColumn JSON option while also
      // specifying a schema.
      checkError(
        exception = intercept[AnalysisException] {
          spark.read.format("json").option("singleVariantColumn", "var").schema("var variant")
        },
        errorClass = "INVALID_SINGLE_VARIANT_COLUMN",
        parameters = Map.empty
      )
      checkError(
        exception = intercept[AnalysisException] {
          spark.read.format("json").option("singleVariantColumn", "another_name")
            .schema("var variant").json(file.getAbsolutePath).collect()
        },
        errorClass = "INVALID_SINGLE_VARIANT_COLUMN",
        parameters = Map.empty
      )
    }
  }

  test("json scan") {
    val content = Seq(
      "true",
      """{"a": [], "b": null}""",
      """{"a": 1}""",
      "[1, 2, 3]"
    ).mkString("\n").getBytes(StandardCharsets.UTF_8)

    withTempDir { dir =>
      val file = new File(dir, "file.json")
      Files.write(file.toPath, content)

      checkAnswer(
        spark.read.format("json").option("singleVariantColumn", "var")
          .load(file.getAbsolutePath)
          .selectExpr("to_json(var)"),
        Seq(Row("true"), Row("""{"a":[],"b":null}"""), Row("""{"a":1}"""), Row("[1,2,3]"))
      )

      checkAnswer(
        spark.read.format("json").schema("a variant, b variant")
          .load(file.getAbsolutePath).selectExpr("to_json(a)", "to_json(b)"),
        Seq(Row(null, null), Row("[]", "null"), Row("1", null), Row(null, null))
      )
    }

    // Test scan with partitions.
    withTempDir { dir =>
      new File(dir, "a=1/b=2/").mkdirs()
      Files.write(new File(dir, "a=1/b=2/file.json").toPath, content)
      checkAnswer(
        spark.read.format("json").option("singleVariantColumn", "var")
          .load(dir.getAbsolutePath).selectExpr("a", "b", "to_json(var)"),
        Seq(Row(1, 2, "true"), Row(1, 2, """{"a":[],"b":null}"""), Row(1, 2, """{"a":1}"""),
          Row(1, 2, "[1,2,3]"))
      )
    }
  }

  test("json scan with map schema") {
    withTempDir { dir =>
      val file = new File(dir, "file.json")
      val content = Seq(
        "true",
        """{"v": null}""",
        """{"v": {"a": 1, "b": null}}"""
      ).mkString("\n").getBytes(StandardCharsets.UTF_8)
      Files.write(file.toPath, content)
      checkAnswer(
        spark.read.format("json").schema("v map<string, variant>")
          .load(file.getAbsolutePath)
          .selectExpr("to_json(v)"),
        Seq(Row(null), Row(null), Row("""{"a":1,"b":null}"""))
      )
    }
  }

  test("group/order/join variant are disabled") {
    var ex = intercept[AnalysisException] {
      spark.sql("select parse_json('') group by 1")
    }
    assert(ex.getErrorClass == "GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE")

    ex = intercept[AnalysisException] {
      spark.sql("select parse_json('') order by 1")
    }
    assert(ex.getErrorClass == "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE")

    ex = intercept[AnalysisException] {
      spark.sql("select parse_json('') sort by 1")
    }
    assert(ex.getErrorClass == "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE")

    ex = intercept[AnalysisException] {
      spark.sql("with t as (select 1 as a, parse_json('') as v) " +
        "select rank() over (partition by a order by v) from t")
    }
    assert(ex.getErrorClass == "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE")

    ex = intercept[AnalysisException] {
      spark.sql("with t as (select parse_json('') as v) " +
        "select t1.v from t as t1 join t as t2 on t1.v = t2.v")
    }
    assert(ex.getErrorClass == "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE")
  }

  test("variant_explode") {
    def check(input: String, expected: Seq[Row]): Unit = {
      withView("v") {
        Seq(input).toDF("json").createOrReplaceTempView("v")
        checkAnswer(sql("select pos, key, to_json(value) from v, " +
          "lateral variant_explode(parse_json(json))"), expected)
        val expectedOuter = if (expected.isEmpty) Seq(Row(null, null, null)) else expected
        checkAnswer(sql("select pos, key, to_json(value) from v, " +
          "lateral variant_explode_outer(parse_json(json))"), expectedOuter)
      }
    }

    Seq("true", "false").foreach { codegenEnabled =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled) {
        check(null, Nil)
        check("1", Nil)
        check("null", Nil)
        check("""{"a": [1, 2, 3], "b": true}""", Seq(Row(0, "a", "[1,2,3]"), Row(1, "b", "true")))
        check("""[null, "hello", {}]""",
          Seq(Row(0, null, "null"), Row(1, null, "\"hello\""), Row(2, null, "{}")))
      }
    }
  }

  test("SPARK-48067: default variant columns works") {
    withTable("t") {
      sql("""create table t(
        v1 variant default null,
        v2 variant default parse_json(null),
        v3 variant default cast(null as variant),
        v4 variant default parse_json('1'),
        v5 variant default parse_json('1'),
        v6 variant default parse_json('{\"k\": \"v\"}'),
        v7 variant default cast(5 as int),
        v8 variant default cast('hello' as string),
        v9 variant default parse_json(to_json(parse_json('{\"k\": \"v\"}')))
      ) using parquet""")
      sql("""insert into t values(DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT,
        DEFAULT, DEFAULT)""")

      val expected = sql("""select
        cast(null as variant) as v1,
        parse_json(null) as v2,
        cast(null as variant) as v3,
        parse_json('1') as v4,
        parse_json('1') as v5,
        parse_json('{\"k\": \"v\"}') as v6,
        cast(cast(5 as int) as variant) as v7,
        cast('hello' as variant) as v8,
        parse_json(to_json(parse_json('{\"k\": \"v\"}'))) as v9
      """)
      val actual = sql("select * from t")
      checkAnswer(actual, expected.collect())
    }
  }

  Seq(
    (
      "basic int parse json",
      VariantExpressionEvalUtils.parseJson(UTF8String.fromString("1")),
      VariantType
    ),
    (
      "basic json parse json",
      VariantExpressionEvalUtils.parseJson(UTF8String.fromString("{\"k\": \"v\"}")),
      VariantType
    ),
    (
      "basic null parse json",
      VariantExpressionEvalUtils.parseJson(UTF8String.fromString("null")),
      VariantType
    ),
    (
      "basic null",
      null,
      VariantType
    ),
    (
      "basic array",
      new GenericArrayData(Array[Int](1, 2, 3, 4, 5)),
      new ArrayType(IntegerType, false)
    ),
    (
      "basic string",
      UTF8String.fromString("literal string"),
      StringType
    ),
    (
      "basic timestamp",
      0L,
      TimestampType
    ),
    (
      "basic int",
      0,
      IntegerType
    ),
    (
      "basic struct",
      Literal.default(new StructType().add("col0", StringType)).eval(),
      new StructType().add("col0", StringType)
    ),
    (
      "complex struct with child variant",
      Literal.default(new StructType()
        .add("col0", StringType)
        .add("col1", new StructType().add("col0", VariantType))
        .add("col2", VariantType)
        .add("col3", new ArrayType(VariantType, false))
      ).eval(),
      new StructType()
        .add("col0", StringType)
        .add("col1", new StructType().add("col0", VariantType))
        .add("col2", VariantType)
        .add("col3", new ArrayType(VariantType, false))
    ),
    (
      "basic array with null",
      new GenericArrayData(Array[Any](1, 2, null)),
      new ArrayType(IntegerType, true)
    ),
    (
      "basic map with null",
      new ArrayBasedMapData(
        new GenericArrayData(Array[Any](UTF8String.fromString("k1"), UTF8String.fromString("k2"))),
        new GenericArrayData(Array[Any](1, null))
      ),
      new MapType(StringType, IntegerType, true)
    )
  ).foreach { case (testName, value, dt) =>
    test(s"SPARK-48067: Variant literal `sql` correctly recreates the variant - $testName") {
      val l = Literal.create(
        VariantExpressionEvalUtils.castToVariant(value, dt.asInstanceOf[DataType]), VariantType)
      val jsonString = l.eval().asInstanceOf[VariantVal]
        .toJson(DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
      val expectedSql = s"PARSE_JSON('$jsonString')"
      assert(l.sql == expectedSql)
      val valueFromLiteralSql =
        spark.sql(s"select ${l.sql}").collect()(0).getAs[VariantVal](0)

      // Cast the variants to their specified type to compare for logical equality.
      // Currently, variant equality naively compares its value and metadata binaries. However,
      // variant equality is more complex than this.
      val castVariantExpr = VariantGet(
        l,
        Literal.create(UTF8String.fromString("$"), StringType),
        dt,
        true,
        Some(DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone).toString())
      )
      val sqlVariantExpr = VariantGet(
        Literal.create(valueFromLiteralSql, VariantType),
        Literal.create(UTF8String.fromString("$"), StringType),
        dt,
        true,
        Some(DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone).toString())
      )
      checkEvaluation(castVariantExpr, sqlVariantExpr.eval())
    }
  }
}
