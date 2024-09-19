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

import org.apache.spark.sql.QueryTest.sameRows
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.expressions.variant.{ToVariantObject, VariantExpressionEvalUtils}
import org.apache.spark.sql.execution.{FileSourceScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarArray
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.VariantVal

class VariantEndToEndSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("parse_json/to_json round-trip") {
    def check(input: String, output: String = null): Unit = {
      val df = Seq(input).toDF("v")
      val variantDF = df.select(to_json(parse_json(col("v"))))
      val expected = if (output != null) output else input
      checkAnswer(variantDF, Seq(Row(expected)))
    }

    check("null")
    check("true")
    check("false")
    check("-1")
    check("1.0E10")
    check("\"\"")
    check("\"" + ("a" * 63) + "\"")
    check("\"" + ("b" * 64) + "\"")
    // scalastyle:off nonascii
    check("\"" + ("你好，世界" * 20) + "\"")
    // scalastyle:on nonascii
    check("[]")
    check("{}")
    // scalastyle:off nonascii
    check(
      "[null, true,   false,-1, 1e10, \"\\uD83D\\uDE05\", [ ], { } ]",
      "[null,true,false,-1,1.0E10,\"😅\",[],{}]"
    )
    // scalastyle:on nonascii
    check("[0.0, 1.00, 1.10, 1.23]", "[0,1,1.1,1.23]")
    withSQLConf(SQLConf.VARIANT_ALLOW_DUPLICATE_KEYS.key -> "true") {
      check(
        """{"c": [], "b": 0, "a": null, "a": {"x": 0, "x": 1}, "b": 1, "b": 2, "c": [3]}""",
        """{"a":{"x":1},"b":2,"c":[3]}"""
      )
    }
  }

  test("from_json/to_json round-trip") {
    def check(input: String, output: String = null): Unit = {
      val df = Seq(input).toDF("v")
      val variantDF = df.select(to_json(from_json(col("v"), VariantType)))
      val expected = if (output != null) output else input
      checkAnswer(variantDF, Seq(Row(expected)))
    }

    check("null")
    check("true")
    check("false")
    check("-1")
    check("1.0E10")
    check("\"\"")
    check("\"" + ("a" * 63) + "\"")
    check("\"" + ("b" * 64) + "\"")
    // scalastyle:off nonascii
    check("\"" + ("你好，世界" * 20) + "\"")
    // scalastyle:on nonascii
    check("[]")
    check("{}")
    // scalastyle:off nonascii
    check(
      "[null, true,   false,-1, 1e10, \"\\uD83D\\uDE05\", [ ], { } ]",
      "[null,true,false,-1,1.0E10,\"😅\",[],{}]"
    )
    // scalastyle:on nonascii
    check("[0.0, 1.00, 1.10, 1.23]", "[0,1,1.1,1.23]")
  }

  test("try_parse_json/to_json round-trip") {
    def check(input: String, output: String = "INPUT IS OUTPUT"): Unit = {
      val df = Seq(input).toDF("v")
      val variantDF = df.selectExpr("to_json(try_parse_json(v)) as v").select(Column("v"))
      val expected = if (output != "INPUT IS OUTPUT") output else input
      checkAnswer(variantDF, Seq(Row(expected)))
    }

    check("null")
    check("true")
    check("false")
    check("-1")
    check("1.0E10")
    check("\"\"")
    check("\"" + ("a" * 63) + "\"")
    check("\"" + ("b" * 64) + "\"")
    // scalastyle:off nonascii
    check("\"" + ("你好，世界" * 20) + "\"")
    // scalastyle:on nonascii
    check("[]")
    check("{}")
    // scalastyle:off nonascii
    check(
      "[null, true,   false,-1, 1e10, \"\\uD83D\\uDE05\", [ ], { } ]",
      "[null,true,false,-1,1.0E10,\"😅\",[],{}]"
    )
    // scalastyle:on nonascii
    check("[0.0, 1.00, 1.10, 1.23]", "[0,1,1.1,1.23]")
    // Places where parse_json should fail and therefore, try_parse_json should return null
    check("{1:2}", null)
    check("{\"a\":1", null)
    check("{\"a\":[a,b,c]}", null)
    check("\"" + "a" * (16 * 1024 * 1024) + "\"", null)
  }

  test("to_json with nested variant") {
    val df = Seq(1).toDF("v")
    val variantDF1 = df.select(to_json(array(
      parse_json(lit("{}")),
      parse_json(lit("\"\"")),
      parse_json(lit("[1, 2, 3]")))))
    checkAnswer(variantDF1, Seq(Row("[{},\"\",[1,2,3]]")))

    val variantDF2 = df.select(
      to_json(named_struct(
        lit("a"), parse_json(lit("""{ "x": 1, "y": null, "z": "str" }""")),
        lit("b"), parse_json(lit("[[]]")),
        lit("c"), parse_json(lit("false"))
      )))
    checkAnswer(variantDF2, Seq(Row("""{"a":{"x":1,"y":null,"z":"str"},"b":[[]],"c":false}""")))
  }

  test("parse_json - Codegen Support") {
    val df = Seq(("1", """{"a": 1}""")).toDF("key", "v").toDF()
    val variantDF = df.select(parse_json(col("v")))
    val plan = variantDF.queryExecution.executedPlan
    assert(plan.isInstanceOf[WholeStageCodegenExec])
    val v = VariantBuilder.parseJson("""{"a":1}""", false)
    val expected = new VariantVal(v.getValue, v.getMetadata)
    checkAnswer(variantDF, Seq(Row(expected)))
  }

  test("to_variant_object - Codegen Support") {
    Seq("CODEGEN_ONLY", "NO_CODEGEN").foreach { codegenMode =>
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) {
        val schema = StructType(Array(
          StructField("v", StructType(Array(StructField("a", IntegerType))))
        ))
        val data = Seq(Row(Row(1)), Row(Row(2)), Row(Row(3)), Row(null))
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        val variantDF = df.select(to_variant_object(col("v")))
        val plan = variantDF.queryExecution.executedPlan
        assert(plan.isInstanceOf[WholeStageCodegenExec] == (codegenMode == "CODEGEN_ONLY"))
        val v1 = VariantExpressionEvalUtils.castToVariant(InternalRow(1),
          StructType(Array(StructField("a", IntegerType))))
        val v2 = VariantExpressionEvalUtils.castToVariant(InternalRow(2),
          StructType(Array(StructField("a", IntegerType))))
        val v3 = VariantExpressionEvalUtils.castToVariant(InternalRow(3),
          StructType(Array(StructField("a", IntegerType))))
        val v4 = VariantExpressionEvalUtils.castToVariant(null,
          StructType(Array(StructField("a", IntegerType))))
        val expected = Seq(Row(new VariantVal(v1.getValue, v1.getMetadata)),
          Row(new VariantVal(v2.getValue, v2.getMetadata)),
          Row(new VariantVal(v3.getValue, v3.getMetadata)),
          Row(new VariantVal(v4.getValue, v4.getMetadata)))
        sameRows(variantDF.collect().toSeq, expected)
      }
    }
  }

  test("schema_of_variant") {
    def check(json: String, expected: String): Unit = {
      val df = Seq(json).toDF("j").selectExpr("schema_of_variant(parse_json(j))")
      checkAnswer(df, Seq(Row(expected)))
    }

    check("null", "VOID")
    check("1", "BIGINT")
    check("1.0", "DECIMAL(1,0)")
    check("0.01", "DECIMAL(2,2)")
    check("1.00", "DECIMAL(1,0)")
    check("10.00", "DECIMAL(2,0)")
    check("10.10", "DECIMAL(3,1)")
    check("0.0", "DECIMAL(1,0)")
    check("-0.0", "DECIMAL(1,0)")
    check("2147483647.999", "DECIMAL(13,3)")
    check("9223372036854775808", "DECIMAL(19,0)")
    check("-9223372036854775808.0", "DECIMAL(19,0)")
    check("9999999999999999999.9999999999999999999", "DECIMAL(38,19)")
    check("9999999999999999999.99999999999999999999", "DOUBLE")
    check("1E0", "DOUBLE")
    check("true", "BOOLEAN")
    check("\"2000-01-01\"", "STRING")
    check("""{"a":0}""", "OBJECT<a: BIGINT>")
    check("""{"b": {"c": "c"}, "a":["a"]}""", "OBJECT<a: ARRAY<STRING>, b: OBJECT<c: STRING>>")
    check("[]", "ARRAY<VOID>")
    check("[false]", "ARRAY<BOOLEAN>")
    check("[null, 1, 1.0]", "ARRAY<DECIMAL(20,0)>")
    check("[null, 1, 1.1]", "ARRAY<DECIMAL(21,1)>")
    check("[123456.789, 123.456789]", "ARRAY<DECIMAL(12,6)>")
    check("[1, 11111111111111111111111111111111111111]", "ARRAY<DECIMAL(38,0)>")
    check("[1.1, 11111111111111111111111111111111111111]", "ARRAY<DOUBLE>")
    check("[1, \"1\"]", "ARRAY<VARIANT>")
    check("[{}, true]", "ARRAY<VARIANT>")
    check("""[{"c": ""}, {"a": null}, {"b": 1}]""", "ARRAY<OBJECT<a: VOID, b: BIGINT, c: STRING>>")
    check("""[{"a": ""}, {"a": null}, {"b": 1}]""", "ARRAY<OBJECT<a: STRING, b: BIGINT>>")
    check(
      """[{"a": 1, "b": null}, {"b": true, "a": 1E0}]""",
      "ARRAY<OBJECT<a: DOUBLE, b: BOOLEAN>>"
    )
  }

  test("from_json variant data type parsing") {
    def check(variantTypeString: String): Unit = {
      val df = Seq("{\"a\": 1, \"b\": [2, 3.1]}").toDF("j").selectExpr("variant_get(from_json(j,\""
        + variantTypeString + "\"),\"$.b[0]\")::int")
      checkAnswer(df, Seq(Row(2)))
    }

    check("variant")
    check("     \t variant ")
    check("  \n  VaRiaNt  ")
  }

  test("is_variant_null with parse_json and variant_get") {
    def check(json: String, path: String, expected: Boolean): Unit = {
      val df = Seq(json).toDF("j").selectExpr(s"is_variant_null(variant_get(parse_json(j),"
        + s"\"${path}\"))")
      checkAnswer(df, Seq(Row(expected)))
    }

    check("{ \"a\": null }", "$.a", expected = true)
    check("{ \"a\": null }", "$.b", expected = false)
    check("{ \"a\": null, \"b\": \"null\" }", "$.b", expected = false)
    check("{ \"a\": null, \"b\": {\"c\": null} }", "$.b.c", expected = true)
    check("{ \"a\": null, \"b\": {\"c\": null, \"d\": [13, null]} }", "$.b.d", expected = false)
    check("{ \"a\": null, \"b\": {\"c\": null, \"d\": [13, null]} }", "$.b.d[0]", expected = false)
    check("{ \"a\": null, \"b\": {\"c\": null, \"d\": [13, null]} }", "$.b.d[1]", expected = true)
    check("{ \"a\": null, \"b\": {\"c\": null, \"d\": [13, null]} }", "$.b.d[2]", expected = false)
  }

  test("schema_of_variant_agg") {
    // Literal input.
    checkAnswer(
      sql("""SELECT schema_of_variant_agg(parse_json('{"a": [1, 2, 3]}'))"""),
      Seq(Row("OBJECT<a: ARRAY<BIGINT>>")))

    // Non-grouping aggregation.
    def checkNonGrouping(input: Seq[String], expected: String): Unit = {
      checkAnswer(input.toDF("json").selectExpr("schema_of_variant_agg(parse_json(json))"),
        Seq(Row(expected)))
    }

    checkNonGrouping(Seq("""{"a": [1, 2, 3]}"""), "OBJECT<a: ARRAY<BIGINT>>")
    checkNonGrouping((0 to 100).map(i => s"""{"a": [$i]}"""), "OBJECT<a: ARRAY<BIGINT>>")
    checkNonGrouping(Seq("""[{"a": 1}, {"b": 2}]"""), "ARRAY<OBJECT<a: BIGINT, b: BIGINT>>")
    checkNonGrouping(Seq("""{"a": [1, 2, 3]}""", """{"a": "banana"}"""), "OBJECT<a: VARIANT>")
    checkNonGrouping(Seq("""{"a": "banana"}""", """{"b": "apple"}"""),
      "OBJECT<a: STRING, b: STRING>")
    checkNonGrouping(Seq("""{"a": "data"}""", null), "OBJECT<a: STRING>")
    checkNonGrouping(Seq(null, null), "VOID")
    checkNonGrouping(Seq("""{"a": null}""", """{"a": null}"""), "OBJECT<a: VOID>")
    checkNonGrouping(Seq(
      """{"hi":[]}""",
      """{"hi":[{},{}]}""",
      """{"hi":[{"it's":[{"me":[{"a": 1}]}]}]}"""),
      "OBJECT<hi: ARRAY<OBJECT<`it's`: ARRAY<OBJECT<me: ARRAY<OBJECT<a: BIGINT>>>>>>>")

    // Grouping aggregation.
    withView("v") {
      (0 to 100).map { id =>
        val json = if (id % 4 == 0) s"""{"a": [$id]}""" else s"""{"a": ["$id"]}"""
        (id, json)
      }.toDF("id", "json").createTempView("v")
      checkAnswer(sql("select schema_of_variant_agg(parse_json(json)) from v group by id % 2"),
        Seq(Row("OBJECT<a: ARRAY<STRING>>"), Row("OBJECT<a: ARRAY<VARIANT>>")))
      checkAnswer(sql("select schema_of_variant_agg(parse_json(json)) from v group by id % 3"),
        Seq.fill(3)(Row("OBJECT<a: ARRAY<VARIANT>>")))
      checkAnswer(sql("select schema_of_variant_agg(parse_json(json)) from v group by id % 4"),
        Seq.fill(3)(Row("OBJECT<a: ARRAY<STRING>>")) ++ Seq(Row("OBJECT<a: ARRAY<BIGINT>>")))
    }
  }

  test("cast to variant with ColumnarArray input") {
    val dataVector = new OnHeapColumnVector(4, LongType)
    dataVector.appendNull()
    dataVector.appendLong(123)
    dataVector.appendNull()
    dataVector.appendLong(456)
    val array = new ColumnarArray(dataVector, 0, 4)
    val variant = Cast(Literal(array, ArrayType(LongType)), VariantType).eval()
    val variant2 = ToVariantObject(Literal(array, ArrayType(LongType))).eval()
    assert(variant.toString == "[null,123,null,456]")
    assert(variant2.toString == "[null,123,null,456]")
    dataVector.close()
  }

  test("cast to variant/to_variant_object with scan input") {
    Seq("NO_CODEGEN", "CODEGEN_ONLY").foreach { codegenMode =>
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) {
        withTempPath { dir =>
          val path = dir.getAbsolutePath
          val input = Seq(
              Row(Array(1, null), Map("k1" -> null, "k2" -> false), Row(null, "str")),
              Row(null, null, null)
            )
          val schema = StructType.fromDDL(
            "a array<int>, m map<string, boolean>, s struct<f1 string, f2 string>")
          spark.createDataFrame(spark.sparkContext.parallelize(input), schema).write.parquet(path)
          val df = spark.read.parquet(path).selectExpr(
            s"cast(cast(a as variant) as ${schema(0).dataType.sql})",
            s"cast(to_variant_object(m) as ${schema(1).dataType.sql})",
            s"cast(to_variant_object(s) as ${schema(2).dataType.sql})")
          checkAnswer(df, input)
          val plan = df.queryExecution.executedPlan
          assert(plan.isInstanceOf[WholeStageCodegenExec] == (codegenMode == "CODEGEN_ONLY"))
        }
      }
    }
  }

  test("json scan metrics") {
    withTempDir { dir =>
      val fileTemp = new File(dir, "file.json.gz")
      val file = fileTemp.getAbsolutePath
      val input = Seq(
        Row(
          """[{"a": 1, "g": 432, "b": {"c": {"d": 32}, "e": [891, {"f": 21}]}}""" +
          """,{"a": {"h":52}, "g": 971, "b": {"c": [{"d": 432}], """ +
          """"e": [{"h":173},[{"f": 65}], 8]}}]"""
        ),
        Row(
          """{"a": {"h":453}, "g": 121, "b": {"c": [{"d": 27}], "e": [{"h":64}, """ +
          """[{"f": 27}], 43]}}"""
        )
      )
      // Repartitioning to make sure that there are multiple workers and their stats are combined
      // correctly.
      spark
        .createDataFrame(spark.sparkContext.parallelize(input, 1), StructType.fromDDL("j string"))
        .repartition(2)
        .write
        .option("compression", "gzip")
        .text(file)
      // Single Variant Column
      val singleColumnDf = spark.read.format("json").option("singleVariantColumn", "var").load(file)

      // Make sure that multiple tasks are used and each worker reads at least one row.
      assert(singleColumnDf.rdd.getNumPartitions == 2)
      singleColumnDf.rdd.mapPartitionsWithIndex((index, iter) => Iterator((index, iter.size)))
        .collect()
        .foreach { case (_, size) => assert(size > 0) }

      val singleColumnResults = singleColumnDf.collect()
      val singleColumnJsonScanNode = findNode(singleColumnDf.queryExecution.executedPlan)(
        _.isInstanceOf[FileSourceScanExec]
      ).get
      assert(singleColumnJsonScanNode.metrics("variantBuilderTopLevelNumVariants").value == 2)
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedNumVariants").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderTopLevelMaxDepth").value == 6)
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedMaxDepth").value == 0)
      // Byte Size of Variants
      val byteSizeActual = singleColumnResults
        .map(_(0).asInstanceOf[VariantVal])
        .map(v => v.getValue.length + v.getMetadata.length)
        .sum
      assert(
        singleColumnJsonScanNode
          .metrics("variantBuilderTopLevelByteSizeBound")
          .value == byteSizeActual
      )
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedByteSizeBound").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderTopLevelNumScalars").value == 17)
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedNumScalars").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderTopLevelNumPaths").value == 41)
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedNumPaths").value == 0)

      // Schema with top level and nested variants
      val multipleColumnDf =
        spark.read
          .format("json")
          .schema("a variant, g int, b struct<c:variant,e:array<variant>>")
          .load(file)
      val multipleColumnResults = multipleColumnDf.collect()
      val multipleColumnJsonScanNode = findNode(multipleColumnDf.queryExecution.executedPlan)(
        _.isInstanceOf[FileSourceScanExec]
      ).get

      // Make sure that multiple tasks are used and each worker reads at least one row.
      assert(multipleColumnDf.rdd.getNumPartitions == 2)
      multipleColumnDf.rdd.mapPartitionsWithIndex((index, iter) => Iterator((index, iter.size)))
        .collect()
        .foreach { case (_, size) => assert(size > 0) }

      // Number of Variants Produced
      assert(multipleColumnJsonScanNode.metrics("variantBuilderTopLevelNumVariants").value == 3)
      assert(multipleColumnJsonScanNode.metrics("variantBuilderNestedNumVariants").value == 11)
      // Max Depth of Variants
      assert(multipleColumnJsonScanNode.metrics("variantBuilderTopLevelMaxDepth").value == 1)
      assert(multipleColumnJsonScanNode.metrics("variantBuilderNestedMaxDepth").value == 2)
      // Byte Size of Variants
      val topLevelByteSizeActual = multipleColumnResults
        .map(_(0).asInstanceOf[VariantVal])
        .filter(_ != null)
        .map(v => v.getValue.length + v.getMetadata.length)
        .sum
      assert(
        multipleColumnJsonScanNode
          .metrics("variantBuilderTopLevelByteSizeBound")
          .value == topLevelByteSizeActual
      )
      val nestedByteSizeActual = multipleColumnDf.filter("g == 432").selectExpr("b.c")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 432")
          .selectExpr("b.e[0]")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 432")
          .selectExpr("b.e[1]")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 121")
          .selectExpr("b.c")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 121")
          .selectExpr("b.e[0]")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 121")
          .selectExpr("b.e[1]")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 121")
          .selectExpr("b.e[2]")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 971")
          .selectExpr("b.c")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 971")
          .selectExpr("b.e[0]")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 971")
          .selectExpr("b.e[1]")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum + multipleColumnDf
          .filter("g == 971")
          .selectExpr("b.e[2]")
          .collect()
          .map(_(0).asInstanceOf[VariantVal])
          .filter(_ != null)
          .map(v => v.getValue.length + v.getMetadata.length)
          .sum
      assert(
        multipleColumnJsonScanNode
          .metrics("variantBuilderNestedByteSizeBound")
          .value == nestedByteSizeActual
      )

      // Number of scalar values in all variants
      assert(multipleColumnJsonScanNode.metrics("variantBuilderTopLevelNumScalars").value == 3)
      assert(multipleColumnJsonScanNode.metrics("variantBuilderNestedNumScalars").value == 11)

      // Number of paths in all variants
      assert(multipleColumnJsonScanNode.metrics("variantBuilderTopLevelNumPaths").value == 5)
      assert(multipleColumnJsonScanNode.metrics("variantBuilderNestedNumPaths").value == 23)
    }
  }

  test("json scan metrics - no rows") {
    withTempDir { dir =>
      val fileTemp = new File(dir, "file.json.gz")
      val file = fileTemp.getAbsolutePath
      val input: Seq[Row] = Seq()
      spark
        .createDataFrame(spark.sparkContext.parallelize(input, 1), StructType.fromDDL("j string"))
        .write
        .option("compression", "gzip")
        .text(file)
      // Single Variant Column
      val singleColumnDf = spark.read.format("json").option("singleVariantColumn", "var").load(file)

      assert(singleColumnDf.rdd.getNumPartitions == 1)
      singleColumnDf.rdd.mapPartitionsWithIndex((index, iter) => Iterator((index, iter.size)))
        .collect()
        .foreach { case (_, size) => assert(size == 0) }

      singleColumnDf.collect()
      val singleColumnJsonScanNode = findNode(singleColumnDf.queryExecution.executedPlan)(
        _.isInstanceOf[FileSourceScanExec]
      ).get
      assert(singleColumnJsonScanNode.metrics.contains("variantBuilderTopLevelNumVariants"))
      assert(singleColumnJsonScanNode.metrics("variantBuilderTopLevelNumVariants").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedNumVariants").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderTopLevelMaxDepth").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedMaxDepth").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderTopLevelByteSizeBound").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedByteSizeBound").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderTopLevelNumScalars").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedNumScalars").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderTopLevelNumPaths").value == 0)
      assert(singleColumnJsonScanNode.metrics("variantBuilderNestedNumPaths").value == 0)

      // Schema with top level and nested variants
      val multipleColumnDf =
        spark.read
          .format("json")
          .schema("a variant, g int, b struct<c:variant,e:array<variant>>")
          .load(file)
      multipleColumnDf.collect()
      val multipleColumnJsonScanNode = findNode(multipleColumnDf.queryExecution.executedPlan)(
        _.isInstanceOf[FileSourceScanExec]
      ).get
      // Number of Variants Produced
      assert(multipleColumnJsonScanNode.metrics("variantBuilderTopLevelNumVariants").value == 0)
      assert(multipleColumnJsonScanNode.metrics("variantBuilderNestedNumVariants").value == 0)
      // Max Depth of Variants
      assert(multipleColumnJsonScanNode.metrics("variantBuilderTopLevelMaxDepth").value == 0)
      assert(multipleColumnJsonScanNode.metrics("variantBuilderNestedMaxDepth").value == 0)
      // Byte Size of Variants
      assert(multipleColumnJsonScanNode.metrics("variantBuilderTopLevelByteSizeBound").value == 0)
      assert(multipleColumnJsonScanNode.metrics("variantBuilderNestedByteSizeBound").value == 0)
      // Number of scalar values in all variants
      assert(multipleColumnJsonScanNode.metrics("variantBuilderTopLevelNumScalars").value == 0)
      assert(multipleColumnJsonScanNode.metrics("variantBuilderNestedNumScalars").value == 0)
      // Number of paths in all variants
      assert(multipleColumnJsonScanNode.metrics("variantBuilderTopLevelNumPaths").value == 0)
      assert(multipleColumnJsonScanNode.metrics("variantBuilderNestedNumPaths").value == 0)
    }
  }

  test("non-json datasource scans don't contain variant metrics") {
    withTempDir { dir =>
      val fileTemp = new File(dir, "file.parquet")
      val file = fileTemp.getAbsolutePath
      val input: Seq[Row] = Seq()
      spark
        .createDataFrame(spark.sparkContext.parallelize(input, 1), StructType.fromDDL("j string"))
        .write
        .parquet(file)
      val df = spark.read.parquet(file)

      df.collect()
      val node = findNode(df.queryExecution.executedPlan)(
        _.isInstanceOf[FileSourceScanExec]
      ).get
      assert(!node.metrics.contains("variantBuilderTopLevelNumVariants"))
    }
  }
}
