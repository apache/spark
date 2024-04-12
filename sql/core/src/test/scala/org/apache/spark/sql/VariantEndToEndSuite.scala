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

import org.apache.spark.sql.catalyst.expressions.{CreateArray, CreateNamedStruct, Literal, StructsToJson}
import org.apache.spark.sql.catalyst.expressions.variant.ParseJson
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.VariantVal

class VariantEndToEndSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("parse_json/to_json round-trip") {
    def check(input: String, output: String = null): Unit = {
      val df = Seq(input).toDF("v")
      val variantDF = df.select(Column(StructsToJson(Map.empty, ParseJson(Column("v").expr))))
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

  test("to_json with nested variant") {
    val df = Seq(1).toDF("v")
    val variantDF1 = df.select(
      Column(StructsToJson(Map.empty, CreateArray(Seq(
        ParseJson(Literal("{}")), ParseJson(Literal("\"\"")), ParseJson(Literal("[1, 2, 3]")))))))
    checkAnswer(variantDF1, Seq(Row("[{},\"\",[1,2,3]]")))

    val variantDF2 = df.select(
      Column(StructsToJson(Map.empty, CreateNamedStruct(Seq(
        Literal("a"), ParseJson(Literal("""{ "x": 1, "y": null, "z": "str" }""")),
        Literal("b"), ParseJson(Literal("[[]]")),
        Literal("c"), ParseJson(Literal("false")))))))
    checkAnswer(variantDF2, Seq(Row("""{"a":{"x":1,"y":null,"z":"str"},"b":[[]],"c":false}""")))
  }

  test("parse_json - Codegen Support") {
    val df = Seq(("1", """{"a": 1}""")).toDF("key", "v").toDF()
    val variantDF = df.select(Column(ParseJson(Column("v").expr)))
    val plan = variantDF.queryExecution.executedPlan
    assert(plan.isInstanceOf[WholeStageCodegenExec])
    val v = VariantBuilder.parseJson("""{"a":1}""")
    val expected = new VariantVal(v.getValue, v.getMetadata)
    checkAnswer(variantDF, Seq(Row(expected)))
  }

  test("schema_of_variant") {
    def check(json: String, expected: String): Unit = {
      val df = Seq(json).toDF("j").selectExpr("schema_of_variant(parse_json(j))")
      checkAnswer(df, Seq(Row(expected)))
    }

    check("null", "VOID")
    check("1", "BIGINT")
    check("1.0", "DECIMAL(1,0)")
    check("1E0", "DOUBLE")
    check("true", "BOOLEAN")
    check("\"2000-01-01\"", "STRING")
    check("""{"a":0}""", "STRUCT<a: BIGINT>")
    check("""{"b": {"c": "c"}, "a":["a"]}""", "STRUCT<a: ARRAY<STRING>, b: STRUCT<c: STRING>>")
    check("[]", "ARRAY<VOID>")
    check("[false]", "ARRAY<BOOLEAN>")
    check("[null, 1, 1.0]", "ARRAY<DECIMAL(20,0)>")
    check("[null, 1, 1.1]", "ARRAY<DECIMAL(21,1)>")
    check("[123456.789, 123.456789]", "ARRAY<DECIMAL(12,6)>")
    check("[1, 11111111111111111111111111111111111111]", "ARRAY<DECIMAL(38,0)>")
    check("[1.1, 11111111111111111111111111111111111111]", "ARRAY<DOUBLE>")
    check("[1, \"1\"]", "ARRAY<VARIANT>")
    check("[{}, true]", "ARRAY<VARIANT>")
    check("""[{"c": ""}, {"a": null}, {"b": 1}]""", "ARRAY<STRUCT<a: VOID, b: BIGINT, c: STRING>>")
    check("""[{"a": ""}, {"a": null}, {"b": 1}]""", "ARRAY<STRUCT<a: STRING, b: BIGINT>>")
    check(
      """[{"a": 1, "b": null}, {"b": true, "a": 1E0}]""",
      "ARRAY<STRUCT<a: DOUBLE, b: BOOLEAN>>"
    )
  }
}
