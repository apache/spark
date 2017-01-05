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

import org.apache.spark.sql.functions.{from_json, struct, to_json}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{CalendarIntervalType, IntegerType, StructType}

class JsonFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("function get_json_object") {
    val df: DataFrame = Seq(("""{"name": "alice", "age": 5}""", "")).toDF("a", "b")
    checkAnswer(
      df.selectExpr("get_json_object(a, '$.name')", "get_json_object(a, '$.age')"),
      Row("alice", "5"))
  }

  val tuples: Seq[(String, String)] =
    ("1", """{"f1": "value1", "f2": "value2", "f3": 3, "f5": 5.23}""") ::
    ("2", """{"f1": "value12", "f3": "value3", "f2": 2, "f4": 4.01}""") ::
    ("3", """{"f1": "value13", "f4": "value44", "f3": "value33", "f2": 2, "f5": 5.01}""") ::
    ("4", null) ::
    ("5", """{"f1": "", "f5": null}""") ::
    ("6", "[invalid JSON string]") ::
    Nil

  test("function get_json_object - null") {
    val df: DataFrame = tuples.toDF("key", "jstring")
    val expected =
      Row("1", "value1", "value2", "3", null, "5.23") ::
        Row("2", "value12", "2", "value3", "4.01", null) ::
        Row("3", "value13", "2", "value33", "value44", "5.01") ::
        Row("4", null, null, null, null, null) ::
        Row("5", "", null, null, null, null) ::
        Row("6", null, null, null, null, null) ::
        Nil

    checkAnswer(
      df.select($"key", functions.get_json_object($"jstring", "$.f1"),
        functions.get_json_object($"jstring", "$.f2"),
        functions.get_json_object($"jstring", "$.f3"),
        functions.get_json_object($"jstring", "$.f4"),
        functions.get_json_object($"jstring", "$.f5")),
      expected)
  }

  test("json_tuple select") {
    val df: DataFrame = tuples.toDF("key", "jstring")
    val expected =
      Row("1", "value1", "value2", "3", null, "5.23") ::
      Row("2", "value12", "2", "value3", "4.01", null) ::
      Row("3", "value13", "2", "value33", "value44", "5.01") ::
      Row("4", null, null, null, null, null) ::
      Row("5", "", null, null, null, null) ::
      Row("6", null, null, null, null, null) ::
      Nil

    checkAnswer(
      df.select($"key", functions.json_tuple($"jstring", "f1", "f2", "f3", "f4", "f5")),
      expected)

    checkAnswer(
      df.selectExpr("key", "json_tuple(jstring, 'f1', 'f2', 'f3', 'f4', 'f5')"),
      expected)
  }

  test("json_tuple filter and group") {
    val df: DataFrame = tuples.toDF("key", "jstring")
    val expr = df
      .select(functions.json_tuple($"jstring", "f1", "f2"))
      .where($"c0".isNotNull)
      .groupBy($"c1")
      .count()

    val expected = Row(null, 1) ::
      Row("2", 2) ::
      Row("value2", 1) ::
      Nil

    checkAnswer(expr, expected)
  }

  test("from_json") {
    val df = Seq("""{"a": 1}""").toDS()
    val schema = new StructType().add("a", IntegerType)

    checkAnswer(
      df.select(from_json($"value", schema)),
      Row(Row(1)) :: Nil)
  }

  test("from_json missing columns") {
    val df = Seq("""{"a": 1}""").toDS()
    val schema = new StructType().add("b", IntegerType)

    checkAnswer(
      df.select(from_json($"value", schema)),
      Row(Row(null)) :: Nil)
  }

  test("from_json invalid json") {
    val df = Seq("""{"a" 1}""").toDS()
    val schema = new StructType().add("a", IntegerType)

    checkAnswer(
      df.select(from_json($"value", schema)),
      Row(null) :: Nil)
  }

  test("to_json") {
    val df = Seq(Tuple1(Tuple1(1))).toDF("a")

    checkAnswer(
      df.select(to_json($"a")),
      Row("""{"_1":1}""") :: Nil)
  }

  test("to_json unsupported type") {
    val df = Seq(Tuple1(Tuple1("interval -3 month 7 hours"))).toDF("a")
      .select(struct($"a._1".cast(CalendarIntervalType).as("a")).as("c"))
    val e = intercept[AnalysisException]{
      // Unsupported type throws an exception
      df.select(to_json($"c")).collect()
    }
    assert(e.getMessage.contains(
      "Unable to convert column a of type calendarinterval to JSON."))
  }

  test("roundtrip in to_json and from_json") {
    val dfOne = Seq(Tuple1(Tuple1(1)), Tuple1(null)).toDF("struct")
    val schemaOne = dfOne.schema(0).dataType.asInstanceOf[StructType]
    val readBackOne = dfOne.select(to_json($"struct").as("json"))
      .select(from_json($"json", schemaOne).as("struct"))
    checkAnswer(dfOne, readBackOne)

    val dfTwo = Seq(Some("""{"a":1}"""), None).toDF("json")
    val schemaTwo = new StructType().add("a", IntegerType)
    val readBackTwo = dfTwo.select(from_json($"json", schemaTwo).as("struct"))
      .select(to_json($"struct").as("json"))
    checkAnswer(dfTwo, readBackTwo)
  }
}
