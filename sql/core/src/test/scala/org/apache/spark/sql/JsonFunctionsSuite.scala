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
import java.time.{Duration, LocalDateTime, Period, ZoneOffset}
import java.util.Locale

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.StreamReadConstraints

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JsonToStructs, Literal, MultiGetJsonObject}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.util.TimestampNanosTestUtils
import org.apache.spark.sql.catalyst.util.TimestampNanosTestUtils.foreachNanosPrecision
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.{DAY, HOUR, MINUTE, SECOND}
import org.apache.spark.sql.types.YearMonthIntervalType.{MONTH, YEAR}

class JsonFunctionsSuite extends SharedSparkSession {
  import testImplicits._

  test("function get_json_object") {
    val df: DataFrame = Seq(("""{"name": "alice", "age": 5}""", "")).toDF("a", "b")
    checkAnswer(
      df.selectExpr("get_json_object(a, '$.name')", "get_json_object(a, '$.age')"),
      Row("alice", "5"))
  }

  test("function get_json_object - support single quotes") {
    val df: DataFrame = Seq(("""{'name': 'fang', 'age': 5}""")).toDF("a")
    checkAnswer(
      df.selectExpr("get_json_object(a, '$.name')", "get_json_object(a, '$.age')"),
      Row("fang", "5"))
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

  test("SPARK-47670: share simple top-level get_json_object paths") {
    val input = Seq[String](
      """{"a":"one","a.b":"dotted","b":2,"obj":{"x":1},"arr":[1,2]}""",
      """{"a":"first","a":"second","b":3}""",
      """{"a":null,"a":"after_null","b":4}""",
      """{"a":"before_error","b":"""",
      """{'a':'single','b':5}""",
      """[1,2,3]""",
      """{"a":"trailing","b":6} trailing text""",
      """{}""",
      null)

    def result(jsonOptimization: Boolean, sharedParsing: Boolean): Seq[Row] = {
      var rows = Seq.empty[Row]
      withSQLConf(
          SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> jsonOptimization.toString,
          SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> sharedParsing.toString) {
        val df = input.toDF("json")
        rows = df.select(
          get_json_object($"json", "$.a"),
          get_json_object($"json", "$['a.b']"),
          get_json_object($"json", "$.b"),
          get_json_object($"json", "$.b").cast(IntegerType),
          get_json_object($"json", "$.obj"),
          get_json_object($"json", "$.arr"),
          get_json_object($"json", "$.missing")).collect().toSeq
      }
      rows
    }

    val legacy = result(jsonOptimization = false, sharedParsing = false)
    assert(result(jsonOptimization = true, sharedParsing = false) == legacy)
    assert(result(jsonOptimization = true, sharedParsing = true) == legacy)
  }

  test("SPARK-57626: share simple nested get_json_object paths") {
    val malformed = """{"a":{"b":1,"c":"\q"},"d":2}"""
    val input = Seq[String](
      """{"a":{"b":1,"c":"x"},"a.b":{"c.d":"dot"},"d":2}""",
      """{"a":{"b":"first"},"a":{"b":"second","c":"later"},"d":3}""",
      """{"a":null,"a":{"b":"after-null","c":null,"c":"after-c-null"},""" +
        """"a.b":{"c.d":4},"d":5}""",
      """{"a":"not-object","a":{"b":{"nested":1},"c":[1,2]},""" +
        """"a.b":{"c.d":"dot"},"d":null,"d":6}""",
      malformed,
      """{'a':{'b':'single','c':7},'a.b':{'c.d':8},'d':9}""",
      """[1,2,3]""",
      """{}""",
      null)

    def result(jsonOptimization: Boolean, sharedParsing: Boolean): Seq[Row] = {
      var rows = Seq.empty[Row]
      withSQLConf(
          SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> jsonOptimization.toString,
          SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> sharedParsing.toString) {
        val query = input.toDF("json").select(
          get_json_object($"json", "$.a.b"),
          get_json_object($"json", "$['a']['c']"),
          get_json_object($"json", "$['a.b']['c.d']"),
          get_json_object($"json", "$.d"))
        if (jsonOptimization && sharedParsing) {
          assert(query.queryExecution.optimizedPlan.exists { plan =>
            plan.expressions.exists(_.exists(_.isInstanceOf[MultiGetJsonObject]))
          })
        }
        rows = query.collect().toSeq
      }
      rows
    }

    val legacy = result(jsonOptimization = false, sharedParsing = false)
    assert(result(jsonOptimization = true, sharedParsing = false) == legacy)
    assert(result(jsonOptimization = true, sharedParsing = true) == legacy)
    assert(legacy.take(6) == Seq(
      Row("1", "x", "dot", "2"),
      Row("first", "later", null, "3"),
      Row("after-null", "after-c-null", "4", "5"),
      Row("{\"nested\":1}", "[1,2]", "dot", "6"),
      Row(null, null, null, null),
      Row("single", "7", "8", "9")))
  }

  test("SPARK-57626: shared nested get_json_object isolates value rendering failures") {
    val invalidSurrogate = "\\" + "uD800"
    val input = Seq(
      s"""{"a":{"b":"before","c":"$invalidSurrogate","d":"after"},"z":"root"}""")

    def result(jsonOptimization: Boolean): Seq[Row] = {
      var rows = Seq.empty[Row]
      withSQLConf(
          SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> jsonOptimization.toString,
          SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> "true") {
        rows = input.toDF("json").select(
          get_json_object($"json", "$.a.b"),
          get_json_object($"json", "$.a.c"),
          get_json_object($"json", "$.a.d"),
          get_json_object($"json", "$.z")).collect().toSeq
      }
      rows
    }

    assert(result(jsonOptimization = true) == result(jsonOptimization = false))
    assert(result(jsonOptimization = true) == Seq(Row("before", null, "after", "root")))
  }

  test("SPARK-47670: shared get_json_object isolates value rendering failures") {
    val invalidSurrogate = "\\" + "uD800"
    val input = Seq(
      s"""{"a":"before","b":"$invalidSurrogate","c":"after"}""",
      s"""{"a":"before","b":{"nested":"$invalidSurrogate"},"c":"after"}""",
      s"""{"a":"before","b":"$invalidSurrogate","b":"valid","c":"after"}""")

    def result(jsonOptimization: Boolean): Seq[Row] = {
      var rows = Seq.empty[Row]
      withSQLConf(
          SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> jsonOptimization.toString,
          SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> "true") {
        rows = input.toDF("json").select(
          get_json_object($"json", "$.a"),
          get_json_object($"json", "$.b"),
          get_json_object($"json", "$.c")).collect().toSeq
      }
      rows
    }

    assert(result(jsonOptimization = true) == result(jsonOptimization = false))
    assert(result(jsonOptimization = true) == Seq(
      Row("before", null, "after"),
      Row("before", s"""{"nested":"$invalidSurrogate"}""", "after"),
      Row("before", null, "after")))
  }

  test("SPARK-47670: shared get_json_object falls back after parser-side rendering failure") {
    val oversized = "x" * (StreamReadConstraints.DEFAULT_MAX_STRING_LEN + 1)

    def result(sharedParsingEnabled: Boolean): Seq[Row] = {
      var rows = Seq.empty[Row]
      withSQLConf(
          SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "true",
          SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> sharedParsingEnabled.toString) {
        val query = Seq(s"""{"a":"$oversized","b.c":2}""").toDF("json").select(
          get_json_object($"json", "$.a"),
          get_json_object($"json", "$['b.c']"))

        if (sharedParsingEnabled) {
          assert(query.queryExecution.optimizedPlan.exists { plan =>
            plan.expressions.exists(_.exists(_.isInstanceOf[MultiGetJsonObject]))
          })
        }
        rows = query.collect().toSeq
      }
      rows
    }

    val sharedResult = result(sharedParsingEnabled = true)
    assert(sharedResult == result(sharedParsingEnabled = false))
    assert(sharedResult == Seq(Row(null, "2")))
  }

  test("SPARK-47670: shared get_json_object does not return partial malformed results") {
    val malformed = """{"a":1,"b":"\q}"}"""

    def result(jsonOptimization: Boolean): Seq[Row] = {
      var rows = Seq.empty[Row]
      withSQLConf(
          SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> jsonOptimization.toString,
          SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> "true") {
        val query = Seq(malformed).toDF("json").select(
          get_json_object($"json", "$.a"),
          get_json_object($"json", "$.b"))
        if (jsonOptimization) {
          assert(query.queryExecution.optimizedPlan.exists { plan =>
            plan.expressions.exists(_.exists(_.isInstanceOf[MultiGetJsonObject]))
          })
        }
        rows = query.collect().toSeq
      }
      rows
    }

    val shared = result(jsonOptimization = true)
    val legacy = result(jsonOptimization = false)
    assert(shared == legacy)
    assert(shared == Seq(Row(null, null)))
  }

  test("SPARK-47670: shared get_json_object handles deeply nested values on a small stack") {
    val depth = 999
    val nested = "[" * depth + "1" + "]" * depth
    val expression = MultiGetJsonObject(
      Literal(s"""{"a":$nested,"b":2}"""), Seq("$.a", "$.b"))
    val result = Array.ofDim[Any](1)
    val thread = new Thread(
      null,
      new Runnable {
        override def run(): Unit = {
          try {
            result(0) = expression.eval()
          } catch {
            case error: Throwable => result(0) = error
          }
        }
      },
      "deep-json-copy",
      256 * 1024)

    thread.start()
    thread.join(30000)
    assert(!thread.isAlive, "Deep JSON extraction did not finish")
    result(0) match {
      case error: Throwable => fail("Deep JSON extraction failed", error)
      case row: InternalRow =>
        assert(row.getUTF8String(0).numBytes() == 2 * depth + 1)
        assert(row.getUTF8String(1).toString == "2")
      case other => fail(s"Unexpected deep JSON extraction result: $other")
    }
  }

  test("SPARK-57626: shared nested get_json_object supports project code generation") {
    withSQLConf(SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> "true") {
      val df = Seq("""{"a":{"x":1,"y":2}}""").toDF("json").select(
        get_json_object($"json", "$.a.x"),
        get_json_object($"json", "$.a.y"))

      checkAnswer(df, Row("1", "2"))
      def containsSharedExtraction(plan: SparkPlan): Boolean = plan match {
        case _: InputAdapter => false
        case other
            if other.expressions.exists(_.exists(_.isInstanceOf[MultiGetJsonObject])) => true
        case other => other.children.exists(containsSharedExtraction)
      }
      assert(df.queryExecution.executedPlan.exists {
        case stage: WholeStageCodegenExec => containsSharedExtraction(stage.child)
        case _ => false
      }, s"Shared get_json_object project was outside whole-stage codegen:\n${df.queryExecution}")
    }
  }

  test("SPARK-42782: Hive compatibility check for get_json_object") {
    val book0 = "{\"author\":\"Nigel Rees\",\"title\":\"Sayings of the Century\"" +
      ",\"category\":\"reference\",\"price\":8.95}"
    val backet0 = "[1,2,{\"b\":\"y\",\"a\":\"x\"}]"
    val backet = "[" + backet0 + ",[3,4],[5,6]]"
    val backetFlat = backet0.substring(0, backet0.length() - 1) + ",3,4,5,6]"

    val book = "[" + book0 + ",{\"author\":\"Herman Melville\",\"title\":\"Moby Dick\"," +
      "\"category\":\"fiction\",\"price\":8.99" +
      ",\"isbn\":\"0-553-21311-3\"},{\"author\":\"J. R. R. Tolkien\"" +
      ",\"title\":\"The Lord of the Rings\",\"category\":\"fiction\"" +
      ",\"reader\":[{\"age\":25,\"name\":\"bob\"},{\"age\":26,\"name\":\"jack\"}]" +
      ",\"price\":22.99,\"isbn\":\"0-395-19395-8\"}]"

    val json = "{\"store\":{\"fruit\":[{\"weight\":8,\"type\":\"apple\"}," +
      "{\"weight\":9,\"type\":\"pear\"}],\"basket\":" + backet + ",\"book\":" + book +
      ",\"bicycle\":{\"price\":19.95,\"color\":\"red\"}}" +
      ",\"email\":\"amy@only_for_json_udf_test.net\"" +
      ",\"owner\":\"amy\",\"zip code\":\"94025\",\"fb:testid\":\"1234\"}"

    // Basic test
    runTest(json, "$.owner", "amy")
    runTest(json, "$.store.bicycle", "{\"price\":19.95,\"color\":\"red\"}")
    runTest(json, "$.store.book", book)
    runTest(json, "$.store.book[0]", book0)
    runTest(json, "$.store.book[*]", book)
    runTest(json, "$.store.book[0].category", "reference")
    runTest(json, "$.store.book[*].category", "[\"reference\",\"fiction\",\"fiction\"]")
    runTest(json, "$.store.book[*].reader[0].age", "25")
    runTest(json, "$.store.book[*].reader[*].age", "[25,26]")
    runTest(json, "$.store.basket[0][1]", "2")
    runTest(json, "$.store.basket[*]", backet)
    runTest(json, "$.store.basket[*][0]", "[1,3,5]")
    runTest(json, "$.store.basket[0][*]", backet0)
    runTest(json, "$.store.basket[*][*]", backetFlat)
    runTest(json, "$.store.basket[0][2].b", "y")
    runTest(json, "$.store.basket[0][*].b", "[\"y\"]")
    runTest(json, "$.non_exist_key", null)
    runTest(json, "$.store.book[10]", null)
    runTest(json, "$.store.book[0].non_exist_key", null)
    runTest(json, "$.store.basket[*].non_exist_key", null)
    runTest(json, "$.store.basket[0][*].non_exist_key", null)
    runTest(json, "$.store.basket[*][*].non_exist_key", null)
    runTest(json, "$.zip code", "94025")
    runTest(json, "$.fb:testid", "1234")
    runTest("{\"a\":\"b\nc\"}", "$.a", "b\nc")

    // Test root array
    runTest("[1,2,3]", "$[0]", "1")
    runTest("[1,2,3]", "$.[0]", null) // Not supported
    runTest("[1,2,3]", "$.[1]", null) // Not supported
    runTest("[1,2,3]", "$[1]", "2")

    runTest("[1,2,3]", "$[3]", null)
    runTest("[1,2,3]", "$.[*]", null) // Not supported
    runTest("[1,2,3]", "$[*]", "[1,2,3]")
    runTest("[1,2,3]", "$", "[1,2,3]")
    runTest("[{\"k1\":\"v1\"},{\"k2\":\"v2\"},{\"k3\":\"v3\"}]", "$[2]", "{\"k3\":\"v3\"}")
    runTest("[{\"k1\":\"v1\"},{\"k2\":\"v2\"},{\"k3\":\"v3\"}]", "$[2].k3", "v3")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1[0].k11[1]", "2")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1[0].k11", "[1,2,3]")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1[0]", "{\"k11\":[1,2,3]}")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1", "[{\"k11\":[1,2,3]}]")
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0]", "{\"k1\":[{\"k11\":[1,2,3]}]}")
    runTest("[[1,2,3],[4,5,6],[7,8,9]]", "$[1]", "[4,5,6]")
    runTest("[[1,2,3],[4,5,6],[7,8,9]]", "$[1][0]", "4")
    runTest("[\"a\",\"b\"]", "$[1]", "b")
    runTest("[[\"a\",\"b\"]]", "$[0][1]", "b")

    runTest("[1,2,3]", "[0]", null)
    runTest("[1,2,3]", "$0", null)
    runTest("[1,2,3]", "0", null)
    runTest("[1,2,3]", "$.", null)

    runTest("[1,2,3]", "$", "[1,2,3]")
    runTest("{\"a\":4}", "$", "{\"a\":4}")

    def runTest(json: String, path: String, exp: String): Unit = {
      checkAnswer(
        Seq(json).toDF().selectExpr(s"get_json_object(value, '$path')"),
        Row(exp))
    }
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

    val nonStringDF = Seq(1, 2).toDF("a")
    checkError(
      exception = intercept[AnalysisException] {
        nonStringDF.select(json_tuple($"a", "1")).collect()
      },
      condition = "DATATYPE_MISMATCH.NON_STRING_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"json_tuple(a, 1)\"",
        "funcName" -> "`json_tuple`"
      ),
      context =
        ExpectedContext(fragment = "json_tuple", callSitePattern = getCurrentClassCallSitePattern)
    )
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

  test("from_json with option (timestampFormat)") {
    val df = Seq("""{"time": "26/08/2015 18:00"}""").toDS()
    val schema = new StructType().add("time", TimestampType)
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0"))))
  }

  test("from_json with option (allowComments)") {
    val df = Seq("""{"str": /* Hello */ "World"}""").toDS()
    val schema = new StructType().add("str", StringType)
    val options = Map("allowComments" -> "true")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row("World")) :: Nil)
  }

  test("from_json with option (allowUnquotedFieldNames)") {
    val df = Seq("""{str: "World"}""").toDS()
    val schema = new StructType().add("str", StringType)
    val options = Map("allowUnquotedFieldNames" -> "true")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row("World")) :: Nil)
  }

  test("from_json with option (allowSingleQuotes)") {
    val df = Seq("""{"str": 'World'}""").toDS()
    val schema = new StructType().add("str", StringType)
    val options = Map("allowSingleQuotes" -> "true")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row("World")) :: Nil)
  }

  test("from_json with option (allowNumericLeadingZeros)") {
    val df = Seq("""{"int": 0018}""").toDS()
    val schema = new StructType().add("int", IntegerType)
    val options = Map("allowNumericLeadingZeros" -> "true")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row(18)) :: Nil)
  }

  test("from_json with option (allowBackslashEscapingAnyCharacter)") {
    val df = Seq("""{"str": "\$10"}""").toDS()
    val schema = new StructType().add("str", StringType)
    val options = Map("allowBackslashEscapingAnyCharacter" -> "true")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row("$10")) :: Nil)
  }

  test("from_json with option (dateFormat)") {
    val df = Seq("""{"time": "26/08/2015"}""").toDS()
    val schema = new StructType().add("time", DateType)
    val options = Map("dateFormat" -> "dd/MM/yyyy")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row(java.sql.Date.valueOf("2015-08-26"))))
  }

  test("from_json with option (allowUnquotedControlChars)") {
    val df = Seq("{\"str\": \"a\u0001b\"}").toDS()
    val schema = new StructType().add("str", StringType)
    val options = Map("allowUnquotedControlChars" -> "true")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row("a\u0001b")) :: Nil)
  }

  test("from_json with option (allowNonNumericNumbers)") {
    val df = Seq("""{"int": +Infinity}""").toDS()
    val schema = new StructType().add("int", FloatType)
    val options = Map("allowNonNumericNumbers" -> "false")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row(null)) :: Nil)
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
      Row(Row(null)) :: Nil)
  }

  test("from_json - json doesn't conform to the array type") {
    val df = Seq("""{"a" 1}""").toDS()
    val schema = ArrayType(StringType)

    checkAnswer(df.select(from_json($"value", schema)), Seq(Row(null)))
  }

  test("from_json array support") {
    val df = Seq("""[{"a": 1, "b": "a"}, {"a": 2}, { }]""").toDS()
    val schema = ArrayType(
      StructType(
        StructField("a", IntegerType) ::
        StructField("b", StringType) :: Nil))

    checkAnswer(
      df.select(from_json($"value", schema)),
      Row(Seq(Row(1, "a"), Row(2, null), Row(null, null))))
  }

  test("from_json uses DDL strings for defining a schema - java") {
    val df = Seq("""{"a": 1, "b": "haa"}""").toDS()
    checkAnswer(
      df.select(from_json($"value", "a INT, b STRING", new java.util.HashMap[String, String]())),
      Row(Row(1, "haa")) :: Nil)
  }

  test("from_json uses DDL strings for defining a schema - scala") {
    val df = Seq("""{"a": 1, "b": "haa"}""").toDS()
    checkAnswer(
      df.select(from_json($"value", "a INT, b STRING", Map[String, String]())),
      Row(Row(1, "haa")) :: Nil)
  }

  test("SPARK-57164: from_json with a nanos timestamp DDL schema string") {
    val df = Seq("""{"c": "2020-01-01T00:00:00.123456789"}""").toDF("value")
    // Fix the session timezone so the TIMESTAMP_LTZ expected value is deterministic.
    withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      foreachNanosPrecision { p =>
        val nano = TimestampNanosTestUtils.nanoOfSecTruncator(p)(123456789)
        Seq(
          s"TIMESTAMP_NTZ($p)" -> TimestampNTZNanosType(p),
          s"TIMESTAMP_LTZ($p)" -> TimestampLTZNanosType(p),
          s"TIMESTAMP($p) WITHOUT TIME ZONE" -> TimestampNTZNanosType(p),
          s"TIMESTAMP($p) WITH LOCAL TIME ZONE" -> TimestampLTZNanosType(p)).foreach {
          case (spelling, expected) =>
            val parsed = df.select(
              from_json($"value", s"c $spelling", Map.empty[String, String]).as("v"))
            // The schema string resolves to the nanos type ...
            assert(parsed.schema("v").dataType.asInstanceOf[StructType]("c").dataType === expected)
            // ... and the JSON datasource correctly parses the nanosecond timestamp, truncating
            // sub-precision digits toward zero.
            val expectedValue = expected match {
              case _: TimestampNTZNanosType => LocalDateTime.of(2020, 1, 1, 0, 0, 0, nano)
              case _: TimestampLTZNanosType =>
                LocalDateTime.of(2020, 1, 1, 0, 0, 0, nano).toInstant(ZoneOffset.UTC)
            }
            checkAnswer(parsed, Row(Row(expectedValue)))
        }
      }
    }
  }

  test("SPARK-57456: to_json with nanos timestamp types") {
    withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      foreachNanosPrecision { p =>
        // The pattern must carry `p` fractional digits to emit the full declared precision; the
        // floored value has exactly `p` significant digits, so the rendered fraction is the first
        // `p` digits of 123456789.
        val fracPat = "S" * p
        val frac = "123456789".take(p)
        val ldt = LocalDateTime.of(2020, 1, 1, 0, 0, 0, 123456789)
        Seq(
          (TimestampNTZNanosType(p): DataType, "timestampNTZFormat",
            s"yyyy-MM-dd'T'HH:mm:ss.$fracPat", ldt: Any,
            s"""{"ts":"2020-01-01T00:00:00.$frac"}"""),
          (TimestampLTZNanosType(p): DataType, "timestampFormat",
            s"yyyy-MM-dd'T'HH:mm:ss.${fracPat}XXX", ldt.toInstant(ZoneOffset.UTC): Any,
            s"""{"ts":"2020-01-01T00:00:00.${frac}Z"}""")).foreach {
          case (nanosType, optKey, fmt, value, expectedJson) =>
            val schema = new StructType().add("ts", nanosType)
            val df = spark.createDataFrame(
              spark.sparkContext.parallelize(Seq(Row(value))), schema)
            checkAnswer(
              df.select(to_json(struct($"ts"), Map(optKey -> fmt))),
              Row(expectedJson))
        }
      }
    }
  }

  test("SPARK-57456: roundtrip in to_json and from_json - nanos timestamps") {
    withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      foreachNanosPrecision { p =>
        val fracPat = "S" * p
        val ldt = LocalDateTime.of(2020, 1, 1, 0, 0, 0, 123456789)
        Seq(
          (TimestampNTZNanosType(p): DataType, "timestampNTZFormat",
            s"yyyy-MM-dd'T'HH:mm:ss.$fracPat", ldt: Any),
          (TimestampLTZNanosType(p): DataType, "timestampFormat",
            s"yyyy-MM-dd'T'HH:mm:ss.${fracPat}XXX", ldt.toInstant(ZoneOffset.UTC): Any)).foreach {
          case (nanosType, optKey, fmt, value) =>
            val schema = new StructType().add("ts", nanosType)
            val df = spark.createDataFrame(
              spark.sparkContext.parallelize(Seq(Row(value))), schema)
            val options = Map(optKey -> fmt)
            // The input column already carries precision `p`, so the to_json -> from_json
            // round-trip with a `p`-digit pattern is loss-free.
            val readBack = df
              .select(to_json(struct($"ts"), options).as("json"))
              .select(from_json($"json", schema, options).as("data"))
              .select($"data.ts".as("ts"))
            checkAnswer(readBack, df.select($"ts"))
        }
      }
    }
  }

  test("from_json with NULL in options map values") {
    val df = Seq("""{"str": "World"}""").toDS()
    val schema = "str STRING"

    checkAnswer(
      df.selectExpr(
        s"from_json(value, '$schema', map('key', 'value', 'mode', NULL))"
      ),
      Row(Row("World")) :: Nil
    )
  }

  test("from_json with NULL in options map key") {
    val df = Seq("""{"str": "World"}""").toDS()
    val schema = "str STRING"

    checkError(
      exception = intercept[SparkRuntimeException] {
        df.selectExpr(
          s"from_json(value, '$schema', map('mode', 'PERMISSIVE', NULL, 'value'))"
        ).show()
      },
      condition = "NULL_MAP_KEY"
    )
  }

  test("to_json - struct") {
    val df = Seq(Tuple1(Tuple1(1))).toDF("a")

    checkAnswer(
      df.select(to_json($"a")),
      Row("""{"_1":1}""") :: Nil)
  }

  test("to_json - array") {
    val df = Seq(Tuple1(Tuple1(1) :: Nil)).toDF("a")
    val df2 = Seq(Tuple1(Map("a" -> 1) :: Nil)).toDF("a")

    checkAnswer(
      df.select(to_json($"a")),
      Row("""[{"_1":1}]""") :: Nil)
    checkAnswer(
      df2.select(to_json($"a")),
      Row("""[{"a":1}]""") :: Nil)
  }

  test("to_json - map") {
    val df1 = Seq(Map("a" -> Tuple1(1))).toDF("a")
    val df2 = Seq(Map("a" -> 1)).toDF("a")

    checkAnswer(
      df1.select(to_json($"a")),
      Row("""{"a":{"_1":1}}""") :: Nil)
    checkAnswer(
      df2.select(to_json($"a")),
      Row("""{"a":1}""") :: Nil)
  }

  test("to_json with option (timestampFormat)") {
    val df = Seq(Tuple1(Tuple1(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0")))).toDF("a")
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm")

    checkAnswer(
      df.select(to_json($"a", options)),
      Row("""{"_1":"26/08/2015 18:00"}""") :: Nil)
  }

  test("to_json ISO default - old dates") {
    withSQLConf("spark.sql.session.timeZone" -> "America/Los_Angeles") {

      val df = Seq(Tuple1(Tuple1(java.sql.Timestamp.valueOf("1800-01-01 00:00:00.0")))).toDF("a")

      checkAnswer(
        df.select(to_json($"a")),
        Row("""{"_1":"1800-01-01T00:00:00.000-07:52:58"}""") :: Nil)
    }
  }

  test("to_json with option (dateFormat)") {
    val df = Seq(Tuple1(Tuple1(java.sql.Date.valueOf("2015-08-26")))).toDF("a")
    val options = Map("dateFormat" -> "dd/MM/yyyy")

    checkAnswer(
      df.select(to_json($"a", options)),
      Row("""{"_1":"26/08/2015"}""") :: Nil)
  }

  test("to_json with option (ignoreNullFields)") {
    val df = Seq(Tuple1(Tuple1(null))).toDF("a")
    val options = Map("ignoreNullFields" -> "true")

    checkAnswer(
      df.select(to_json($"a", options)),
      Row("""{}""") :: Nil)
  }

  test("to_json - interval support") {
    val baseDf = Seq(Tuple1(Tuple1("-3 month 7 hours"))).toDF("a")
    val df = baseDf.select(struct($"a._1".cast(CalendarIntervalType).as("a")).as("c"))
    checkAnswer(
      df.select(to_json($"c")),
      Row("""{"a":"-3 months 7 hours"}""") :: Nil)

    val df1 = baseDf
      .select(struct(map($"a._1".cast(CalendarIntervalType), lit("a")).as("col1")).as("c"))
    checkAnswer(
      df1.select(to_json($"c")),
      Row("""{"col1":{"-3 months 7 hours":"a"}}""") :: Nil)

    val df2 = baseDf
      .select(struct(map(lit("a"), $"a._1".cast(CalendarIntervalType)).as("col1")).as("c"))
    checkAnswer(
      df2.select(to_json($"c")),
      Row("""{"col1":{"a":"-3 months 7 hours"}}""") :: Nil)
  }

  test("roundtrip in to_json and from_json - struct") {
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

  test("roundtrip in to_json and from_json - array") {
    val dfOne = Seq(Tuple1(Tuple1(1) :: Nil), Tuple1(null :: Nil)).toDF("array")
    val schemaOne = dfOne.schema(0).dataType
    val readBackOne = dfOne.select(to_json($"array").as("json"))
      .select(from_json($"json", schemaOne).as("array"))
    checkAnswer(dfOne, readBackOne)

    val dfTwo = Seq(Some("""[{"a":1}]"""), None).toDF("json")
    val schemaTwo = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val readBackTwo = dfTwo.select(from_json($"json", schemaTwo).as("array"))
      .select(to_json($"array").as("json"))
    checkAnswer(dfTwo, readBackTwo)
  }

  test("SPARK-19637 Support to_json in SQL") {
    val df1 = Seq(Tuple1(Tuple1(1))).toDF("a")
    checkAnswer(
      df1.selectExpr("to_json(a)"),
      Row("""{"_1":1}""") :: Nil)

    val df2 = Seq(Tuple1(Tuple1(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0")))).toDF("a")
    checkAnswer(
      df2.selectExpr("to_json(a, map('timestampFormat', 'dd/MM/yyyy HH:mm'))"),
      Row("""{"_1":"26/08/2015 18:00"}""") :: Nil)

    checkError(
      exception = intercept[AnalysisException] {
        df2.selectExpr("to_json(a, named_struct('a', 1))")
      },
      condition = "INVALID_OPTIONS.NON_MAP_FUNCTION",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "to_json(a, named_struct('a', 1))",
        start = 0,
        stop = 31
      )
    )

    checkError(
      exception = intercept[AnalysisException] {
        df2.selectExpr("to_json(a, map('a', 1))")
      },
      condition = "INVALID_OPTIONS.NON_STRING_TYPE",
      parameters = Map("mapType" -> "\"MAP<STRING, INT>\""),
      context = ExpectedContext(
        fragment = "to_json(a, map('a', 1))",
        start = 0,
        stop = 22
      )
    )
  }

  test("SPARK-19967 Support from_json in SQL") {
    val df1 = Seq("""{"a": 1}""").toDS()
    checkAnswer(
      df1.selectExpr("from_json(value, 'a INT')"),
      Row(Row(1)) :: Nil)

    val df2 = Seq("""{"c0": "a", "c1": 1, "c2": {"c20": 3.8, "c21": 8}}""").toDS()
    checkAnswer(
      df2.selectExpr("from_json(value, 'c0 STRING, c1 INT, c2 STRUCT<c20: DOUBLE, c21: INT>')"),
      Row(Row("a", 1, Row(3.8, 8))) :: Nil)

    val df3 = Seq("""{"time": "26/08/2015 18:00"}""").toDS()
    checkAnswer(
      df3.selectExpr(
        "from_json(value, 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy HH:mm'))"),
      Row(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0"))))

    checkError(
      exception = intercept[AnalysisException] {
        df3.selectExpr("from_json(value, 1)")
      },
      condition = "INVALID_SCHEMA.NON_STRING_LITERAL",
      parameters = Map("inputSchema" -> "\"1\""),
      context = ExpectedContext(
        fragment = "from_json(value, 1)",
        start = 0,
        stop = 18
      )
    )

    checkError(
      exception = intercept[AnalysisException] {
        df3.selectExpr("""from_json(value, 'time InvalidType')""")
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map(
        "error" -> "'InvalidType'",
        "hint" -> ""
      ),
      context = ExpectedContext(
        fragment = "from_json(value, 'time InvalidType')",
        start = 0,
        stop = 35
      )
    )
    checkError(
      exception = intercept[AnalysisException] {
        df3.selectExpr("from_json(value, 'time Timestamp', named_struct('a', 1))")
      },
      condition = "INVALID_OPTIONS.NON_MAP_FUNCTION",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "from_json(value, 'time Timestamp', named_struct('a', 1))",
        start = 0,
        stop = 55
      )
    )
    checkError(
      exception = intercept[AnalysisException] {
        df3.selectExpr("from_json(value, 'time Timestamp', map('a', 1))")
      },
      condition = "INVALID_OPTIONS.NON_STRING_TYPE",
      parameters = Map("mapType" -> "\"MAP<STRING, INT>\""),
      context = ExpectedContext(
        fragment = "from_json(value, 'time Timestamp', map('a', 1))",
        start = 0,
        stop = 46
      )
    )
  }

  test("SPARK-24027: from_json - map<string, int>") {
    val in = Seq("""{"a": 1, "b": 2, "c": 3}""").toDS()
    val schema =
      """
        |{
        |  "type" : "map",
        |  "keyType" : "string",
        |  "valueType" : "integer",
        |  "valueContainsNull" : true
        |}
      """.stripMargin
    val out = in.select(from_json($"value", schema, Map[String, String]()))

    assert(out.columns.head == "entries")
    checkAnswer(out, Row(Map("a" -> 1, "b" -> 2, "c" -> 3)))
  }

  test("SPARK-24027: from_json - map<string, struct>") {
    val in = Seq("""{"a": {"b": 1}}""").toDS()
    val schema = MapType(StringType, new StructType().add("b", IntegerType), true)
    val out = in.select(from_json($"value", schema))

    checkAnswer(out, Row(Map("a" -> Row(1))))
  }

  test("SPARK-24027: from_json - map<string, map<string, int>>") {
    val in = Seq("""{"a": {"b": 1}}""").toDS()
    val schema = "map<string, map<string, int>>"
    val out = in.select(from_json($"value", schema, Map.empty[String, String]))

    checkAnswer(out, Row(Map("a" -> Map("b" -> 1))))
  }

  test("SPARK-24027: roundtrip - from_json -> to_json  - map<string, string>") {
    val json = """{"a":1,"b":2,"c":3}"""
    val schema = MapType(StringType, IntegerType, true)
    val out = Seq(json).toDS().select(to_json(from_json($"value", schema)))

    checkAnswer(out, Row(json))
  }

  test("SPARK-24027: roundtrip - to_json -> from_json  - map<string, string>") {
    val in = Seq(Map("a" -> 1)).toDF()
    val schema = MapType(StringType, IntegerType, true)
    val out = in.select(from_json(to_json($"value"), schema))

    checkAnswer(out, in)
  }

  test("SPARK-24027: from_json - wrong map<string, int>") {
    val in = Seq("""{"a" 1}""").toDS()
    val schema = MapType(StringType, IntegerType)
    val out = in.select(from_json($"value", schema, Map[String, String]()))

    checkAnswer(out, Row(null))
  }

  test("SPARK-24027: from_json of a map with unsupported key type") {
    val schema = MapType(StructType(StructField("f", IntegerType) :: Nil), StringType)
    checkError(
      exception = intercept[AnalysisException] {
        Seq("""{{"f": 1}: "a"}""").toDS().select(from_json($"value", schema))
      },
      condition = "DATATYPE_MISMATCH.INVALID_JSON_MAP_KEY_TYPE",
      parameters = Map(
        "schema" -> "\"MAP<STRUCT<f: INT>, STRING>\"",
        "sqlExpr" -> "\"entries\""),
      context =
        ExpectedContext(fragment = "from_json", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("SPARK-24709: infers schemas of json strings and pass them to from_json") {
    val in = Seq("""{"a": [1, 2, 3]}""").toDS()
    val out = in.select(from_json($"value", schema_of_json("""{"a": [1]}""")) as "parsed")
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
      .select(schema_of_json(lit("{a:1}"), Map("allowUnquotedFieldNames" -> "true").asJava))
    checkAnswer(df, Seq(Row("STRUCT<a: BIGINT>")))
  }

  test("from_json - array of primitive types") {
    val df = Seq("[1, 2, 3]").toDF("a")
    val schema = new ArrayType(IntegerType, false)

    checkAnswer(df.select(from_json($"a", schema)), Seq(Row(Array(1, 2, 3))))
  }

  test("from_json - array of primitive types - malformed row") {
    val df = Seq("[1, 2 3]").toDF("a")
    val schema = new ArrayType(IntegerType, false)

    checkAnswer(df.select(from_json($"a", schema)), Seq(Row(null)))
  }

  test("from_json - array of arrays") {
    withTempView("jsonTable") {
      val jsonDF = Seq("[[1], [2, 3], [4, 5, 6]]").toDF("a")
      val schema = new ArrayType(ArrayType(IntegerType, false), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(
        sql("select json[0][0], json[1][1], json[2][2] from jsonTable"),
        Seq(Row(1, 3, 6)))
    }
  }

  test("from_json - array of arrays - malformed row") {
    withTempView("jsonTable") {
      val jsonDF = Seq("[[1], [2, 3], 4, 5, 6]]").toDF("a")
      val schema = new ArrayType(ArrayType(IntegerType, false), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(sql("select json[0] from jsonTable"), Seq(Row(null)))
    }
  }

  test("from_json - array of structs") {
    withTempView("jsonTable") {
      val jsonDF = Seq("""[{"a":1}, {"a":2}, {"a":3}]""").toDF("a")
      val schema = new ArrayType(new StructType().add("a", IntegerType), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(
        sql("select json[0], json[1], json[2] from jsonTable"),
        Seq(Row(Row(1), Row(2), Row(3))))
    }
  }

  test("from_json - array of structs - malformed row") {
    withTempView("jsonTable") {
      val jsonDF = Seq("""[{"a":1}, {"a:2}, {"a":3}]""").toDF("a")
      val schema = new ArrayType(new StructType().add("a", IntegerType), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(sql("select json[0], json[1]from jsonTable"), Seq(Row(null, null)))
    }
  }

  test("from_json - array of maps") {
    withTempView("jsonTable") {
      val jsonDF = Seq("""[{"a":1}, {"b":2}]""").toDF("a")
      val schema = new ArrayType(MapType(StringType, IntegerType, false), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(
        sql("""select json[0], json[1] from jsonTable"""),
        Seq(Row(Map("a" -> 1), Map("b" -> 2))))
    }
  }

  test("from_json - array of maps - malformed row") {
    withTempView("jsonTable") {
      val jsonDF = Seq("""[{"a":1} "b":2}]""").toDF("a")
      val schema = new ArrayType(MapType(StringType, IntegerType, false), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(sql("""select json[0] from jsonTable"""), Seq(Row(null)))
    }
  }

  test("to_json - array of primitive types") {
    val df = Seq(Array(1, 2, 3)).toDF("a")
    checkAnswer(df.select(to_json($"a")), Seq(Row("[1,2,3]")))
  }

  test("roundtrip to_json -> from_json - array of primitive types") {
    val arr = Array(1, 2, 3)
    val df = Seq(arr).toDF("a")
    checkAnswer(df.select(from_json(to_json($"a"), ArrayType(IntegerType))), Row(arr))
  }

  test("roundtrip from_json -> to_json - array of primitive types") {
    val json = "[1,2,3]"
    val df = Seq(json).toDF("a")
    val schema = new ArrayType(IntegerType, false)

    checkAnswer(df.select(to_json(from_json($"a", schema))), Seq(Row(json)))
  }

  test("roundtrip from_json -> to_json - array of arrays") {
    val json = "[[1],[2,3],[4,5,6]]"
    val jsonDF = Seq(json).toDF("a")
    val schema = new ArrayType(ArrayType(IntegerType, false), false)

    checkAnswer(
      jsonDF.select(to_json(from_json($"a", schema))),
      Seq(Row(json)))
  }

  test("roundtrip from_json -> to_json - array of maps") {
    val json = """[{"a":1},{"b":2}]"""
    val jsonDF = Seq(json).toDF("a")
    val schema = new ArrayType(MapType(StringType, IntegerType, false), false)

    checkAnswer(
      jsonDF.select(to_json(from_json($"a", schema))),
      Seq(Row(json)))
  }

  test("roundtrip from_json -> to_json - array of structs") {
    val json = """[{"a":1},{"a":2},{"a":3}]"""
    val jsonDF = Seq(json).toDF("a")
    val schema = new ArrayType(new StructType().add("a", IntegerType), false)

    checkAnswer(
      jsonDF.select(to_json(from_json($"a", schema))),
      Seq(Row(json)))
  }

  test("pretty print - roundtrip from_json -> to_json") {
    val json = """[{"book":{"publisher":[{"country":"NL","year":[1981,1986,1999]}]}}]"""
    val jsonDF = Seq(json).toDF("root")
    val expected =
      """[ {
        |  "book" : {
        |    "publisher" : [ {
        |      "country" : "NL",
        |      "year" : [ 1981, 1986, 1999 ]
        |    } ]
        |  }
        |} ]""".stripMargin

    checkAnswer(
      jsonDF.select(
        to_json(
          from_json($"root", schema_of_json(lit(json))),
          Map("pretty" -> "true"))),
      Seq(Row(expected)))
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
        condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
        parameters = Map(
          "badRecord" -> "[null,null,{\"a\" 1, \"b\": 11}]",
          "failFastMode" -> "FAILFAST")
      )

      checkError(
        exception = intercept[AnalysisException] {
          df.select(from_json($"value", schema, Map("mode" -> "DROPMALFORMED"))).collect()
        },
        condition = "PARSE_MODE_UNSUPPORTED",
        parameters = Map(
          "funcName" -> "`from_json`",
          "mode" -> "DROPMALFORMED"))
    }
  }

  test("SPARK-36069: from_json invalid json schema - check field name and field value") {
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      val schema = new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("_unparsed", StringType)
      val badRec = """{"a": "1", "b": 11}"""
      val df = Seq(badRec, """{"a": 2, "b": 12}""").toDS()

      checkAnswer(
        df.select(from_json($"value", schema, Map("mode" -> "PERMISSIVE"))),
        Row(Row(null, 11, badRec)) :: Row(Row(2, 12, null)) :: Nil)

      val ex = intercept[SparkException] {
        df.select(from_json($"value", schema, Map("mode" -> "FAILFAST"))).collect()
      }

      checkError(
        exception = ex,
        condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
        parameters = Map(
          "badRecord" -> "[null,11,{\"a\": \"1\", \"b\": 11}]",
          "failFastMode" -> "FAILFAST")
      )
      checkError(
        exception = ex.getCause.asInstanceOf[SparkRuntimeException],
        condition = "CANNOT_PARSE_JSON_FIELD",
        parameters = Map(
          "fieldName" -> toSQLValue("a", StringType),
          "fieldValue" -> "1",
          "jsonType" -> "VALUE_STRING",
          "dataType" -> "\"INT\"")
      )
    }
  }

  test("corrupt record column in the middle") {
    val schema = new StructType()
      .add("a", IntegerType)
      .add("_unparsed", StringType)
      .add("b", IntegerType)
    val badRec = """{"a" 1, "b": 11}"""
    val df = Seq(badRec, """{"a": 2, "b": 12}""").toDS()

    checkAnswer(
      df.select(from_json($"value", schema, Map("columnNameOfCorruptRecord" -> "_unparsed"))),
      Row(Row(null, badRec, null)) :: Row(Row(2, null, 12)) :: Nil)
  }

  test("parse timestamps with locale") {
    Seq("en-US", "ko-KR", "zh-CN", "ru-RU").foreach { langTag =>
      val locale = Locale.forLanguageTag(langTag)
      val ts = new SimpleDateFormat("dd/MM/yyyy HH:mm").parse("06/11/2018 18:00")
      val timestampFormat = "dd MMM yyyy HH:mm"
      val sdf = new SimpleDateFormat(timestampFormat, locale)
      val input = Seq(s"""{"time": "${sdf.format(ts)}"}""").toDS()
      val options = Map("timestampFormat" -> timestampFormat, "locale" -> langTag)
      val df = input.select(from_json($"value", "time timestamp", options))

      checkAnswer(df, Row(Row(java.sql.Timestamp.valueOf("2018-11-06 18:00:00.0"))))
    }
  }

  test("from_json - timestamp in micros") {
    val df = Seq("""{"time": "1970-01-01T00:00:00.123456"}""").toDS()
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
      condition = "INVALID_SCHEMA.NON_STRING_LITERAL",
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

  test("SPARK-48863: parse object as an array with partial results enabled") {
    val schema = StructType(StructField("a", StringType) :: StructField("c", IntegerType) :: Nil)

    // Value can be parsed correctly and should return the same result with or without the flag.
    Seq(false, true).foreach { enabled =>
      withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> s"${enabled}") {
        checkAnswer(
          Seq("""{"a": "b", "c": 1}""").toDF("c0")
            .select(from_json($"c0", ArrayType(schema))),
          Row(Seq(Row("b", 1)))
        )
      }
    }

    // Value does not match the schema.
    val df = Seq("""{"a": "b", "c": "1"}""").toDF("c0")

    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "true") {
      checkAnswer(
        df.select(from_json($"c0", ArrayType(schema))),
        Row(Seq(Row("b", null)))
      )
    }

    withSQLConf(SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "false") {
      checkAnswer(
        df.select(from_json($"c0", ArrayType(schema))),
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
      exception = intercept[AnalysisException] {
        df.select(from_json($"json", invalidJsonSchema, Map.empty[String, String])).collect()
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'{'", "hint" -> ""),
      ExpectedContext("from_json", getCurrentClassCallSitePattern)
    )

    val invalidDataType = "MAP<INT, cow>"
    checkError(
      exception = intercept[AnalysisException] {
        df.select(from_json($"json", invalidDataType, Map.empty[String, String])).collect()
      },
      condition = "UNSUPPORTED_DATATYPE",
      parameters = Map("typeName" -> "\"COW\""),
      ExpectedContext("from_json", getCurrentClassCallSitePattern)
    )

    val invalidTableSchema = "x INT, a cow"
    checkError(
      exception = intercept[AnalysisException] {
        df.select(from_json($"json", invalidTableSchema, Map.empty[String, String])).collect()
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'INT'", "hint" -> ""),
      ExpectedContext("from_json", getCurrentClassCallSitePattern)
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
          condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
          parameters = Map(
            "badRecord" -> "[null,null]",
            "failFastMode" -> "FAILFAST")
        )

        checkError(
          exception = intercept[SparkException] {
            df.select(from_json($"value", schema, Map("mode" -> "FAILFAST"))("a")).collect()
          },
          condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
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
          condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
          parameters = Map(
            "badRecord" -> "[null]",
            "failFastMode" -> "FAILFAST")
        )

        checkError(
          exception = intercept[SparkException] {
            df.select(from_json($"value", schema, Map("mode" -> "FAILFAST"))("a")).collect()
          },
          condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
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

  test("SPARK-47670: separately projected from_json fields preserve malformed-input semantics") {
    withSQLConf(
        SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "true",
        SQLConf.JSON_ENABLE_PARTIAL_RESULTS.key -> "true") {
      val schema = StructType.fromDDL("a int, b struct<x: int>")
      val input = Seq("""{"a":1,"b":{"x":""").toDF("json")
      val parsed = from_json($"json", schema)
      val query = input.select(parsed.getField("a"), parsed.getField("b"))

      val parsedSchemas = query.queryExecution.optimizedPlan.expressions.flatMap(_.collect {
        case jsonToStructs: JsonToStructs => jsonToStructs.schema
      })
      assert(parsedSchemas == Seq(
        StructType.fromDDL("a int"),
        StructType.fromDDL("b struct<x: int>")))
      checkAnswer(query, Row(null, null))
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

  test("to_json: unable to convert column of ObjectType to JSON") {
    val df = Seq(1).toDF("a")
    val schema = StructType(StructField("b", ObjectType(classOf[java.lang.Integer])) :: Nil)
    val row = InternalRow.fromSeq(Seq(Integer.valueOf(1)))
    val structData = Column(Literal.create(row, schema))
    checkError(
      exception = intercept[AnalysisException] {
        df.select($"a").withColumn("c", to_json(structData)).collect()
      },
      condition = "DATATYPE_MISMATCH.CANNOT_CONVERT_TO_JSON",
      parameters = Map(
        "sqlExpr" -> "\"to_json(NAMED_STRUCT('b', 1))\"",
        "name" -> "`b`",
        "type" -> "\"JAVA.LANG.INTEGER\""
      ),
      context = ExpectedContext("to_json", getCurrentClassCallSitePattern)
    )
  }

  test("json_array_length function") {
    val df = Seq(null, "[]", "[1, 2, 3]", "{\"key\": 1}", "invalid json")
      .toDF("a")

    val expected = Seq(Row(null), Row(0), Row(3), Row(null), Row(null))

    checkAnswer(df.selectExpr("json_array_length(a)"), expected)
    checkAnswer(df.select(json_array_length($"a")), expected)
  }

  test("json_object_keys function") {
    val df = Seq(null, "{}", "{\"key1\":1, \"key2\": 2}", "[1, 2, 3]", "invalid json")
      .toDF("a")

    val expected = Seq(Row(null), Row(Seq.empty),
      Row(Seq("key1", "key2")), Row(null), Row(null))

    checkAnswer(df.selectExpr("json_object_keys(a)"), expected)
    checkAnswer(df.select(json_object_keys($"a")), expected)
  }

  test("function get_json_object - Codegen Support") {
    withTempView("GetJsonObjectTable") {
      val data = Seq(("1", """{"f1": "value1", "f5": 5.23}""")).toDF("key", "jstring")
      data.createOrReplaceTempView("GetJsonObjectTable")
      val df = sql("SELECT key, get_json_object(jstring, '$.f1') FROM GetJsonObjectTable")
      val plan = df.queryExecution.executedPlan
      assert(plan.isInstanceOf[WholeStageCodegenExec])
      checkAnswer(df, Seq(Row("1", "value1")))
    }
  }

  test("function get_json_object - path is null") {
    val data = Seq(("""{"name": "alice", "age": 5}""", "")).toDF("a", "b")
    val df = data.selectExpr("get_json_object(a, null)")
    val plan = df.queryExecution.executedPlan
    assert(plan.isInstanceOf[WholeStageCodegenExec])
    checkAnswer(df, Row(null))
  }

  test("function get_json_object - json is null") {
    val data = Seq(("""{"name": "alice", "age": 5}""", "")).toDF("a", "b")
    val df = data.selectExpr("get_json_object(null, '$.name')")
    val plan = df.queryExecution.executedPlan
    assert(plan.isInstanceOf[WholeStageCodegenExec])
    checkAnswer(df, Row(null))
  }

  test("function json_tuple - field names foldable") {
    withTempView("t") {
      val json = """{"a":1, "b":2, "c":3}"""
      val df = Seq((json, "a", "b", "c")).toDF("json", "c1", "c2", "c3")
      df.createOrReplaceTempView("t")

      // Json and all field names are foldable.
      val df1 = sql(s"SELECT json_tuple('$json', 'a', 'b', 'c') from t")
      checkAnswer(df1, Row("1", "2", "3"))

      // All field names are foldable.
      val df2 = sql("SELECT json_tuple(json, 'a', 'b', 'c') from t")
      checkAnswer(df2, Row("1", "2", "3"))

      // The field names some foldable, some non-foldable.
      val df3 = sql("SELECT json_tuple(json, 'a', c2, 'c') from t")
      checkAnswer(df3, Row("1", "2", "3"))

      // All field names are non-foldable.
      val df4 = sql("SELECT json_tuple(json, c1, c2, c3) from t")
      checkAnswer(df4, Row("1", "2", "3"))
    }
  }

  test("from_json/to_json with TIME type - all precisions") {
    import java.time.LocalTime
    // Test data: (precision, LocalTime, expected JSON string)
    val testData = Seq(
      (0, LocalTime.of(14, 30, 45), "14:30:45"),
      (1, LocalTime.of(14, 30, 45, 100000000), "14:30:45.1"),
      (2, LocalTime.of(14, 30, 45, 120000000), "14:30:45.12"),
      (3, LocalTime.of(14, 30, 45, 123000000), "14:30:45.123"),
      (4, LocalTime.of(14, 30, 45, 123400000), "14:30:45.1234"),
      (5, LocalTime.of(14, 30, 45, 123450000), "14:30:45.12345"),
      (6, LocalTime.of(14, 30, 45, 123456000), "14:30:45.123456"),
      (7, LocalTime.of(14, 30, 45, 123456700), "14:30:45.1234567"),
      (8, LocalTime.of(14, 30, 45, 123456780), "14:30:45.12345678"),
      (9, LocalTime.of(14, 30, 45, 123456789), "14:30:45.123456789")
    )

    testData.foreach { case (precision, time, timeStr) =>
      val schema = new StructType().add("time", TimeType(precision))

      // Test from_json
      val parseResult = Seq(s"""{"time": "$timeStr"}""").toDS()
        .select(from_json($"value", schema)).collect().head.getAs[Row](0).getAs[LocalTime](0)
      assert(parseResult == time, s"from_json failed for precision $precision")

      // Test to_json
      val jsonResult = Seq(time).toDF("time")
        .select(to_json(struct($"time"))).collect().head.getString(0)
      assert(jsonResult == s"""{"time":"$timeStr"}""", s"to_json failed for precision $precision")

      // Test roundtrip
      val roundtrip = Seq(time).toDF("time")
        .select(to_json(struct($"time")).as("json"))
        .select(from_json($"json", schema).as("struct"))
        .select($"struct.time").collect().head.getAs[LocalTime](0)
      assert(roundtrip == time, s"Roundtrip failed for precision $precision")
    }

    // Test custom format
    val schema = new StructType().add("time", TimeType(6))
    val customTime = LocalTime.of(14, 30, 45, 123456000)
    val customFormat = Map("timeFormat" -> "HH-mm-ss.SSSSSS")

    checkAnswer(
      Seq("""{"time": "14-30-45.123456"}""").toDS()
        .select(from_json($"value", schema, customFormat).as("data"))
        .select($"data.time"),
      Row(customTime) :: Nil)

    checkAnswer(
      Seq(customTime).toDF("time").select(to_json(struct($"time"), customFormat)),
      Row("""{"time":"14-30-45.123456"}""") :: Nil)
  }

  test("TIME type with arrays and nulls") {
    import java.time.LocalTime

    // Test array support
    val times = Seq(LocalTime.of(9, 0, 0), LocalTime.of(17, 45, 30))
    val schema = new StructType().add("times", ArrayType(TimeType()))
    val jsonStr = """{"times":["09:00:00","17:45:30"]}"""

    // to_json
    checkAnswer(
      Seq(times).toDF("times").select(to_json(struct($"times"))),
      Row(jsonStr) :: Nil)

    // from_json
    val parsed = Seq(jsonStr).toDS()
      .select(from_json($"value", schema).as("data"))
      .select($"data.times").collect().head.getSeq[LocalTime](0)
    assert(parsed == times)

    // Test null handling
    checkAnswer(
      Seq("""{"time": null}""").toDS()
        .select(from_json($"value", new StructType().add("time", TimeType())).as("data"))
        .select($"data.time"),
      Row(null) :: Nil)

    // Verify schema inference treats TIME strings as STRING (like TIMESTAMP)
    val inferredSchema = spark.sql("SELECT schema_of_json('{\"time\": \"14:30:45\"}')")
      .collect().head.getString(0)
    assert(inferredSchema.contains("STRING"))
  }
}
