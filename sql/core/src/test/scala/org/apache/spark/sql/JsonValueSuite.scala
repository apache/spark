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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.expressions.JsonValue
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * End-to-end tests for the SQL:2016 `JSON_VALUE` scalar function.
 */
class JsonValueSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val doc =
    """{"id":7,"name":"Ada","tags":["x","y"],"addr":{"city":"NYC"},"score":null,"f":"3.14"}"""

  test("plain call goes through routine resolution and can be shadowed via SET PATH") {
    // `json_value` is now a registered built-in, so `withUserDefinedFunction`'s cleanup assertion
    // does not fit; drop the temporary routine explicitly instead.
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_value(a STRING, b STRING) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        // A plain call is an ordinary function call, so the temporary routine shadows the
        // built-in scalar function.
        checkAnswer(sql(s"SELECT json_value('$doc', '$$.name')"), Row("shadowed"))
        checkAnswer(sql(s"SELECT json_value(*, '$$.name') FROM VALUES ('$doc') AS t(j)"),
          Row("shadowed"))
        // The clause-bearing form is not a function call, so it stays the built-in function.
        checkAnswer(sql(s"SELECT json_value('$doc', '$$.name' RETURNING STRING)"), Row("Ada"))
        assert(sql(s"SELECT json_value('$doc', '$$.name' RETURNING STRING)")
          .queryExecution.analyzed.expressions.exists(_.exists(_.isInstanceOf[JsonValue])))
        // A constructor source is still a clause-free outer call, so the temporary routine shadows
        // it instead of letting the built-in scalar function run.
        checkAnswer(sql(s"SELECT json_value(json_array(1, 2, 3), '$$[0]')"), Row("shadowed"))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_value")
      }
    }
  }

  test("extract a scalar value as STRING by default") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.name')"), Row("Ada"))
    // Numbers and booleans come back as their JSON text under the default STRING RETURNING.
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.id')"), Row("7"))
  }

  test("RETURNING casts the scalar to the requested type") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.id' RETURNING INT)"), Row(7))
    assert(sql(s"SELECT json_value('$doc', '$$.id' RETURNING INT)").schema.head.dataType
      === IntegerType)
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.f' RETURNING DOUBLE)"), Row(3.14d))
    checkAnswer(sql("SELECT json_value('{\"v\":\"true\"}', '$.v' RETURNING BOOLEAN)"), Row(true))
    checkAnswer(
      sql("SELECT json_value('{\"v\":\"2020-01-02\"}', '$.v' RETURNING DATE)"),
      Row(java.sql.Date.valueOf("2020-01-02")))
  }

  test("default RETURNING type is STRING") {
    assert(sql(s"SELECT json_value('$doc', '$$.name')").schema.head.dataType === StringType)
  }

  test("qualified plain JSON_VALUE resolves to the built-in scalar function") {
    checkAnswer(sql(s"SELECT builtin.json_value('$doc', '$$.name')"), Row("Ada"))
    checkAnswer(sql(s"SELECT system.builtin.json_value('$doc', '$$.name')"), Row("Ada"))
  }

  test("a raw JSON number keeps its exact source digits (no double rounding)") {
    // The matched scalar is read straight from the parser, so a fraction with more digits than a
    // double can represent reaches the DECIMAL cast (and the default STRING form) intact.
    val big = """{"v":0.123456789012345678}"""
    checkAnswer(
      sql(s"SELECT json_value('$big', '$$.v' RETURNING DECIMAL(38,18))"),
      Row(new java.math.BigDecimal("0.123456789012345678")))
    checkAnswer(sql(s"SELECT json_value('$big', '$$.v')"), Row("0.123456789012345678"))
  }

  test("a present JSON null yields SQL NULL") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.score')"), Row(null))
  }

  test("a non-scalar (object/array) match is an ON ERROR case, NULL by default") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.addr')"), Row(null))
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.tags')"), Row(null))
  }

  test("a missing path is an ON EMPTY case, NULL by default") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.missing')"), Row(null))
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.addr.zip')"), Row(null))
  }

  test("NULL JSON input propagates to NULL (not ON EMPTY / ON ERROR)") {
    checkAnswer(sql("SELECT json_value(CAST(NULL AS STRING), '$.a' ERROR ON EMPTY ERROR ON ERROR)"),
      Row(null))
  }

  test("DEFAULT ON EMPTY") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.missing' DEFAULT '?' ON EMPTY)"), Row("?"))
    checkAnswer(
      sql(s"SELECT json_value('$doc', '$$.missing' RETURNING INT DEFAULT 42 ON EMPTY)"), Row(42))
  }

  test("ERROR ON EMPTY raises for a missing path") {
    val e = intercept[SparkRuntimeException] {
      sql(s"SELECT json_value('$doc', '$$.missing' ERROR ON EMPTY)").collect()
    }
    assert(e.getCondition == "JSON_VALUE_ON_ERROR.EMPTY")
  }

  test("DEFAULT ON ERROR for a non-scalar match and for malformed input") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.addr' DEFAULT 'n/a' ON ERROR)"), Row("n/a"))
    checkAnswer(sql("SELECT json_value('not json', '$.a' DEFAULT 'bad' ON ERROR)"), Row("bad"))
  }

  test("ERROR ON ERROR raises for malformed input") {
    val e = intercept[SparkRuntimeException] {
      sql("SELECT json_value('not json', '$.a' ERROR ON ERROR)").collect()
    }
    assert(e.getCondition == "JSON_VALUE_ON_ERROR.ERROR")
  }

  test("a valid JSON prefix followed by trailing content is an ON ERROR case") {
    // The whole input must be a single well-formed JSON value: a value that parses but is trailed
    // by garbage (or a second root value) is malformed, even when the path matches within the
    // prefix. NULL ON ERROR by default; ERROR ON ERROR raises.
    checkAnswer(sql("""SELECT json_value('{"a":1} trailing', '$.a')"""), Row(null))
    checkAnswer(sql("""SELECT json_value('{"a":1}{"a":2}', '$.a')"""), Row(null))
    val e = intercept[SparkRuntimeException] {
      sql("""SELECT json_value('{"a":1} trailing', '$.a' ERROR ON ERROR)""").collect()
    }
    assert(e.getCondition == "JSON_VALUE_ON_ERROR.ERROR")
  }

  test("ERROR ON ERROR raises for a non-scalar match") {
    val e = intercept[SparkRuntimeException] {
      sql(s"SELECT json_value('$doc', '$$.addr' ERROR ON ERROR)").collect()
    }
    assert(e.getCondition == "JSON_VALUE_ON_ERROR.ERROR")
  }

  test("a failed cast is an ON ERROR case") {
    // NULL ON ERROR default.
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.name' RETURNING INT)"), Row(null))
    // DEFAULT ON ERROR.
    checkAnswer(
      sql(s"SELECT json_value('$doc', '$$.name' RETURNING INT DEFAULT -1 ON ERROR)"), Row(-1))
    // ERROR ON ERROR.
    val e = intercept[SparkRuntimeException] {
      sql(s"SELECT json_value('$doc', '$$.name' RETURNING INT ERROR ON ERROR)").collect()
    }
    assert(e.getCondition == "JSON_VALUE_ON_ERROR.ERROR")
  }

  test("independent ON EMPTY and ON ERROR behaviors") {
    // Missing path -> ON EMPTY branch; malformed input / non-scalar value -> ON ERROR branch.
    checkAnswer(
      sql(s"SELECT json_value('$doc', '$$.missing' DEFAULT 'e' ON EMPTY DEFAULT 'r' ON ERROR)"),
      Row("e"))
    checkAnswer(
      sql(s"SELECT json_value('$doc', '$$.addr' DEFAULT 'e' ON EMPTY DEFAULT 'r' ON ERROR)"),
      Row("r"))
  }

  test("ON ERROR governs a failed cast in BOTH ANSI and non-ANSI mode") {
    // The extracted-scalar cast is always an ANSI (throwing) cast, so a bad conversion routes to ON
    // ERROR identically regardless of the session's ANSI setting -- a non-ANSI session must not
    // silently return NULL and bypass DEFAULT / ERROR ON ERROR. (A NULL-ON-ERROR-only check would
    // pass vacuously in non-ANSI mode, where a lenient cast already yields NULL.)
    Seq("true", "false").foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        withClue(s"ansi=$ansi ") {
          // A valid conversion still succeeds in both modes (the ANSI cast is not stricter here).
          checkAnswer(sql(s"SELECT json_value('$doc', '$$.id' RETURNING INT)"), Row(7))
          // NULL ON ERROR (default).
          checkAnswer(sql(s"SELECT json_value('$doc', '$$.name' RETURNING INT)"), Row(null))
          // DEFAULT ON ERROR.
          checkAnswer(
            sql(s"SELECT json_value('$doc', '$$.name' RETURNING INT DEFAULT -1 ON ERROR)"),
            Row(-1))
          // ERROR ON ERROR.
          val e = intercept[SparkRuntimeException] {
            sql(s"SELECT json_value('$doc', '$$.name' RETURNING INT ERROR ON ERROR)").collect()
          }
          assert(e.getCondition == "JSON_VALUE_ON_ERROR.ERROR")
        }
      }
    }
  }

  test("works over a column of JSON documents") {
    withTempView("docs") {
      Seq(
        (1, """{"a":10}"""),
        (2, """{"a":20}"""),
        (3, """{"b":30}"""),   // missing -> NULL ON EMPTY
        (4, "not json"))       // malformed -> NULL ON ERROR
        .toDF("k", "j").createOrReplaceTempView("docs")
      checkAnswer(
        sql("SELECT k, json_value(j, '$.a' RETURNING INT) AS a FROM docs ORDER BY k"),
        Seq(Row(1, 10), Row(2, 20), Row(3, null), Row(4, null)))
    }
  }

  test("foldable string expression path is accepted by plain SQL/JSON path functions") {
    checkAnswer(sql("""SELECT json_value('{"a":1}', concat('$', '.a'))"""), Row("1"))
    checkAnswer(sql("""SELECT json_query('{"a":[1]}', concat('$', '.a'))"""), Row("[1]"))
    checkAnswer(sql("""SELECT json_exists('{"a":1}', concat('$', '.a'))"""), Row(true))
  }

  test("invalid: a non-foldable path is rejected as a non-foldable argument") {
    // A column path does not match the constructor grammar and is parsed as a plain function
    // call; the registered built-in requires a foldable string path.
    Seq("json_value", "json_query", "json_exists").foreach { func =>
      val e = intercept[AnalysisException] {
        sql(s"SELECT $func(a, b) FROM VALUES ('{}', '$$.x') AS t(a, b)").collect()
      }
      assert(e.getCondition == "NON_FOLDABLE_ARGUMENT", s"for $func")
    }
  }

  test("invalid: star source in plain SQL/JSON path functions is not expanded") {
    val functions = for {
      prefix <- Seq("", "builtin.", "system.builtin.")
      functionName <- Seq("json_value", "json_query", "json_exists")
    } yield s"$prefix$functionName"

    functions.foreach { func =>
      val e = intercept[AnalysisException] {
        sql(s"""SELECT $func(*, '$$.a') FROM VALUES ('{"a":1}') AS t(j)""").collect()
      }
      assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"for $func(*)")
    }
  }

  test("invalid: star source in clause-bearing SQL/JSON path functions is not expanded") {
    Seq(
      "json_value(*, '$.a' RETURNING STRING)",
      "json_query(*, '$' WITH ARRAY WRAPPER)",
      "json_exists(*, '$.a' TRUE ON ERROR)"
    ).foreach { expr =>
      val e = intercept[AnalysisException] {
        sql(s"""SELECT $expr FROM VALUES ('{"a":1}') AS t(j)""").collect()
      }
      assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"for $expr")
    }
  }

  test("single-pass rejects stars in plain and clause-bearing SQL/JSON path functions") {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      Seq(
        "json_value(*, '$.a')",
        "json_value(*, '$.a' RETURNING STRING)",
        "json_query(*, '$' WITH ARRAY WRAPPER)",
        "json_exists(*, '$.a' TRUE ON ERROR)"
      ).foreach { expr =>
        val e = intercept[AnalysisException] {
          spark.sql(s"""SELECT $expr FROM VALUES ('{"a":1}') AS t(j)""")
            .queryExecution.analyzed
        }
        assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"for $expr")
      }
    }
  }

  test("invalid: plain SQL/JSON path functions reject non-string and null path arguments") {
    Seq("json_value", "json_query", "json_exists").foreach { func =>
      val nonStringPath = intercept[AnalysisException] {
        sql(s"SELECT $func('{}', 1)").collect()
      }
      assert(nonStringPath.getCondition == "UNEXPECTED_INPUT_TYPE", s"for $func")

      val nonFoldableNonStringPath = intercept[AnalysisException] {
        sql(s"SELECT $func(a, i) FROM VALUES ('{}', 1) AS t(a, i)").collect()
      }
      assert(nonFoldableNonStringPath.getCondition == "UNEXPECTED_INPUT_TYPE", s"for $func")

      val nullPath = intercept[AnalysisException] {
        sql(s"SELECT $func('{}', CAST(NULL AS STRING))").collect()
      }
      assert(nullPath.getCondition == "DATATYPE_MISMATCH.UNEXPECTED_NULL", s"for $func")
    }
  }

  test("invalid: plain SQL/JSON path functions require exactly two arguments") {
    // A call that does not match the SQL/JSON grammar (wrong arity) is parsed as a plain function
    // call and reaches the registered built-in, which requires exactly the (jsonStr, path) pair.
    Seq("json_value", "json_query", "json_exists").foreach { func =>
      Seq(s"$func('{}')", s"$func('{}', '$$.a', '$$.b')").foreach { call =>
        val e = intercept[AnalysisException](sql(s"SELECT $call").collect())
        assert(e.getCondition == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION", s"for $call")
      }
    }
  }

  // (func, RETURNS type, body, expected shadowed result) for the routed path functions. The
  // shadow gate is shared across all routed built-ins; JsonArraySuite covers `json_array`.
  private val pathFunctionShadows = Seq(
    ("json_value", "STRING", "'persistent'", Row("persistent")),
    ("json_query", "STRING", "'persistent'", Row("persistent")),
    ("json_exists", "BOOLEAN", "false", Row(false)))

  test("plain SQL/JSON path function with star can be shadowed by a persistent function in PATH") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      pathFunctionShadows.foreach { case (func, retType, body, expected) =>
        withDatabase("path_json_fn") {
          sql("CREATE DATABASE path_json_fn")
          sql(s"CREATE FUNCTION path_json_fn.$func(a STRING, b STRING) RETURNS $retType " +
            s"RETURN $body")
          try {
            sql("SET PATH = spark_catalog.path_json_fn, system.builtin")
            checkAnswer(
              sql(s"""SELECT $func(*, '$$.a') FROM VALUES ('{"a":1}') AS t(j)"""),
              expected)
          } finally {
            sql("SET PATH = DEFAULT_PATH")
            sql(s"DROP FUNCTION IF EXISTS path_json_fn.$func")
          }
        }
      }
    }
  }

  test("two-part builtin SQL/JSON path function respects persistentCatalogFirst before " +
      "rejecting star") {
    pathFunctionShadows.foreach { case (func, retType, body, expected) =>
      withDatabase("builtin") {
        sql("CREATE DATABASE builtin")
        sql(s"CREATE FUNCTION builtin.$func(a STRING, b STRING) RETURNS $retType RETURN $body")
        try {
          val query = s"""SELECT builtin.$func(*, '$$.a') FROM VALUES ('{"a":1}') AS t(j)"""
          withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "false") {
            val e = intercept[AnalysisException] {
              sql(query).collect()
            }
            assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"for $func")
          }
          withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
            checkAnswer(sql(query), expected)
          }
        } finally {
          sql(s"DROP FUNCTION IF EXISTS builtin.$func")
        }
      }
    }
  }

  test("invalid: wildcard path is rejected at analysis") {
    val e = intercept[AnalysisException] {
      sql(s"SELECT json_value('$doc', '$$.tags[*]')").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_PATH")
  }

  test("invalid: non-scalar RETURNING type is rejected at analysis") {
    val e = intercept[AnalysisException] {
      sql(s"SELECT json_value('$doc', '$$.addr' RETURNING STRUCT<x:INT>)").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_SCALAR_RETURNING_TYPE")
  }

  test("invalid: a DEFAULT that cannot cast to the RETURNING type is rejected at analysis") {
    // The DEFAULT-to-RETURNING cast is validated up front, so an uncastable default (here an
    // ARRAY DEFAULT with RETURNING INT) fails at analysis rather than late in the fallback branch.
    Seq("ON EMPTY", "ON ERROR").foreach { clause =>
      val e = intercept[AnalysisException] {
        sql(s"SELECT json_value('{}', '$$.x' RETURNING INT DEFAULT array(1) $clause)").collect()
      }
      assert(e.getCondition == "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION",
        s"unexpected condition for $clause: ${e.getCondition}")
    }
  }

  test("bracket path syntax and array index") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.tags[0]')"), Row("x"))
    checkAnswer(sql(s"SELECT json_value('$doc', '$$[\\'name\\']')"), Row("Ada"))
  }

  test("sql renders a bracket-quoted path as a valid, re-parseable string literal") {
    // A path containing single quotes (e.g. `$['name']`) must be escaped when rendered back to
    // SQL, or the round-tripped statement is malformed. Extract the JsonValue expression, render
    // its `.sql`, and confirm the rendered form both parses and evaluates to the same result.
    val df = sql(s"SELECT json_value('$doc', '$$[\\'name\\']')")
    val jsonValue = df.queryExecution.analyzed.expressions
      .flatMap(_.collect { case jv: JsonValue => jv }).head
    val rendered = jsonValue.sql
    assert(rendered.contains("\\'name\\'"), s"path was not escaped in: $rendered")
    checkAnswer(sql(s"SELECT $rendered"), Row("Ada"))
  }

  test("nested path into an object") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.addr.city')"), Row("NYC"))
  }

  test("DEFAULT NULL behaves like NULL ON ERROR/EMPTY") {
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.missing' DEFAULT NULL ON EMPTY)"), Row(null))
    checkAnswer(sql(s"SELECT json_value('$doc', '$$.addr' DEFAULT NULL ON ERROR)"), Row(null))
  }

  test("DEFAULT is a real child expression, not a constant") {
    // A DEFAULT that references a column proves the DEFAULT clause is resolved as a child, and that
    // the value flows per row.
    withTempView("t") {
      Seq((1, "a"), (2, "b")).toDF("k", "fallback").createOrReplaceTempView("t")
      checkAnswer(
        sql("SELECT json_value('{}', '$.x' DEFAULT fallback ON EMPTY) FROM t ORDER BY k"),
        Seq(Row("a"), Row("b")))
    }
  }

  test("a non-string JSON input is rejected at analysis") {
    // JSON_VALUE takes a STRING JSON input; an INT input is a type mismatch (matching the existing
    // JSON functions' ExpectsInputTypes behavior), not a silent coercion.
    val e = intercept[AnalysisException] {
      sql("SELECT json_value(123, '$.a')").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
  }
}
