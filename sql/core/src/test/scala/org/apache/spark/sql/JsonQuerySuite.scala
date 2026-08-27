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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{JsonQuery, JsonQueryBehavior, JsonQueryQuotes,
  JsonQueryWrapper, Literal}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{CharType, StringType, VarcharType}

/**
 * End-to-end tests for the SQL:2016 `JSON_QUERY` function.
 */
class JsonQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val doc =
    """{"id":7,"name":"Ada","tags":["x","y"],"addr":{"city":"NYC"},"score":null}"""

  test("extract an object or array as verbatim JSON text") {
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.addr')"), Row("""{"city":"NYC"}"""))
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.tags')"), Row("""["x","y"]"""))
  }

  test("default RETURNING type is STRING") {
    assert(sql(s"SELECT json_query('$doc', '$$.addr')").schema.head.dataType === StringType)
  }

  test("RETURNING VARCHAR/CHAR is normalized to STRING and does not truncate") {
    // JSON_QUERY returns the fragment verbatim (no length-enforcing cast), so a CHAR/VARCHAR
    // RETURNING must not advertise a length it cannot enforce. The result type is STRING and the
    // value is not truncated -- including when char/varchar type info is otherwise preserved.
    Seq("false", "true").foreach { preserve =>
      withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> preserve) {
        // VARCHAR(n) and CHAR(n) exercise the two separate normalization branches.
        Seq("VARCHAR(2)", "CHAR(2)").foreach { returning =>
          val df = sql(s"SELECT json_query('$doc', '$$.addr' RETURNING $returning)")
          assert(df.schema.head.dataType === StringType, s"$returning preserve=$preserve")
          checkAnswer(df, Row("""{"city":"NYC"}"""))
        }
      }
    }
  }

  test("a directly-constructed JsonQuery with a CHAR/VARCHAR RETURNING is rejected") {
    // The parser normalizes CHAR/VARCHAR to STRING, but a raw CharType/VarcharType supplied by
    // direct Catalyst construction would otherwise advertise a length JSON_QUERY does not enforce.
    // isValidReturningType rejects it, so checkInputDataTypes fails.
    Seq(VarcharType(2), CharType(2)).foreach { returning =>
      val expr = JsonQuery(Literal("{}"), "$.a", returning, JsonQueryWrapper.Without,
        JsonQueryQuotes.Keep, JsonQueryBehavior.Null, JsonQueryBehavior.Null)
      expr.checkInputDataTypes() match {
        case DataTypeMismatch(errorSubClass, _) =>
          assert(errorSubClass == "INVALID_JSON_QUERY_RETURNING_TYPE", s"for $returning")
        case other => fail(s"expected DataTypeMismatch for $returning, got $other")
      }
    }
  }

  test("RETURNING STRING is allowed (the result is JSON text)") {
    checkAnswer(
      sql(s"SELECT json_query('$doc', '$$.addr' RETURNING STRING)"), Row("""{"city":"NYC"}"""))
  }

  test("a scalar result is emitted as JSON text under the default WITHOUT ARRAY WRAPPER") {
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.id')"), Row("7"))
    // A string scalar keeps its surrounding quotes by default (KEEP QUOTES).
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.name')"), Row("\"Ada\""))
  }

  test("a present JSON null yields the JSON text null") {
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.score')"), Row("null"))
  }

  test("a missing path is an ON EMPTY case, NULL by default") {
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.missing')"), Row(null))
  }

  test("NULL JSON input propagates to NULL (not ON EMPTY / ON ERROR)") {
    checkAnswer(sql("SELECT json_query(CAST(NULL AS STRING), '$.a')"), Row(null))
  }

  test("WITH [UNCONDITIONAL] ARRAY WRAPPER wraps the result in a one-element array") {
    checkAnswer(
      sql(s"SELECT json_query('$doc', '$$.tags[0]' WITH ARRAY WRAPPER)"), Row("""["x"]"""))
    checkAnswer(
      sql(s"SELECT json_query('$doc', '$$.tags' WITH UNCONDITIONAL ARRAY WRAPPER)"),
      Row("""[["x","y"]]"""))
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.id' WITH ARRAY WRAPPER)"), Row("[7]"))
  }

  test("WITH CONDITIONAL ARRAY WRAPPER wraps only a scalar") {
    checkAnswer(
      sql(s"SELECT json_query('$doc', '$$.id' WITH CONDITIONAL ARRAY WRAPPER)"), Row("[7]"))
    checkAnswer(
      sql(s"SELECT json_query('$doc', '$$.addr' WITH CONDITIONAL ARRAY WRAPPER)"),
      Row("""{"city":"NYC"}"""))
    checkAnswer(
      sql(s"SELECT json_query('$doc', '$$.tags' WITH CONDITIONAL ARRAY WRAPPER)"),
      Row("""["x","y"]"""))
  }

  test("OMIT QUOTES strips the quotes from a scalar string result") {
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.name' OMIT QUOTES)"), Row("Ada"))
    // KEEP QUOTES is the default and keeps them.
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.name' KEEP QUOTES)"), Row("\"Ada\""))
    // OMIT QUOTES is a no-op for a non-string scalar and for structural results.
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.id' OMIT QUOTES)"), Row("7"))
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.addr' OMIT QUOTES)"), Row("""{"city":"NYC"}"""))
  }

  test("OMIT QUOTES unescapes an escaped string scalar") {
    // Pass the JSON via a column so SQL string-literal escaping does not rewrite it first. The
    // stored JSON is {"s":"a\"b\\c\n"}; s decodes to a, quote, b, backslash, c, newline.
    val df = Seq("""{"s":"a\"b\\c\n"}""").toDF("j")
    // KEEP QUOTES (default) returns the verbatim, re-escaped JSON string.
    checkAnswer(df.selectExpr("json_query(j, '$.s')"), Row(""""a\"b\\c\n""""))
    // OMIT QUOTES returns the raw unescaped content, which is intentionally no longer valid JSON.
    checkAnswer(df.selectExpr("json_query(j, '$.s' OMIT QUOTES)"), Row("a\"b\\c\n"))
  }

  test("the JSON_QUERY keyword is non-reserved and usable as an identifier") {
    // JSON_QUERY and the OBJECT keyword introduced for the ON EMPTY / ON ERROR clause are
    // non-reserved in both modes, so they remain usable as column names.
    withTable("t") {
      sql("CREATE TABLE t (json_query INT, object STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 'x')")
      checkAnswer(sql("SELECT json_query, object FROM t"), Row(1, "x"))
    }
  }

  test("EMPTY ARRAY / EMPTY OBJECT ON EMPTY") {
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.missing' EMPTY ARRAY ON EMPTY)"), Row("[]"))
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.missing' EMPTY OBJECT ON EMPTY)"), Row("{}"))
  }

  test("ERROR ON EMPTY raises for a missing path") {
    val e = intercept[SparkRuntimeException] {
      sql(s"SELECT json_query('$doc', '$$.missing' ERROR ON EMPTY)").collect()
    }
    assert(e.getCondition == "JSON_QUERY_ON_ERROR.EMPTY")
  }

  test("malformed input is an ON ERROR case, NULL by default") {
    checkAnswer(sql("SELECT json_query('not json', '$.a')"), Row(null))
    checkAnswer(sql("SELECT json_query('not json', '$.a' EMPTY ARRAY ON ERROR)"), Row("[]"))
    checkAnswer(sql("SELECT json_query('not json', '$.a' EMPTY OBJECT ON ERROR)"), Row("{}"))
  }

  test("ERROR ON ERROR raises for malformed input") {
    val e = intercept[SparkRuntimeException] {
      sql("SELECT json_query('not json', '$.a' ERROR ON ERROR)").collect()
    }
    assert(e.getCondition == "JSON_QUERY_ON_ERROR.ERROR")
  }

  test("a valid JSON prefix followed by trailing content is an ON ERROR case") {
    checkAnswer(sql("""SELECT json_query('{"a":{"b":1}} trailing', '$.a')"""), Row(null))
    checkAnswer(sql("""SELECT json_query('{"a":1}{"a":2}', '$.a')"""), Row(null))
    val e = intercept[SparkRuntimeException] {
      sql("""SELECT json_query('{"a":{"b":1}} trailing', '$.a' ERROR ON ERROR)""").collect()
    }
    assert(e.getCondition == "JSON_QUERY_ON_ERROR.ERROR")
  }

  test("nested path into an object and array index") {
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.addr.city')"), Row("\"NYC\""))
    checkAnswer(sql(s"SELECT json_query('$doc', '$$.tags[1]')"), Row("\"y\""))
  }

  test("invalid: wildcard path is rejected at analysis") {
    val e = intercept[AnalysisException] {
      sql(s"SELECT json_query('$doc', '$$.tags[*]')").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_PATH")
  }

  test("invalid: non-string RETURNING type is rejected at analysis") {
    val e = intercept[AnalysisException] {
      sql(s"SELECT json_query('$doc', '$$.id' RETURNING INT)").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_QUERY_RETURNING_TYPE")
  }

  test("invalid: OMIT QUOTES combined with an array wrapper is rejected at analysis") {
    val e = intercept[AnalysisException] {
      sql(s"SELECT json_query('$doc', '$$.name' WITH ARRAY WRAPPER OMIT QUOTES)").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_QUERY_WRAPPER_AND_QUOTES")
  }

  test("sql renders a bracket-quoted path as a valid, re-parseable string literal") {
    val df = sql(s"SELECT json_query('$doc', '$$[\\'addr\\']')")
    val jsonQuery = df.queryExecution.analyzed.expressions
      .flatMap(_.collect { case jq: JsonQuery => jq }).head
    val rendered = jsonQuery.sql
    assert(rendered.contains("\\'addr\\'"), s"path was not escaped in: $rendered")
    checkAnswer(sql(s"SELECT $rendered"), Row("""{"city":"NYC"}"""))
  }

  test("works over a column of JSON documents") {
    val df = Seq(
      """{"a":{"x":1}}""",
      """{"a":[1,2]}""",
      """{"b":2}""",
      "not json").toDF("j")
    checkAnswer(
      df.selectExpr("json_query(j, '$.a')"),
      Seq(Row("""{"x":1}"""), Row("[1,2]"), Row(null), Row(null)))
  }
}
