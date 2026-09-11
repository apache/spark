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
import org.apache.spark.sql.catalyst.expressions.{JsonExists, JsonExistsBehavior, Literal}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.BooleanType

/**
 * End-to-end tests for the SQL:2016 `JSON_EXISTS` predicate.
 */
class JsonExistsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val doc =
    """{"id":7,"addr":{"city":"NYC"},"score":null,"tags":["x","y"]}"""

  test("returns BOOLEAN") {
    assert(sql(s"SELECT json_exists('$doc', '$$.id')").schema.head.dataType === BooleanType)
  }

  test("path present -> true, absent -> false") {
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.addr.city')"), Row(true))
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.addr.zip')"), Row(false))
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.missing')"), Row(false))
  }

  test("present but JSON null -> true (distinguishes present-null from absent)") {
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.score')"), Row(true))
  }

  test("a matched object or array -> true") {
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.addr')"), Row(true))
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.tags')"), Row(true))
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.tags[1]')"), Row(true))
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.tags[5]')"), Row(false))
  }

  test("NULL input yields NULL (Unknown), not the ON ERROR path") {
    checkAnswer(
      sql("SELECT json_exists(CAST(NULL AS STRING), '$.a' ERROR ON ERROR)"), Row(null))
  }

  test("malformed input: FALSE ON ERROR by default, and each ON ERROR behavior") {
    checkAnswer(sql("SELECT json_exists('not json', '$.a')"), Row(false))
    checkAnswer(sql("SELECT json_exists('not json', '$.a' FALSE ON ERROR)"), Row(false))
    checkAnswer(sql("SELECT json_exists('not json', '$.a' TRUE ON ERROR)"), Row(true))
    checkAnswer(sql("SELECT json_exists('not json', '$.a' UNKNOWN ON ERROR)"), Row(null))
    val e = intercept[SparkRuntimeException] {
      sql("SELECT json_exists('not json', '$.a' ERROR ON ERROR)").collect()
    }
    assert(e.getCondition == "JSON_EXISTS_ON_ERROR")
  }

  test("empty or whitespace-only input is malformed -> ON ERROR") {
    checkAnswer(sql("SELECT json_exists('', '$.a')"), Row(false))
    checkAnswer(sql("SELECT json_exists('', '$.a' TRUE ON ERROR)"), Row(true))
    checkAnswer(sql("SELECT json_exists('   ', '$.a')"), Row(false))
    checkAnswer(sql("SELECT json_exists('   ', '$.a' TRUE ON ERROR)"), Row(true))
  }

  test("a partial/prefix-garbage input is malformed -> ON ERROR") {
    // A valid prefix followed by trailing garbage is not a single well-formed value. This holds
    // whether the path matches (drainToRootEnd runs after the match) or is absent (drainToRootEnd
    // runs after the miss) -- both surface the trailing content as malformed input.
    checkAnswer(sql("""SELECT json_exists('{"a":1} trailing', '$.a')"""), Row(false))
    checkAnswer(sql("""SELECT json_exists('{"a":1} trailing', '$.a' TRUE ON ERROR)"""), Row(true))
    checkAnswer(sql("""SELECT json_exists('{"a":1} trailing', '$.missing')"""), Row(false))
    checkAnswer(
      sql("""SELECT json_exists('{"a":1} trailing', '$.missing' TRUE ON ERROR)"""), Row(true))
    // A second well-formed root value (not just garbage) is also more than one value -> malformed;
    // drainToRootEnd must reject the trailing root, even though the path matched in the first one.
    checkAnswer(sql("""SELECT json_exists('{"a":1} {"b":2}', '$.a')"""), Row(false))
    checkAnswer(sql("""SELECT json_exists('{"a":1} {"b":2}', '$.a' TRUE ON ERROR)"""), Row(true))
  }

  test("works over a column of JSON documents") {
    withTempView("docs") {
      Seq(
        (1, """{"a":1}"""),
        (2, """{"b":2}"""),      // absent -> false
        (3, """{"a":null}"""),   // present null -> true
        (4, "not json"))         // malformed -> false (default)
        .toDF("k", "j").createOrReplaceTempView("docs")
      checkAnswer(
        sql("SELECT k, json_exists(j, '$.a') AS e FROM docs ORDER BY k"),
        Seq(Row(1, true), Row(2, false), Row(3, true), Row(4, false)))
    }
  }

  test("bracket and nested path syntax") {
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$[\\'addr\\'][\\'city\\']')"), Row(true))
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.addr.city')"), Row(true))
  }

  test("sql escapes a quoted path literal so the rendering re-parses") {
    // A bracket-quoted path contains single quotes; the `sql` rendering must escape them, otherwise
    // it would emit invalid SQL such as JSON_EXISTS('{}', '$['a']['b']').
    val e = JsonExists(Literal("{}"), "$['a']['b']", JsonExistsBehavior.False)
    val parsed = spark.sessionState.sqlParser.parseExpression(e.sql)
    assert(parsed.isInstanceOf[JsonExists])
    assert(parsed.asInstanceOf[JsonExists].path === "$['a']['b']")
  }

  test("lax wildcard [*]: true iff the array has elements; auto-wraps a non-array") {
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.tags[*]')"), Row(true))
    checkAnswer(sql("""SELECT json_exists('{"tags":[]}', '$.tags[*]')"""), Row(false))
    checkAnswer(sql("""SELECT json_exists('{"a":[1,2]}', '$.a[*]')"""), Row(true))
    // lax auto-wrap: a non-array value (scalar or object) is a single-element array.
    checkAnswer(sql("""SELECT json_exists('{"a":5}', '$.a[*]')"""), Row(true))
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.addr[*]')"), Row(true))
    // lax auto-wrap for an explicit index: over a non-array, [0] matches the wrapped value and
    // any [i>0] does not.
    checkAnswer(sql("""SELECT json_exists('{"a":5}', '$.a[0]')"""), Row(true))
    checkAnswer(sql("""SELECT json_exists('{"a":5}', '$.a[1]')"""), Row(false))
  }

  test("lax embedded wildcard $.a[*].b matches when any element has the field") {
    checkAnswer(sql("""SELECT json_exists('{"a":[{"b":1},{"c":2}]}', '$.a[*].b')"""), Row(true))
    // Match only in a later element (guards the short-circuit loop).
    checkAnswer(sql("""SELECT json_exists('{"a":[{"c":1},{"b":2}]}', '$.a[*].b')"""), Row(true))
    checkAnswer(sql("""SELECT json_exists('{"a":[{"c":1}]}', '$.a[*].b')"""), Row(false))
  }

  test("lax member wildcard .* and ['*'] match any member") {
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.addr.*')"), Row(true))
    checkAnswer(sql("""SELECT json_exists('{}', '$.*')"""), Row(false))
    // ['*'] is the bracket-quoted spelling of the same member wildcard.
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$[\\'*\\']')"), Row(true))
    checkAnswer(sql(s"SELECT json_exists('$doc', '$$.addr[\\'*\\']')"), Row(true))
    checkAnswer(sql("SELECT json_exists('{}', '$[\\'*\\']')"), Row(false))
    // lax auto-unwrap: a member wildcard over an array applies to each element, matching iff some
    // element has a member.
    checkAnswer(sql("""SELECT json_exists('[{"a":1}]', '$.*')"""), Row(true))
    checkAnswer(sql("""SELECT json_exists('[1]', '$.*')"""), Row(false))
  }

  test("index step over a non-array member is skipped without corrupting the traversal") {
    // $.*[1] visits each member; [1] over the object member `a` must be fully consumed (return
    // false) so the traversal advances to member `b`, where b[1] matches.
    checkAnswer(sql("""SELECT json_exists('{"a":{"x":1},"b":[0,1]}', '$.*[1]')"""), Row(true))
    checkAnswer(sql("""SELECT json_exists('{"a":{"x":1}}', '$.*[1]')"""), Row(false))
  }

  test("lax auto-unwrap: a member step over an array applies to each element") {
    checkAnswer(sql("""SELECT json_exists('{"a":[{"b":1},{"b":2}]}', '$.a.b')"""), Row(true))
    checkAnswer(sql("""SELECT json_exists('{"a":[{"c":1}]}', '$.a.b')"""), Row(false))
  }

  test("duplicate object keys: first-match, consistent with JSON_VALUE / JSON_TABLE") {
    // A named-key step follows only the first member with that name; a later duplicate is ignored.
    checkAnswer(sql("""SELECT json_exists('{"a":{},"a":{"b":1}}', '$.a.b')"""), Row(false))
    checkAnswer(sql("""SELECT json_exists('{"a":{"b":1},"a":{}}', '$.a.b')"""), Row(true))
    // The duplicate key itself still exists, so a path that stops at it matches.
    checkAnswer(sql("""SELECT json_exists('{"a":{},"a":{"b":1}}', '$.a')"""), Row(true))
  }

  test("invalid: an unparseable path is rejected at analysis") {
    val e = intercept[AnalysisException] {
      sql(s"SELECT json_exists('$doc', '$$[')").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_PATH")
  }

  test("usable in a WHERE predicate") {
    withTempView("docs") {
      Seq((1, """{"a":1}"""), (2, """{"b":2}""")).toDF("k", "j").createOrReplaceTempView("docs")
      checkAnswer(
        sql("SELECT k FROM docs WHERE json_exists(j, '$.a')"), Seq(Row(1)))
    }
  }
}
