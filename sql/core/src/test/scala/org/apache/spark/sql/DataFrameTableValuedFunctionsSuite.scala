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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameTableValuedFunctionsSuite extends QueryTest with SharedSparkSession {

  test("explode") {
    val actual1 = spark.tvf.explode(array(lit(1), lit(2)))
    val expected1 = spark.sql("SELECT * FROM explode(array(1, 2))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.explode(map(lit("a"), lit(1), lit("b"), lit(2)))
    val expected2 = spark.sql("SELECT * FROM explode(map('a', 1, 'b', 2))")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.explode(array())
    val expected3 = spark.sql("SELECT * FROM explode(array())")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.explode(map())
    val expected4 = spark.sql("SELECT * FROM explode(map())")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.explode(lit(null).cast("array<int>"))
    val expected5 = spark.sql("SELECT * FROM explode(null :: array<int>)")
    checkAnswer(actual5, expected5)

    val actual6 = spark.tvf.explode(lit(null).cast("map<string, int>"))
    val expected6 = spark.sql("SELECT * FROM explode(null :: map<string, int>)")
    checkAnswer(actual6, expected6)
  }

  test("explode_outer") {
    val actual1 = spark.tvf.explode_outer(array(lit(1), lit(2)))
    val expected1 = spark.sql("SELECT * FROM explode_outer(array(1, 2))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.explode_outer(map(lit("a"), lit(1), lit("b"), lit(2)))
    val expected2 = spark.sql("SELECT * FROM explode_outer(map('a', 1, 'b', 2))")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.explode_outer(array())
    val expected3 = spark.sql("SELECT * FROM explode_outer(array())")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.explode_outer(map())
    val expected4 = spark.sql("SELECT * FROM explode_outer(map())")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.explode_outer(lit(null).cast("array<int>"))
    val expected5 = spark.sql("SELECT * FROM explode_outer(null :: array<int>)")
    checkAnswer(actual5, expected5)

    val actual6 = spark.tvf.explode_outer(lit(null).cast("map<string, int>"))
    val expected6 = spark.sql("SELECT * FROM explode_outer(null :: map<string, int>)")
    checkAnswer(actual6, expected6)
  }

  test("inline") {
    val actual1 = spark.tvf.inline(array(struct(lit(1), lit("a")), struct(lit(2), lit("b"))))
    val expected1 = spark.sql("SELECT * FROM inline(array(struct(1, 'a'), struct(2, 'b')))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.inline(array().cast("array<struct<a:int,b:int>>"))
    val expected2 = spark.sql("SELECT * FROM inline(array() :: array<struct<a:int,b:int>>)")
    checkAnswer(actual2, expected2)

    val actual3 = spark.tvf.inline(array(
      named_struct(lit("a"), lit(1), lit("b"), lit(2)),
      lit(null),
      named_struct(lit("a"), lit(3), lit("b"), lit(4))
    ))
    val expected3 = spark.sql(
      "SELECT * FROM " +
        "inline(array(named_struct('a', 1, 'b', 2), null, named_struct('a', 3, 'b', 4)))")
    checkAnswer(actual3, expected3)
  }

  test("inline_outer") {
    val actual1 = spark.tvf.inline_outer(array(struct(lit(1), lit("a")), struct(lit(2), lit("b"))))
    val expected1 = spark.sql("SELECT * FROM inline_outer(array(struct(1, 'a'), struct(2, 'b')))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.inline_outer(array().cast("array<struct<a:int,b:int>>"))
    val expected2 = spark.sql("SELECT * FROM inline_outer(array() :: array<struct<a:int,b:int>>)")
    checkAnswer(actual2, expected2)

    val actual3 = spark.tvf.inline_outer(array(
      named_struct(lit("a"), lit(1), lit("b"), lit(2)),
      lit(null),
      named_struct(lit("a"), lit(3), lit("b"), lit(4))
    ))
    val expected3 = spark.sql(
      "SELECT * FROM " +
        "inline_outer(array(named_struct('a', 1, 'b', 2), null, named_struct('a', 3, 'b', 4)))")
    checkAnswer(actual3, expected3)
  }

  test("json_tuple") {
    val actual = spark.tvf.json_tuple(lit("""{"a":1,"b":2}"""), lit("a"), lit("b"))
    val expected = spark.sql("""SELECT * FROM json_tuple('{"a":1,"b":2}', 'a', 'b')""")
    checkAnswer(actual, expected)

    val ex = intercept[AnalysisException] {
      spark.tvf.json_tuple(lit("""{"a":1,"b":2}""")).collect()
    }
    assert(ex.errorClass.get == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION")
    assert(ex.messageParameters("functionName") == "`json_tuple`")
  }

  test("posexplode") {
    val actual1 = spark.tvf.posexplode(array(lit(1), lit(2)))
    val expected1 = spark.sql("SELECT * FROM posexplode(array(1, 2))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.posexplode(map(lit("a"), lit(1), lit("b"), lit(2)))
    val expected2 = spark.sql("SELECT * FROM posexplode(map('a', 1, 'b', 2))")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.posexplode(array())
    val expected3 = spark.sql("SELECT * FROM posexplode(array())")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.posexplode(map())
    val expected4 = spark.sql("SELECT * FROM posexplode(map())")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.posexplode(lit(null).cast("array<int>"))
    val expected5 = spark.sql("SELECT * FROM posexplode(null :: array<int>)")
    checkAnswer(actual5, expected5)

    val actual6 = spark.tvf.posexplode(lit(null).cast("map<string, int>"))
    val expected6 = spark.sql("SELECT * FROM posexplode(null :: map<string, int>)")
    checkAnswer(actual6, expected6)
  }

  test("posexplode_outer") {
    val actual1 = spark.tvf.posexplode_outer(array(lit(1), lit(2)))
    val expected1 = spark.sql("SELECT * FROM posexplode_outer(array(1, 2))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.posexplode_outer(map(lit("a"), lit(1), lit("b"), lit(2)))
    val expected2 = spark.sql("SELECT * FROM posexplode_outer(map('a', 1, 'b', 2))")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.posexplode_outer(array())
    val expected3 = spark.sql("SELECT * FROM posexplode_outer(array())")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.posexplode_outer(map())
    val expected4 = spark.sql("SELECT * FROM posexplode_outer(map())")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.posexplode_outer(lit(null).cast("array<int>"))
    val expected5 = spark.sql("SELECT * FROM posexplode_outer(null :: array<int>)")
    checkAnswer(actual5, expected5)

    val actual6 = spark.tvf.posexplode_outer(lit(null).cast("map<string, int>"))
    val expected6 = spark.sql("SELECT * FROM posexplode_outer(null :: map<string, int>)")
    checkAnswer(actual6, expected6)
  }

  test("stack") {
    val actual = spark.tvf.stack(lit(2), lit(1), lit(2), lit(3))
    val expected = spark.sql("SELECT * FROM stack(2, 1, 2, 3)")
    checkAnswer(actual, expected)
  }

  test("collations") {
    val actual = spark.tvf.collations()
    val expected = spark.sql("SELECT * FROM collations()")
    checkAnswer(actual, expected)
  }

  test("sql_keywords") {
    val actual = spark.tvf.sql_keywords()
    val expected = spark.sql("SELECT * FROM sql_keywords()")
    checkAnswer(actual, expected)
  }

  test("variant_explode") {
    val actual1 = spark.tvf.variant_explode(parse_json(lit("""["hello", "world"]""")))
    val expected1 = spark.sql(
      """SELECT * FROM variant_explode(parse_json('["hello", "world"]'))""")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.variant_explode(parse_json(lit("""{"a": true, "b": 3.14}""")))
    val expected2 = spark.sql(
      """SELECT * FROM variant_explode(parse_json('{"a": true, "b": 3.14}'))""")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.variant_explode(parse_json(lit("[]")))
    val expected3 = spark.sql("SELECT * FROM variant_explode(parse_json('[]'))")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.variant_explode(parse_json(lit("{}")))
    val expected4 = spark.sql("SELECT * FROM variant_explode(parse_json('{}'))")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.variant_explode(lit(null).cast("variant"))
    val expected5 = spark.sql("SELECT * FROM variant_explode(null :: variant)")
    checkAnswer(actual5, expected5)

    // not a variant object/array
    val actual6 = spark.tvf.variant_explode(parse_json(lit("1")))
    val expected6 = spark.sql("SELECT * FROM variant_explode(parse_json('1'))")
    checkAnswer(actual6, expected6)
  }

  test("variant_explode_outer") {
    val actual1 = spark.tvf.variant_explode_outer(parse_json(lit("""["hello", "world"]""")))
    val expected1 = spark.sql(
      """SELECT * FROM variant_explode_outer(parse_json('["hello", "world"]'))""")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.variant_explode_outer(parse_json(lit("""{"a": true, "b": 3.14}""")))
    val expected2 = spark.sql(
      """SELECT * FROM variant_explode_outer(parse_json('{"a": true, "b": 3.14}'))""")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.variant_explode_outer(parse_json(lit("[]")))
    val expected3 = spark.sql("SELECT * FROM variant_explode_outer(parse_json('[]'))")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.variant_explode_outer(parse_json(lit("{}")))
    val expected4 = spark.sql("SELECT * FROM variant_explode_outer(parse_json('{}'))")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.variant_explode_outer(lit(null).cast("variant"))
    val expected5 = spark.sql("SELECT * FROM variant_explode_outer(null :: variant)")
    checkAnswer(actual5, expected5)

    // not a variant object/array
    val actual6 = spark.tvf.variant_explode_outer(parse_json(lit("1")))
    val expected6 = spark.sql("SELECT * FROM variant_explode_outer(parse_json('1'))")
    checkAnswer(actual6, expected6)
  }
}
