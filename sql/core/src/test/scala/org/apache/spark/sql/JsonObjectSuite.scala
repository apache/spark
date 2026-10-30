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

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.antlr.v4.runtime.atn.PredictionMode

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.analysis.resolver.ExplicitlyUnsupportedResolverFeature
import org.apache.spark.sql.catalyst.expressions.{
  Cast,
  CodegenObjectFactoryMode,
  Collate,
  Expression,
  JsonConstructorNullBehavior,
  JsonObjectExpr,
  Literal,
  UnresolvedJsonConstructor
}
import org.apache.spark.sql.catalyst.parser.{ParseException, SparkParserBailErrorStrategy, SqlBaseLexer, SqlBaseParser}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join}
import org.apache.spark.sql.execution.{ProjectExec, WholeStageCodegenExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, CharType, GeographyType, GeometryType, IntegerType, MapType, StringType, StructType, VarcharType}

/**
 * End-to-end tests for the SQL:2016 `JSON_OBJECT` constructor function.
 */
class JsonObjectSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("in the comma form, colon-bearing arguments bind as colon members (documented precedence)") {
    // `JSON_OBJECT(a:b, c:d)` matches both list forms: the colon-member list and the comma
    // (MySQL-compat) list. The colon-member alternative is declared first, so it wins -- `a:b, c:d`
    // reads as the two members (a -> b) and (c -> d), the SQL-standard `key : value` reading, not
    // as a single comma-form pair whose key and value are `:`-extractions.
    checkAnswer(
      sql("SELECT json_object(a:b, c:d) FROM VALUES ('k1','v1','k2','v2') t(a,b,c,d)"),
      Row("""{"k1":"v1","k2":"v2"}"""))
    // Parenthesizing each argument forces the comma form (one member whose key/value are the
    // `:`-extractions). The non-VARIANT extraction target then errors -- proving it parsed as one
    // comma-form pair, not two colon members (which resolve cleanly above).
    val e = intercept[AnalysisException] {
      sql("SELECT json_object((a:b), (c:d)) FROM VALUES ('k1','v1','k2','v2') t(a,b,c,d)").collect()
    }
    assert(e.getCondition == "COLUMN_IS_NOT_VARIANT_TYPE")
  }

  test("basic object from key-value pairs using VALUE keyword") {
    checkAnswer(
      sql("SELECT json_object('id' VALUE 7, 'name' VALUE 'Ada')"),
      Row("""{"id":7,"name":"Ada"}"""))
  }

  test("construct object using optional KEY keyword") {
    checkAnswer(
      sql("SELECT json_object(KEY 'id' VALUE 7, KEY 'name' VALUE 'Ada')"),
      Row("""{"id":7,"name":"Ada"}"""))
  }

  test("construct object using colon syntax") {
    checkAnswer(
      sql("SELECT json_object('id': 7, 'name': 'Ada')"),
      Row("""{"id":7,"name":"Ada"}"""))
  }

  test("colon member with a column-valued value") {
    // The `key : value` member overlaps the semi-structured extraction form `expr : path`
    // (`primaryExpression COLON semiStructuredExtractionPath`), which also accepts a bare
    // identifier on the right. Confirm a column-valued colon member still parses as a JSON_OBJECT
    // pair rather than as `'k' : v` semi-structured extraction.
    checkAnswer(
      sql("SELECT json_object('k': v) FROM VALUES ('x') t(v)"),
      Row("""{"k":"x"}"""))
  }

  test("a colon member splits at the first top-level colon after the key") {
    val from =
      """FROM (SELECT parse_json('{"x":1,"y":"s"}') AS v, 'kk' AS k, 'e' AS `end`, 'c' AS `case`)"""
    Seq(
      "json_object('k': v:x)" -> """{"k":1}""",
      "json_object(k: v:x)" -> """{"kk":1}""",
      "json_object('k': v:x ABSENT ON NULL)" -> """{"k":1}""",
      "json_object(k: v:x RETURNING STRING)" -> """{"kk":1}""",
      "json_object('a': v:x, k: v:y)" -> """{"a":1,"kk":"s"}""",
      "json_object((v:y::string) : 1)" -> """{"s":1}""",
      "json_object(v:y::string VALUE v:x)" -> """{"s":1}""",
      "json_object(CASE WHEN v:x::int = 1 THEN 'a' END : v:y)" -> """{"a":"s"}""",
      // `end` and `case` are non-reserved and usable as column names: the separator check must read
      // each CASE/END token as keyword or column by its neighbour, not count it blindly.
      "json_object(CASE WHEN end = 'e' AND v:x::int = 1 THEN 'a' END : v:y)" -> """{"a":"s"}""",
      "json_object(case : v:x)" -> """{"c":1}""",
      "json_object(array('a')[0] : v:x)" -> """{"a":1}""",
      "json_object(end : v:x)" -> """{"e":1}"""
    ).foreach { case (call, expected) =>
      checkAnswer(sql(s"SELECT $call $from"), Row(expected))
    }
    checkAnswer(
      spark.sql(s"SELECT json_object('k': v:x) $from", Map("v" -> "zzz")),
      Row("""{"k":1}"""))
    checkAnswer(
      spark.sql("SELECT json_object('k' :p VALUE 1), 'a' :p", Map("p" -> "x")),
      Row("""{"kx":1}""", "ax"))
  }

  test("a case-named column with a postfix operator is a column, not a CASE keyword") {
    // `case` is non-reserved, so `case[0]` (subscript) and `case COLLATE ...` apply a postfix
    // operator to a column named `case`; neither `CASE [` nor `CASE COLLATE` begins a real CASE
    // expression (`[` has no array-literal form, COLLATE needs a left operand). The separator check
    // must read the token right after `case` as a postfix on an operand -- like the already-handled
    // `.` -- so the top-level colon is still the member separator, not swallowed as a CASE branch.
    checkAnswer(
      sql("""SELECT json_object(case[0] : v:x)
            |FROM (SELECT array('c') AS `case`, parse_json('{"x":1}') AS v)""".stripMargin),
      Row("""{"c":1}"""))
    checkAnswer(
      sql("""SELECT json_object(case COLLATE UTF8_LCASE : v:x)
            |FROM (SELECT 'c' AS `case`, parse_json('{"x":1}') AS v)""".stripMargin),
      Row("""{"c":1}"""))
  }

  test("a colon key cannot coalesce a trailing named parameter marker") {
    // `'k' :p` coalesces the marker into the key under the VALUE form, but in the colon form the
    // first top-level `:` is the member separator, so a bare marker-coalesced key is a clean parse
    // error (never a silent mis-split). Parenthesize the key or use the VALUE form.
    val e = intercept[ParseException] {
      spark.sql("SELECT json_object('k' :p : 1)", Map("p" -> "x"))
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
    checkAnswer(
      spark.sql("SELECT json_object(('k' :p) : 1)", Map("p" -> "x")), Row("""{"kx":1}"""))
    checkAnswer(
      spark.sql("SELECT json_object('k' :p VALUE 1)", Map("p" -> "x")), Row("""{"kx":1}"""))
  }

  test("a simple CASE over a named-parameter operand is a valid colon key") {
    // `CASE :p WHEN ... END : value`: the `:p` operand must parse as a named-parameter marker, not
    // be read as the member separator. `CASE :` is ambiguous with a `case` column before the
    // separator (`case : value`), so the separator check decides it by looking ahead for a complete
    // CASE body -- a top-level WHEN closed by its END. It is present here, so `CASE` is the keyword
    // and `:p` stays its operand.
    val from = "FROM (SELECT parse_json('{\"x\":1}') AS v)"
    checkAnswer(
      spark.sql(
        s"SELECT json_object(CASE :p WHEN 1 THEN 'one' ELSE 'other' END : v:x) $from",
        Map("p" -> 1)),
      Row("""{"one":1}"""))
    checkAnswer(
      spark.sql(
        s"SELECT json_object(CASE :p WHEN 1 THEN 'one' ELSE 'other' END : v:x) $from",
        Map("p" -> 2)),
      Row("""{"other":1}"""))
  }

  test("a CASE-valued value does not flip a bare `case` key to the CASE keyword") {
    // `case : CASE WHEN ... END`: the value's WHEN belongs to the value's CASE, not to the key, so
    // the WHEN lookahead must stop at that inner CASE and keep the leading `case` a column with the
    // top-level colon as the member separator. Guards the lookahead against over-triggering.
    checkAnswer(
      sql("""SELECT json_object(case : CASE WHEN v:x::int = 1 THEN 'y' ELSE 'z' END)
            |FROM (SELECT 'c' AS `case`, parse_json('{"x":1}') AS v)""".stripMargin),
      Row("""{"c":"y"}"""))
  }

  test("a `case` column inside a simple-CASE colon key is a column, not a nested CASE") {
    // The simple-CASE key's own body references a column named `case` (the THEN result). The
    // key-colon lookahead must not count that `case` as a nested CASE...END, or it would consume
    // the real END and misread the `:p` operand marker as the member separator.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val from = "FROM (SELECT 'c' AS `case`, 'v' AS value)"
      checkAnswer(
        spark.sql(s"SELECT json_object(CASE :p WHEN 1 THEN case ELSE 'x' END : value) $from",
          Map("p" -> 1)),
        Row("""{"c":"v"}"""))
      checkAnswer(
        spark.sql(s"SELECT json_object(CASE :p WHEN 1 THEN case ELSE 'x' END : value) $from",
          Map("p" -> 2)),
        Row("""{"x":"v"}"""))
      // A postfix operator on the body `case` column is the case most likely to be miscounted as a
      // nested CASE...END by the key-colon lookahead.
      val q = s"SELECT json_object(CASE :p WHEN 1 THEN case::string ELSE 'x' END : value) $from"
      checkAnswer(spark.sql(q, Map("p" -> 1)), Row("""{"c":"v"}"""))
    }
  }

  test("a non-foldable key with escapable characters is JSON-escaped") {
    // A dynamic (non-foldable) key is written through the generator's field-name path, which must
    // escape quotes and backslashes exactly as a quoted key would.
    val df = Seq(("key\"with\\chars", "v")).toDF("k", "v")
    checkAnswer(
      df.selectExpr("json_object(k VALUE v)"),
      Row("""{"key\"with\\chars":"v"}"""))
  }

  test("a bare `when` column value does not flip a `case` colon key to the CASE keyword") {
    // Outside ANSI mode `when` is a bare column; its WHEN token must not be read as the WHEN of a
    // CASE keyword in the `case` key. A lone `when` value has no closing END, so `case` stays a
    // column and the colon stays the member separator.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkAnswer(
        sql("""SELECT json_object(case : when)
              |FROM (SELECT 'c' AS `case`, 'w' AS `when`)""".stripMargin),
        Row("""{"c":"w"}"""))
    }
  }

  test("a case column nested in a colon key is a column, not a CASE keyword") {
    // A `case` column in a CAST or parens is below the key's top level, where the token after it
    // (`AS`, `+`) does not prove it is a column; without the depth guard the spurious CASE count
    // hid the real separator colon (the parens already balance any colon inside).
    checkAnswer(
      sql("""SELECT json_object(CAST(case AS STRING) : v:x)
            |FROM (SELECT 'c' AS `case`, parse_json('{"x":1}') AS v)""".stripMargin),
      Row("""{"c":1}"""))
    checkAnswer(
      sql("""SELECT json_object((case + 1)::string : v:x)
            |FROM (SELECT 1 AS `case`, parse_json('{"x":1}') AS v)""".stripMargin),
      Row("""{"2":1}"""))
  }

  test("a member whose key and value are named key and value splits at VALUE") {
    checkAnswer(
      sql("SELECT json_object(key VALUE value + 1) FROM VALUES ('a', 1) t(key, value)"),
      Row("""{"a":2}"""))
    checkAnswer(
      sql("SELECT json_object(KEY key VALUE value) FROM VALUES ('a', 1) t(key, value)"),
      Row("""{"a":1}"""))
    checkAnswer(
      sql("SELECT json_object(KEY value VALUE key) FROM VALUES ('a', 'b') t(key, value)"),
      Row("""{"b":"a"}"""))
    checkAnswer(
      sql("SELECT json_object('x' VALUE 0, key VALUE value + 1) " +
        "FROM VALUES ('a', 1) t(key, value)"),
      Row("""{"x":0,"a":2}"""))
  }

  test("a key column continued by an operator is the key, not the optional KEY keyword") {
    // `key` is non-reserved; `key + 1` continues the `key` column into the key expression rather
    // than reading `key` as the optional KEY keyword with key `+ 1`. The arithmetic key is itself a
    // type error (JSON object keys must be STRING), but the report names `(key + 1)` -- proof the
    // `key` column is bound into the key, not dropped. Were it read as KEY, the key would be `+ 1`.
    val ex = intercept[AnalysisException] {
      sql("SELECT json_object(key + 1 VALUE value) FROM VALUES (10, 'x') t(key, value)")
    }
    assert(ex.getCondition == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    assert(ex.getMessage.contains("(key + 1)"))
  }

  test("JSON_OBJECT members parse in SLL mode without the LL fallback") {
    Seq(
      "SELECT JSON_OBJECT('k': V)",
      "SELECT JSON_OBJECT(K: V)",
      "SELECT JSON_OBJECT('k': V:X, K: V:Y)",
      "SELECT JSON_OBJECT(KEY 'id' VALUE 7)",
      "SELECT JSON_OBJECT(KEY VALUE VALUE 1)",
      // Exercises the optional-KEY `jsonOptionalKeyFollowsOperand` guard (`key + 1` as the key).
      "SELECT JSON_OBJECT(key + 1 VALUE value)").foreach { query =>
      val parser = new SqlBaseParser(
        new CommonTokenStream(new SqlBaseLexer(CharStreams.fromString(query))))
      parser.setErrorHandler(new SparkParserBailErrorStrategy())
      parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
      parser.singleStatement()
    }
  }

  test("construct object using comma-separated key-value syntax") {
    checkAnswer(
      sql("SELECT json_object('id', 7, 'name', 'Ada')"),
      Row("""{"id":7,"name":"Ada"}"""))
  }

  test("an odd number of arguments in the comma syntax is rejected") {
    // The comma form requires paired key/value arguments; a dangling key ('name') has no value.
    // JSON_OBJECT is a non-reserved keyword, so when the constructor grammar cannot match, the call
    // parses as an ordinary function call and routes to the registered built-in, whose builder
    // rejects the odd argument count rather than silently dropping the dangling key.
    val e = intercept[AnalysisException] {
      sql("SELECT json_object('id', 7, 'name')")
    }
    assert(e.getCondition == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION")
  }

  test("mixing the VALUE/colon form and the comma form is a parse error") {
    // The two member-list styles are mutually exclusive grammar alternatives, so a single
    // constructor cannot mix `key VALUE value` (or `key : value`) members with `key, value` ones.
    Seq(
      "SELECT json_object('a', 1, 'b' VALUE 2)",
      "SELECT json_object('a' VALUE 1, 'b', 2)",
      "SELECT json_object('a' : 1, 'b', 2)").foreach { query =>
      intercept[ParseException](sql(query))
    }
  }

  test("construct object with NULL values (default NULL ON NULL)") {
    checkAnswer(
      sql("SELECT json_object('id': 7, 'v': NULL)"),
      Row("""{"id":7,"v":null}"""))
  }

  test("construct object with explicit NULL ON NULL") {
    checkAnswer(
      sql("SELECT json_object('id', 7, 'v', NULL NULL ON NULL)"),
      Row("""{"id":7,"v":null}"""))
  }

  test("construct object with NULL values and ABSENT ON NULL") {
    checkAnswer(
      sql("SELECT json_object('id': 7, 'v': NULL ABSENT ON NULL)"),
      Row("""{"id":7}"""))
  }

  test("construct empty object") {
    checkAnswer(
      sql("SELECT json_object()"),
      Row("{}"))
  }

  test("construct object with mixed scalar types") {
    checkAnswer(
      sql("""SELECT json_object('int': 42, 'str': 'hello', 'bool': true,
             'float': 3.14)"""),
      Row("""{"int":42,"str":"hello","bool":true,"float":3.14}"""))
  }

  test("construct object with decimal type via Jackson") {
    checkAnswer(
      sql("""SELECT json_object('d' VALUE CAST('123.45' AS DECIMAL(5,2)))"""),
      Row("""{"d":123.45}"""))
  }

  test("construct object with DATE type via Jackson") {
    checkAnswer(
      sql("""SELECT json_object('d' VALUE DATE'2020-01-02')"""),
      Row("""{"d":"2020-01-02"}"""))
  }

  test("construct object with TIMESTAMP type via Jackson") {
    // Note: Jackson includes timezone offset when session timezone is set
    checkAnswer(
      sql("""SELECT json_object('ts' VALUE TIMESTAMP'2020-01-02 10:30:00')"""),
      Row("""{"ts":"2020-01-02T10:30:00.000-08:00"}"""))
  }

  test("TIMESTAMP rendering follows the session time zone on direct and routed paths") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val ts = "TIMESTAMP'2020-01-02 10:30:00'"
      val expected = sql(s"SELECT to_json(named_struct('ts', $ts))")
      checkAnswer(sql(s"SELECT json_object('ts' VALUE $ts)"), expected)
      checkAnswer(sql(s"SELECT json_object('ts', $ts)"), expected)
      checkAnswer(expected, Row("""{"ts":"2020-01-02T10:30:00.000Z"}"""))
    }
  }

  test("struct value renders like to_json") {
    // A struct value must render exactly like `to_json` of the equivalent member.
    checkAnswer(
      sql("SELECT json_object('s' VALUE named_struct('a', 1, 'b', 'x'))"),
      Row("""{"s":{"a":1,"b":"x"}}"""))
    checkAnswer(
      sql("SELECT json_object('s' VALUE named_struct('a', 1, 'b', 'x'))"),
      sql("SELECT to_json(named_struct('s', named_struct('a', 1, 'b', 'x')))"))
  }

  test("array value renders like to_json") {
    checkAnswer(
      sql("SELECT json_object('a' VALUE array(1, 2, 3))"),
      Row("""{"a":[1,2,3]}"""))
    checkAnswer(
      sql("SELECT json_object('a' VALUE array(1, 2, 3))"),
      sql("SELECT to_json(named_struct('a', array(1, 2, 3)))"))
  }

  test("map value renders like to_json") {
    checkAnswer(
      sql("SELECT json_object('m' VALUE map('x', 1, 'y', 2))"),
      Row("""{"m":{"x":1,"y":2}}"""))
    checkAnswer(
      sql("SELECT json_object('m' VALUE map('x', 1, 'y', 2))"),
      sql("SELECT to_json(named_struct('m', map('x', 1, 'y', 2)))"))
  }

  test("nested complex value combining struct, array and map renders like to_json") {
    val value = "named_struct('arr', array(1, 2), 'm', map('k', named_struct('n', 3)))"
    checkAnswer(
      sql(s"SELECT json_object('c' VALUE $value)"),
      sql(s"SELECT to_json(named_struct('c', $value))"))
  }

  test("struct value honors spark.sql.jsonGenerator.ignoreNullFields like to_json") {
    // `ON NULL` controls only top-level members; a null field *inside* a struct value follows
    // spark.sql.jsonGenerator.ignoreNullFields, like `to_json`.
    val value = "named_struct('a', 1, 'b', CAST(NULL AS INT))"
    Seq("true", "false").foreach { ignore =>
      withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> ignore) {
        checkAnswer(
          sql(s"SELECT json_object('s' VALUE $value)"),
          sql(s"SELECT to_json(named_struct('s', $value))"))
      }
    }
    withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "false") {
      checkAnswer(sql(s"SELECT json_object('s' VALUE $value)"), Row("""{"s":{"a":1,"b":null}}"""))
    }
    withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "true") {
      checkAnswer(sql(s"SELECT json_object('s' VALUE $value)"), Row("""{"s":{"a":1}}"""))
    }
  }

  test("top-level ON NULL and struct-internal ignoreNullFields are independent") {
    // With NULL ON NULL (default) and ignoreNullFields=true, a top-level NULL member is kept as
    // `null` while a null field inside a struct value is dropped.
    withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "true") {
      checkAnswer(
        sql("""SELECT json_object('top' VALUE CAST(NULL AS INT),
               's' VALUE named_struct('a', 1, 'b', CAST(NULL AS INT)))"""),
        Row("""{"top":null,"s":{"a":1}}"""))
    }
  }

  test("string escaping in keys") {
    checkAnswer(
      sql("""SELECT json_object('key"with"quotes' VALUE 1)"""),
      Row("""{"key\"with\"quotes":1}"""))
  }

  // For scalar string values JSON_OBJECT must escape exactly like to_json of the equivalent
  // struct (both go through the same Jackson generator); assert that equivalence rather than
  // hand-encoding the escaping, which is easy to get wrong across Scala/SQL/JSON layers.
  test("string escaping in values matches to_json") {
    checkAnswer(
      sql("""SELECT json_object('msg' VALUE 'hello
world')"""),
      sql("""SELECT to_json(named_struct('msg', 'hello
world'))"""))
  }

  test("string escaping with backslash matches to_json") {
    checkAnswer(
      sql("""SELECT json_object('path' VALUE 'c:\windows')"""),
      sql("""SELECT to_json(named_struct('path', 'c:\windows'))"""))
  }

  test("nested JSON_OBJECT spliced raw") {
    checkAnswer(
      sql("""SELECT json_object('a' VALUE json_object('b' VALUE 1))"""),
      Row("""{"a":{"b":1}}"""))
    checkAnswer(
      sql("""SELECT json_object('a', json_object('b', 1))"""),
      Row("""{"a":{"b":1}}"""))
  }

  test("nested JSON_OBJECT with multiple levels") {
    checkAnswer(
      sql("""SELECT json_object('outer' VALUE
             json_object('inner' VALUE 42, 'name' VALUE 'test'))"""),
      Row("""{"outer":{"inner":42,"name":"test"}}"""))
  }

  test("a nested JSON_ARRAY value is spliced raw") {
    checkAnswer(
      sql("SELECT json_object('a' VALUE json_array(1, 2))"),
      Row("""{"a":[1,2]}"""))
  }

  test("JSON_OBJECT nested directly in JSON_ARRAY is spliced as an object element") {
    // The inverse nesting direction: a JSON_OBJECT in a JSON_ARRAY element position stays on the
    // direct grammar path (JsonArrayValueContext), so it is spliced as a JSON object rather than
    // routed through resolution and emitted as a quoted string.
    checkAnswer(
      sql("SELECT json_array(json_object('a', 1), json_object('b', 2))"),
      Row("""[{"a":1},{"b":2}]"""))
  }

  test("a nested JSON_QUERY value is spliced under KEEP QUOTES and quoted under OMIT QUOTES") {
    // JSON_QUERY emits JSON text under the default KEEP QUOTES, so a lexically nested JSON_QUERY is
    // spliced raw: the matched object is {"x":1}, not the quoted string "{\"x\":1}".
    checkAnswer(
      sql("""SELECT json_object('a' VALUE json_query('{"o":{"x":1}}', '$.o'))"""),
      Row("""{"a":{"x":1}}"""))
    // OMIT QUOTES returns the matched scalar string's decoded content (Ada, not "Ada") -- an
    // ordinary string -- so it takes the quoted path (emitsImplicitJsonText is false), never the
    // invalid splice {"a":Ada}.
    checkAnswer(
      sql("""SELECT json_object('a' VALUE json_query('{"n":"Ada"}', '$.n' OMIT QUOTES))"""),
      Row("""{"a":"Ada"}"""))
  }

  test("null key error") {
    val e = intercept[SparkRuntimeException] {
      sql("SELECT json_object(NULL VALUE 'value')").collect()
    }
    // Assert the structured error contract, not just the message text.
    assert(e.getCondition == "JSON_OBJECT_NULL_KEY")
    assert(e.getSqlState == "22004")
  }

  test("a null key is validated before a null value is omitted under ABSENT ON NULL") {
    // ABSENT ON NULL omits members with a null value, but the key is validated first, so a null key
    // still raises JSON_OBJECT_NULL_KEY rather than being silently dropped along with the member.
    val e = intercept[SparkRuntimeException] {
      sql("SELECT json_object(NULL VALUE NULL ABSENT ON NULL)").collect()
    }
    assert(e.getCondition == "JSON_OBJECT_NULL_KEY")
    assert(e.getSqlState == "22004")
  }

  test("non-foldable key and value expressions") {
    val df = Seq(("key1", "val1"), ("key2", "val2")).toDF("k", "v")
    checkAnswer(
      df.selectExpr("json_object(k VALUE v)"),
      Seq(Row("""{"key1":"val1"}"""), Row("""{"key2":"val2"}""")))
  }

  test("non-foldable with NULL value and NULL ON NULL") {
    val df = Seq(("k", null), ("key", "val")).toDF("k", "v")
    checkAnswer(
      df.selectExpr("json_object(k VALUE v)"),
      Seq(Row("""{"k":null}"""), Row("""{"key":"val"}""")))
  }

  test("non-foldable with NULL value and ABSENT ON NULL") {
    val df = Seq(("k", null), ("key", "val")).toDF("k", "v")
    checkAnswer(
      df.selectExpr("json_object(k VALUE v ABSENT ON NULL)"),
      Seq(Row("{}"), Row("""{"key":"val"}""")))
  }

  test("multiple keys with ABSENT ON NULL") {
    checkAnswer(
      sql("""SELECT json_object('a' VALUE 1, 'b' VALUE NULL, 'c' VALUE 3
             ABSENT ON NULL)"""),
      Row("""{"a":1,"c":3}"""))
  }

  test("duplicate keys are emitted in source order") {
    checkAnswer(
      sql("SELECT json_object('k' VALUE 1, 'k' VALUE 2)"),
      Row("""{"k":1,"k":2}"""))
  }

  test("non-string key type is rejected at analysis, not at execution") {
    val ex = intercept[AnalysisException] {
      sql("SELECT json_object(1 VALUE 'x')")
    }
    assert(ex.getMessage.contains("UNEXPECTED_INPUT_TYPE"))
  }

  test("non-string key type reports the actual key argument") {
    val ex = intercept[AnalysisException] {
      sql("SELECT json_object('ok' VALUE 1, 2 VALUE 'bad')")
    }
    checkError(
      exception = ex,
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      sqlState = Some("42K09"),
      parameters = Map(
        "sqlExpr" -> "\"JSON_OBJECT(ok VALUE 1, 2 VALUE bad)\"",
        "paramIndex" -> "third",
        "requiredType" -> "\"STRING\"",
        "inputSql" -> "\"2\"",
        "inputType" -> "\"INT\""),
      queryContext = Array(ExpectedContext("json_object('ok' VALUE 1, 2 VALUE 'bad')", 7, 46)))
  }

  test("collated STRING RETURNING is accepted") {
    // isValidReturningType must accept any StringType instance, not just the default collation.
    checkAnswer(
      sql("SELECT json_object('a' VALUE 1 RETURNING STRING COLLATE UTF8_LCASE)"),
      Row("""{"a":1}"""))
  }

  test("an invalid RETURNING type is reported under DATATYPE_MISMATCH") {
    // The error is emitted as a DataTypeMismatch, so its condition must resolve under
    // DATATYPE_MISMATCH -- not as a top-level INVALID_JSON_RETURNING_TYPE class.
    val e = intercept[AnalysisException] {
      sql("SELECT json_object('a' VALUE 1 RETURNING INT)").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_RETURNING_TYPE")
  }

  test("a directly-constructed JsonObjectExpr with a CHAR/VARCHAR RETURNING is rejected") {
    // The parser normalizes CHAR/VARCHAR RETURNING to STRING, but a raw CharType/VarcharType from
    // direct Catalyst construction would advertise a length JSON_OBJECT does not enforce.
    Seq(VarcharType(2), CharType(2)).foreach { returning =>
      val expr = JsonObjectExpr(
        Seq((Literal("k"), Literal(1))), Seq(false), Seq(false),
        JsonConstructorNullBehavior.Null, returning)
      expr.checkInputDataTypes() match {
        case DataTypeMismatch(errorSubClass, _) =>
          assert(errorSubClass == "INVALID_JSON_RETURNING_TYPE", s"for $returning")
        case other => fail(s"expected DataTypeMismatch for $returning, got $other")
      }
    }
  }

  test("value accepts an unparenthesized predicate expression") {
    // valueExpr is parsed as a full `expression`, so ordinary predicates work without parentheses.
    checkAnswer(sql("SELECT json_object('isnull' VALUE 1 IS NULL)"), Row("""{"isnull":false}"""))
    checkAnswer(sql("SELECT json_object('gt' : 2 > 1)"), Row("""{"gt":true}"""))
  }

  test("widening the value to expression does not change documented forms") {
    // Design-doc examples where a value abuts the ON NULL / RETURNING keywords must still parse and
    // evaluate identically after widening valueExpression -> expression.
    checkAnswer(sql("SELECT json_object('id': 7, 'v': NULL)"), Row("""{"id":7,"v":null}"""))
    checkAnswer(
      sql("SELECT json_object('id': 7, 'v': NULL ABSENT ON NULL)"), Row("""{"id":7}"""))
    checkAnswer(
      sql("SELECT json_object('id', 7, 'v', NULL ABSENT ON NULL)"), Row("""{"id":7}"""))
    checkAnswer(
      sql("SELECT json_object('id' VALUE 7, 'name' VALUE 'Ada')"),
      Row("""{"id":7,"name":"Ada"}"""))
  }

  test("an unsupported value type is rejected at analysis") {
    // A spatial value: JacksonUtils.verifyType accepts it (it is an AtomicType) but
    // JacksonGenerator cannot serialize it, so JSON_OBJECT must reject it up front, not at runtime.
    val bad = JsonObjectExpr(
      Seq((Literal("k"), Literal.create(null, GeometryType(4326)))),
      Seq(false), Seq(false), JsonConstructorNullBehavior.Null, StringType)
    bad.checkInputDataTypes() match {
      case DataTypeMismatch(sub, _) => assert(sub == "CANNOT_CONVERT_TO_JSON")
      case other => fail(s"expected DataTypeMismatch, got $other")
    }
    // A spatial type nested as a MAP VALUE must still be rejected: the guard descends recursively
    // into map values (unlike the top-level atomic case above), so this exercises that descent.
    val badNested = JsonObjectExpr(
      Seq((Literal("k"), Literal.create(null, MapType(StringType, GeometryType(4326))))),
      Seq(false), Seq(false), JsonConstructorNullBehavior.Null, StringType)
    badNested.checkInputDataTypes() match {
      case DataTypeMismatch(sub, _) => assert(sub == "CANNOT_CONVERT_TO_JSON")
      case other => fail(s"expected DataTypeMismatch for a nested spatial value, got $other")
    }
    // The guard also descends into STRUCT fields and ARRAY elements, so a spatial type buried in
    // either -- the other two recursive branches -- must be rejected just like the map-value case.
    val nestedBadTypes = Seq(
      new StructType().add("g", GeometryType(4326)),
      ArrayType(GeographyType(4326)))
    nestedBadTypes.foreach { dt =>
      val expr = JsonObjectExpr(
        Seq((Literal("k"), Literal.create(null, dt))),
        Seq(false), Seq(false), JsonConstructorNullBehavior.Null, StringType)
      expr.checkInputDataTypes() match {
        case DataTypeMismatch(sub, _) => assert(sub == "CANNOT_CONVERT_TO_JSON", s"for $dt")
        case other => fail(s"expected DataTypeMismatch for a spatial type in $dt, got $other")
      }
    }
    // A spatial type appearing only as a MAP KEY is fine: JacksonGenerator writes map keys via
    // toString, so the value-type guard must not over-reject it.
    val ok = JsonObjectExpr(
      Seq((Literal("k"), Literal.create(null, MapType(GeometryType(4326), IntegerType)))),
      Seq(false), Seq(false), JsonConstructorNullBehavior.Null, StringType)
    assert(ok.checkInputDataTypes().isSuccess)
  }

  test("a raw (FORMAT JSON) value that is not a string is rejected") {
    // A raw-spliced value must carry JSON text (string); a non-string raw value would fail with a
    // ClassCastException at eval, so reject it at analysis with the FORMAT JSON input error.
    val expr = JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(true), Seq(true),
      JsonConstructorNullBehavior.Null, StringType)
    expr.checkInputDataTypes() match {
      case DataTypeMismatch(sub, _) => assert(sub == "INVALID_JSON_FORMAT_JSON_INPUT")
      case other => fail(s"expected DataTypeMismatch, got $other")
    }
  }

  test("SQL renders an explicit collated RETURNING and an explicit ON NULL clause") {
    val collated = JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(false), Seq(false), JsonConstructorNullBehavior.Null,
      StringType("UTF8_LCASE"))
    assert(collated.sql.contains("RETURNING STRING COLLATE UTF8_LCASE"))
    // .sql always renders an explicit ON NULL clause so reparse stays on the direct grammar path.
    val default = JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType)
    assert(default.sql == "JSON_OBJECT('k' VALUE 1 NULL ON NULL)")
  }

  test("SQL renders a raw nested value spliced back to raw even after collation wrapping") {
    val inner = JsonObjectExpr(
      Seq((Literal("b"), Literal(1))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType)
    // Simulate the default-collation rule wrapping the raw nested value in a Cast. rawJson stays
    // frozen true; the Cast hides the bare constructor, so .sql renders an explicit FORMAT JSON so
    // reparse splices it raw.
    val wrapped = JsonObjectExpr(
      Seq((Literal("a"), Cast(inner, StringType("UTF8_LCASE")))), Seq(true), Seq(false),
      JsonConstructorNullBehavior.Null, StringType)
    assert(wrapped.sql.contains("FORMAT JSON"))
    checkAnswer(sql(s"SELECT ${wrapped.sql}"), Row("""{"a":{"b":1}}"""))
  }

  test("emitted SQL reparses and evaluates with raw-vs-quoted semantics preserved") {
    // The .sql renderings above are round-trip contracts: reparsing and evaluating them must
    // reproduce the original raw-vs-quoted splicing.
    // A raw nested value renders as a bare constructor and reparses back to raw splicing.
    checkAnswer(
      sql("SELECT JSON_OBJECT('a' VALUE JSON_OBJECT('b' VALUE 1))"), Row("""{"a":{"b":1}}"""))
    // A quoted value that the optimizer inlined as an implicit-JSON expression is neutralized with
    // CAST(... AS STRING); reparsing must keep it quoted rather than splicing it raw.
    val inner = JsonObjectExpr(
      Seq((Literal("b"), Literal(1))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType)
    val quoted = JsonObjectExpr(
      Seq((Literal("a"), inner)), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType)
    assert(quoted.sql ==
      "JSON_OBJECT('a' VALUE CAST(JSON_OBJECT('b' VALUE 1 NULL ON NULL) AS STRING) NULL ON NULL)")
    checkAnswer(sql(s"SELECT ${quoted.sql}"), Row("""{"a":"{\"b\":1}"}"""))
  }

  test("generated column names omit the synthetic NULL ON NULL ownership clause") {
    // Canonical `.sql` appends a default NULL ON NULL so a clause-free call reparses to the builtin
    // (see the shadowing test), but that marker is never reparsed for display. Unaliased column
    // names must therefore not expose ownership text the user never wrote; a non-default
    // ABSENT ON NULL the user did write is kept.
    def colName(q: String): String = sql(q).schema.head.name
    assert(colName("SELECT json_object()") == "JSON_OBJECT()")
    assert(colName("SELECT json_object('id' VALUE 7)") == "JSON_OBJECT(id VALUE 7)")
    assert(colName("SELECT json_object('id', 7)") == "JSON_OBJECT(id VALUE 7)")
    assert(colName("SELECT json_object('a' VALUE json_object('b' VALUE 1))") ==
      "JSON_OBJECT(a VALUE JSON_OBJECT(b VALUE 1))")
    assert(colName("SELECT json_object('v' VALUE NULL ABSENT ON NULL)") ==
      "JSON_OBJECT(v VALUE NULL ABSENT ON NULL)")
  }

  test("ABSENT ON NULL round-trips through .sql keeping null members omitted") {
    // The ABSENT renderer arm must emit `ABSENT ON NULL` (not the default clause and not nothing),
    // so a canonical round trip keeps dropping null-valued members rather than retaining them.
    val absent = JsonObjectExpr(
      Seq((Literal("a"), Literal(1)), (Literal("b"), Literal.create(null, IntegerType))),
      Seq(false, false), Seq(false, false), JsonConstructorNullBehavior.Absent, StringType)
    assert(absent.sql == "JSON_OBJECT('a' VALUE 1, 'b' VALUE CAST(NULL AS INT) ABSENT ON NULL)")
    checkAnswer(sql(s"SELECT ${absent.sql}"), Row("""{"a":1}"""))
  }

  test("JSON_OBJECT is foldable only when it cannot throw") {
    assert(JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType).foldable)
    // Folding a possibly-null key would surface the null-key error at optimization even for rows a
    // filter/join drops.
    assert(!JsonObjectExpr(
      Seq((Literal.create(null, StringType), Literal(1))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType).foldable)
    assert(!JsonObjectExpr(
      Seq((Literal("k"), Literal("[1]"))), Seq(true), Seq(true),
      JsonConstructorNullBehavior.Null, StringType).foldable)
  }

  test("a constant JSON_OBJECT is accepted as a column DEFAULT") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, c STRING DEFAULT json_object('a' VALUE 1)) USING parquet")
      sql("INSERT INTO t (id) VALUES (1)")
      checkAnswer(sql("SELECT c FROM t"), Row("""{"a":1}"""))
    }
  }

  test("a non-foldable JSON_OBJECT projection participates in whole-stage codegen") {
    // Rendering through an Invoke of the object evaluator (not CodegenFallback) keeps the enclosing
    // Project eligible for whole-stage codegen; a CodegenFallback expression would exclude it. A
    // column-valued member keeps the constructor from being constant-folded away.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql("SELECT json_object('id' VALUE id) FROM range(5)")
      val fusedProjects = df.queryExecution.executedPlan.collect {
        case w: WholeStageCodegenExec => w
      }.flatMap(_.collect { case p: ProjectExec => p })
      assert(fusedProjects.nonEmpty,
        s"expected the JSON_OBJECT Project to be fused into whole-stage codegen, but got:\n" +
          df.queryExecution.executedPlan)
      checkAnswer(df, (0 until 5).map(i => Row(s"""{"id":$i}""")))
    }
  }

  test("a constant JSON_OBJECT is constant-folded") {
    val df = sql("SELECT json_object('a' VALUE 1) AS r FROM range(2)")
    assert(!df.queryExecution.optimizedPlan.exists(
      _.expressions.exists(_.exists(_.isInstanceOf[JsonObjectExpr]))))
    checkAnswer(df, Seq(Row("""{"a":1}"""), Row("""{"a":1}""")))
  }

  test("a null key raises JSON_OBJECT_NULL_KEY before the value is evaluated") {
    // The key is checked before the value is evaluated, so a null key wins deterministically even
    // when the value expression would itself throw.
    // `raise_error(k)` references the column so it is neither foldable nor evaluated before the
    // key null-check; if the value ran first the error would come from `raise_error`, not the key.
    val e = intercept[SparkRuntimeException] {
      sql("SELECT json_object(k VALUE raise_error(k)) " +
        "FROM VALUES (CAST(NULL AS STRING)) t(k)").collect()
    }
    assert(e.getCondition == "JSON_OBJECT_NULL_KEY")
    assert(e.getSqlState == "22004")
  }

  test("a foldable literal key is emitted on every row") {
    // A foldable non-null key must be written for every row, not dropped after the first.
    checkAnswer(
      sql("SELECT json_object('id' VALUE a) FROM VALUES (1), (2) t(a)"),
      Seq(Row("""{"id":1}"""), Row("""{"id":2}""")))
    // A foldable key that evaluates to null must still raise JSON_OBJECT_NULL_KEY per row rather
    // than being silently skipped.
    Seq("NULL", "CAST(NULL AS STRING)").foreach { k =>
      val e = intercept[SparkRuntimeException] {
        sql(s"SELECT json_object($k VALUE 1)").collect()
      }
      assert(e.getCondition == "JSON_OBJECT_NULL_KEY", s"for key $k")
      assert(e.getSqlState == "22004", s"for key $k")
    }
  }

  test("CHAR/VARCHAR RETURNING is normalized to STRING regardless of preserveCharVarcharTypeInfo") {
    Seq("CHAR(2)", "VARCHAR(2)").foreach { returning =>
      Seq("true", "false").foreach { preserve =>
        withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> preserve) {
          assert(
            sql(s"SELECT json_object('k' VALUE 1 RETURNING $returning)").schema.head.dataType
              === StringType,
            s"for RETURNING $returning, preserveCharVarcharTypeInfo=$preserve")
        }
      }
    }
  }

  test("object default collation applies only when RETURNING is not explicitly collated") {
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withTable("t") {
        sql(
          """CREATE TABLE t DEFAULT COLLATION UTF8_LCASE AS
            |SELECT json_object('k' VALUE 1) AS a,
            |  json_object('k' VALUE 1 RETURNING STRING COLLATE UTF8_BINARY) AS b""".stripMargin)
        val schema = spark.table("t").schema
        // Omitted RETURNING (default STRING) follows the table's default collation.
        assert(schema("a").dataType === StringType("UTF8_LCASE"))
        // Explicit RETURNING ... COLLATE is the user's choice and must not be overwritten.
        assert(schema("b").dataType === StringType("UTF8_BINARY"))
      }
    }
  }

  test("default collation recurses into a nested JSON_OBJECT value") {
    // The rule casts each DefaultStringProducingExpression, recursing through a nested constructor
    // (the flat cases above only cover a top-level constructor). This CTAS runs the default
    // analyzer (single-pass included). Confirm the schema collation and that raw splicing still
    // produces well-formed nested JSON at runtime.
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withTable("t") {
        sql(
          """CREATE TABLE t DEFAULT COLLATION UTF8_LCASE AS
            |SELECT json_object('a' VALUE json_object('b' VALUE 1)) AS a""".stripMargin)
        assert(spark.table("t").schema("a").dataType === StringType("UTF8_LCASE"))
        checkAnswer(spark.table("t"), Row("""{"a":{"b":1}}"""))
      }
    }
  }

  test("view default collation preserves an explicit collated RETURNING") {
    // Exercises the CREATE VIEW resolution path (in addition to the CTAS path above): the explicit
    // RETURNING collation must survive the view's default collation. Runs under dual-run (the
    // default) so the single-pass resolver's view re-resolution is exercised for parity.
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withView("v") {
        sql(
          """CREATE VIEW v DEFAULT COLLATION UTF8_LCASE AS
            |SELECT json_object('k' VALUE 1) AS a,
            |  json_object('k' VALUE 1 RETURNING STRING COLLATE UTF8_BINARY) AS b""".stripMargin)
        val schema = spark.table("v").schema
        assert(schema("a").dataType === StringType("UTF8_LCASE"))
        assert(schema("b").dataType === StringType("UTF8_BINARY"))
      }
    }
  }

  test("reports nullable to keep NullPropagation from skipping the null-key check") {
    // The value is never null, but a possibly-null key makes JSON_OBJECT throwable, so it must
    // report nullable: were it non-nullable, `NullPropagation` would fold `IS [NOT] NULL` and
    // `count(...)` away and skip the eval that must raise JSON_OBJECT_NULL_KEY (see below).
    assert(JsonObjectExpr(
      Seq((Literal.create(null, StringType), Literal(1))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType).nullable)
    assert(sql("SELECT json_object(k VALUE 1) FROM VALUES ('a'), (NULL) t(k)")
      .schema.head.nullable)
    // A non-null key with a null value cannot throw, so the result is non-nullable.
    assert(!JsonObjectExpr(
      Seq((Literal("k"), Literal.create(null, IntegerType))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType).nullable)
    assert(!sql("SELECT json_object('id' VALUE a) FROM VALUES (1), (2) t(a)")
      .schema.head.nullable)
  }

  test("IS NOT NULL and count still raise a null-key error") {
    // NullPropagation must not rewrite these to a constant / `count(1)` and skip evaluation, which
    // would swallow the null-key error on the null-key row.
    Seq(
      "SELECT json_object(k VALUE 1) IS NOT NULL FROM VALUES ('a'), (NULL) t(k)",
      "SELECT count(json_object(k VALUE 1)) FROM VALUES ('a'), (NULL) t(k)").foreach { query =>
      val e = intercept[SparkRuntimeException](sql(query).collect())
      assert(e.getCondition == "JSON_OBJECT_NULL_KEY")
    }
  }

  test("is throwable only when a key may be null or a value needs validation") {
    // A possibly-null key throws at runtime, so throwable must be true even when no child is
    // itself throwable.
    def obj(key: Expression, raw: Boolean = false): JsonObjectExpr = JsonObjectExpr(
      Seq((key, Literal("[1]"))), Seq(raw), Seq(raw), JsonConstructorNullBehavior.Null, StringType)
    assert(obj(Literal.create(null, StringType)).throwable)
    assert(obj(Literal("k"), raw = true).throwable)
    assert(!obj(Literal("k")).throwable)
  }

  test("throwable keeps a null-key predicate above a filtering join") {
    // The optimizer must not push a throwable predicate below the join (PushPredicateThroughJoin
    // only pushes non-throwable conditions), so a null-key row the join eliminates is never
    // evaluated and does not throw. Were the constructor not throwable, the predicate would push to
    // the probe side and throw on the eliminated row. The predicate is a throwable comparison
    // (`json_object(...) = literal`) that exercises the pushdown path.
    withTempView("t", "u") {
      Seq((1, "k1"), (2, null)).toDF("id", "k").createOrReplaceTempView("t")
      Seq(1).toDF("id").createOrReplaceTempView("u")
      // id=2 has a null key but does not join u, so it is dropped first (the predicate stays above
      // the join, so the constructor is never evaluated on the eliminated row).
      checkAnswer(
        sql("""SELECT t.id FROM t JOIN u ON t.id = u.id
              |WHERE json_object(t.k VALUE 1) = '{"k1":1}'""".stripMargin),
        Row(1))
      // With the null-key row surviving the join, evaluation still throws.
      Seq(2).toDF("id").createOrReplaceTempView("u")
      val e = intercept[SparkRuntimeException] {
        sql("""SELECT t.id FROM t JOIN u ON t.id = u.id
              |WHERE json_object(t.k VALUE 1) = '{"k1":1}'""".stripMargin).collect()
      }
      assert(e.getCondition == "JSON_OBJECT_NULL_KEY")
    }
  }

  test("a non-throwing JSON_OBJECT predicate is pushed below a join") {
    // Range-based views keep the filter from being folded into a local relation.
    withTempView("t", "u") {
      spark.range(3).selectExpr("id", "id AS x").createOrReplaceTempView("t")
      spark.range(2).createOrReplaceTempView("u")
      val df = sql("""SELECT t.id FROM t JOIN u ON t.id = u.id
                     |WHERE json_object('a' VALUE t.x) = '{"a":1}'""".stripMargin)
      val join = df.queryExecution.optimizedPlan.collectFirst { case j: Join => j }
        .getOrElse(fail("expected a Join in the optimized plan"))
      assert(join.exists {
        case f: Filter => f.condition.exists(_.isInstanceOf[JsonObjectExpr])
        case _ => false
      })
      checkAnswer(df, Row(1))
    }
  }

  test("raw splicing is decided from the source, not the optimized plan shape") {
    // A directly-nested JSON_OBJECT carries implicit FORMAT JSON and is spliced raw.
    checkAnswer(
      sql("SELECT json_object('a' VALUE json_object('b' VALUE 1))"),
      Row("""{"a":{"b":1}}"""))
    // But a JSON_OBJECT result surfaced as a column is a plain STRING and must stay quoted -- even
    // though CollapseProject may inline the inner JSON_OBJECT into the outer value position. The
    // decision is frozen from the lexical argument at parse time, so the result stays
    // {"a":"{\"b\":1}"}, never {"a":{"b":1}}.
    val inlined =
      sql("SELECT json_object('a' VALUE o) AS r FROM (SELECT json_object('b' VALUE 1) AS o) t")
    checkAnswer(inlined, Row("""{"a":"{\"b\":1}"}"""))
    // Referencing the alias twice blocks CollapseProject from inlining it; the result is identical,
    // confirming independence from plan shape.
    val notInlined =
      sql("SELECT json_object('a' VALUE o) AS r, o FROM (SELECT json_object('b' VALUE 1) AS o) t")
    checkAnswer(notInlined, Row("""{"a":"{\"b\":1}"}""", """{"b":1}"""))
  }

  test("COLLATE on a nested JSON_OBJECT value still splices it raw") {
    // COLLATE is a pass-through wrapper (collation metadata only), so a nested constructor under
    // COLLATE is still spliced raw -- a no-op-looking annotation must not flip it to a quoted
    // string, and the COLLATE form must agree with the bare form.
    Seq("UTF8_BINARY", "UTF8_LCASE").foreach { collation =>
      checkAnswer(
        sql(s"SELECT json_object('a' VALUE json_object('b' VALUE 1) COLLATE $collation)"),
        Row("""{"a":{"b":1}}"""))
    }
    checkAnswer(
      sql("SELECT json_object('a' VALUE json_object('b' VALUE 1) COLLATE UTF8_BINARY)"),
      sql("SELECT json_object('a' VALUE json_object('b' VALUE 1))"))
  }

  test("explicit CAST(... AS STRING) still cancels raw splicing") {
    // In contrast to COLLATE, an explicit CAST(... AS STRING) is the documented way to quote a
    // nested constructor rather than splice it raw.
    checkAnswer(
      sql("SELECT json_object('a' VALUE CAST(json_object('b' VALUE 1) AS STRING))"),
      Row("""{"a":"{\"b\":1}"}"""))
  }

  test("explicit FORMAT JSON splices a string value raw; a plain string is quoted") {
    checkAnswer(
      sql("""SELECT json_object('a' VALUE '{"b":1}' FORMAT JSON)"""), Row("""{"a":{"b":1}}"""))
    checkAnswer(
      sql("SELECT json_object('a' VALUE '[1,2]' FORMAT JSON)"), Row("""{"a":[1,2]}"""))
    // FORMAT JSON applies to the colon form too, not just VALUE.
    checkAnswer(
      sql("""SELECT json_object('a' : '{"b":1}' FORMAT JSON)"""), Row("""{"a":{"b":1}}"""))
    // Without FORMAT JSON the same string is quoted, even though its contents are valid JSON.
    checkAnswer(
      sql("""SELECT json_object('a' VALUE '{"b":1}')"""), Row("""{"a":"{\"b\":1}"}"""))
  }

  test("per-member rawJson and validation flags stay tied to their own member") {
    // Ordered object mixing a quoted string (rawJson=false), a trusted nested producer
    // (rawJson=true), and an explicit FORMAT JSON string (rawJson=true, validated). Each member
    // must render from its own flag, not a neighbour's.
    checkAnswer(
      sql("""SELECT json_object(
            |  'a' VALUE '{"x":1}',
            |  'b' VALUE json_object('c' VALUE 1),
            |  'd' VALUE '[1,2]' FORMAT JSON)""".stripMargin),
      Row("""{"a":"{\"x\":1}","b":{"c":1},"d":[1,2]}"""))
    // A later malformed explicit value: validation must fire for that member (flattened arg 8), not
    // be masked by the trusted/quoted earlier members.
    val e = intercept[SparkRuntimeException] {
      sql("""SELECT json_object(
            |  'a' VALUE '{"x":1}',
            |  'b' VALUE json_object('c' VALUE 1),
            |  'd' VALUE '[1,2]' FORMAT JSON,
            |  'e' VALUE '{bad' FORMAT JSON)""".stripMargin).collect()
    }
    assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE")
    assert(e.getMessageParameters.get("position") == "8")
  }

  test("a malformed FORMAT JSON value raises an error at runtime") {
    Seq("'{bad'", "'1,2'", "''").foreach { value =>
      val e = intercept[SparkRuntimeException] {
        sql(s"SELECT json_object('a' VALUE $value FORMAT JSON)").collect()
      }
      assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE", s"for $value")
    }
    // The reported position is the flattened value-argument position, not the member ordinal: the
    // bad value below is the 4th argument (`a`, 1, `b`, `{bad`), not the 2nd member.
    val runtimeErr = intercept[SparkRuntimeException] {
      sql("SELECT json_object('a' VALUE 1, 'b' VALUE '{bad' FORMAT JSON)").collect()
    }
    assert(runtimeErr.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE")
    assert(runtimeErr.getMessageParameters.get("position") == "4")
  }

  test("malformed FORMAT JSON is validated per row, so a filtered-out row does not raise") {
    // FORMAT JSON validation is throwable, so the constructor is not constant-folded and runs per
    // row at eval time: a row the WHERE drops is never validated and must not raise.
    val values = "FROM VALUES ('ok', '[1]'), ('bad', '{oops') AS t(id, j)"
    checkAnswer(
      sql(s"SELECT json_object('a' VALUE j FORMAT JSON) $values WHERE id = 'ok'"),
      Row("""{"a":[1]}"""))
    val e = intercept[SparkRuntimeException] {
      sql(s"SELECT json_object('a' VALUE j FORMAT JSON) $values").collect()
    }
    assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE")
  }

  test("FORMAT JSON on a non-string value is rejected at analysis") {
    val e = intercept[AnalysisException] {
      sql("SELECT json_object('a' VALUE 123 FORMAT JSON)")
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_FORMAT_JSON_INPUT")
    // The reported position is the flattened value-argument position, not the member ordinal: the
    // bad value below is the 4th argument (`a`, 1, `b`, 123), not the 2nd member.
    val laterMemberErr = intercept[AnalysisException] {
      sql("SELECT json_object('a' VALUE 1, 'b' VALUE 123 FORMAT JSON)")
    }
    assert(laterMemberErr.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_FORMAT_JSON_INPUT")
    assert(laterMemberErr.getMessageParameters.get("position") == "4")
  }

  test("a NULL FORMAT JSON value follows ON NULL like any other null") {
    checkAnswer(sql("SELECT json_object('a' VALUE NULL FORMAT JSON)"), Row("""{"a":null}"""))
    checkAnswer(
      sql("SELECT json_object('a' VALUE NULL FORMAT JSON ABSENT ON NULL)"), Row("{}"))
  }

  test("a per-row (non-foldable) FORMAT JSON value is validated for each row") {
    // A column-valued FORMAT JSON is non-foldable, so it exercises the per-row validation branch
    // (not the cached foldable branch).
    checkAnswer(
      Seq("""{"b":1}""").toDF("j").selectExpr("json_object('a' VALUE j FORMAT JSON)"),
      Row("""{"a":{"b":1}}"""))
    val e = intercept[SparkRuntimeException] {
      Seq("bad").toDF("j").selectExpr("json_object('a' VALUE j FORMAT JSON)").collect()
    }
    assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE")
  }

  test("a caught FORMAT JSON error does not corrupt later rows in the same task") {
    // The evaluator validates every raw value before writing to its per-task generator, so a
    // malformed row that a `try_*` wrapper swallows must leave the generator clean: later rows
    // reusing the same evaluator must render exactly as they would on their own. `decode(..utf-8)`
    // round-trips the bytes back to the JSON string so the per-row result is checkable.
    // Force one task so the valid rows after `'{bad'` share its evaluator -- otherwise the rows
    // split across tasks and `'{bad'` is the last row its evaluator sees, hiding the corruption.
    val query =
      "SELECT decode(try_to_binary(json_object('a' VALUE s FORMAT JSON), 'utf-8'), 'utf-8') " +
        "FROM VALUES ('1'), ('{bad'), ('2'), ('3') AS t(s)"
    val expected = Seq(Row("""{"a":1}"""), Row(null), Row("""{"a":2}"""), Row("""{"a":3}"""))
    val modes = Seq(CodegenObjectFactoryMode.NO_CODEGEN, CodegenObjectFactoryMode.CODEGEN_ONLY)
    modes.foreach { mode =>
      withSQLConf(
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
        SQLConf.CODEGEN_FACTORY_MODE.key -> mode.toString,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key ->
          (mode == CodegenObjectFactoryMode.CODEGEN_ONLY).toString) {
        checkAnswer(sql(query), expected)
      }
    }
  }

  test("each foldable FORMAT JSON member splices its own validated text across rows") {
    // Two constant FORMAT JSON members at different positions share the validated-text cache. A
    // column-valued member keeps the object from being constant-folded, so rows after the first
    // read the cache: a fill or read at the wrong index would splice the other member's text.
    // Force one task so a single evaluator sees both rows and the second takes the cache-read path.
    withSQLConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1") {
      checkAnswer(
        sql("SELECT json_object('a' VALUE '[1]' FORMAT JSON, 'b' VALUE x, " +
          "'c' VALUE '{\"d\":2}' FORMAT JSON) FROM VALUES (1), (2) t(x)"),
        Seq(Row("""{"a":[1],"b":1,"c":{"d":2}}"""), Row("""{"a":[1],"b":2,"c":{"d":2}}""")))
    }
  }

  test("a constant-folded raw nested value round-trips through .sql as raw") {
    // ConstantFolding rewrites the nested json_array(1) to the literal '[1]', but rawJson stays
    // frozen true. .sql must emit FORMAT JSON so reparse splices it raw rather than quoting it. The
    // column-valued member keeps the outer constructor from folding.
    val from = "FROM VALUES (1) t(x)"
    val jsonObj = sql(s"SELECT json_object('a' VALUE json_array(1), 'b' VALUE x) AS r $from")
      .queryExecution.optimizedPlan.expressions
      .flatMap(_.collect { case j: JsonObjectExpr => j }).head
    assert(jsonObj.children(1).isInstanceOf[Literal],
      "the nested constructor should have been constant-folded to a literal")
    assert(jsonObj.sql.contains("FORMAT JSON"))
    checkAnswer(sql(s"SELECT ${jsonObj.sql} AS r $from"), Row("""{"a":[1],"b":1}"""))
  }

  test("canonical .sql reparses to the built-in even under a shadowing routine") {
    // .sql renders an explicit ON NULL clause, forcing the direct grammar path, so a resolved
    // built-in round-trips to the built-in even when a same-named routine is on the path.
    val objSql = JsonObjectExpr(
      Seq((Literal("id"), Literal(7))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType).sql
    assert(objSql.contains("NULL ON NULL"))
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b INT) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        // The clause-free call is shadowed, but the canonical .sql (with ON NULL) is not.
        checkAnswer(sql("SELECT json_object('id', 7)"), Row("shadowed"))
        checkAnswer(sql(s"SELECT $objSql"), Row("""{"id":7}"""))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("SQL renders a COLLATE-wrapped raw nested value as its bare constructor") {
    // A raw value behind a pass-through Collate renders as the bare inner constructor so reparse
    // re-derives raw splicing (the collation does not affect the spliced-raw bytes).
    val inner = JsonObjectExpr(
      Seq((Literal("b"), Literal(1))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType)
    val wrapped = JsonObjectExpr(
      Seq((Literal("a"), Collate(inner, Literal("UTF8_BINARY")))), Seq(true), Seq(false),
      JsonConstructorNullBehavior.Null, StringType)
    assert(wrapped.sql ==
      "JSON_OBJECT('a' VALUE JSON_OBJECT('b' VALUE 1 NULL ON NULL) NULL ON NULL)")
    checkAnswer(sql(s"SELECT ${wrapped.sql}"), Row("""{"a":{"b":1}}"""))
  }

  test("plain call goes through routine resolution and can be shadowed via SET PATH") {
    // `withUserDefinedFunction` is unusable here: its cleanup asserts the name no longer resolves,
    // but `json_object` is now a registered built-in, so drop the temporary routine explicitly.
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b INT) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        // A plain call is an ordinary function call, so the temporary routine (ahead of
        // system.builtin on the path) shadows the built-in constructor.
        checkAnswer(sql("SELECT json_object('id', 7)"), Row("shadowed"))
        checkAnswer(sql("SELECT json_object('id' VALUE 7)"), Row("shadowed"))
        // The clause-free colon spelling routes through resolution too, so it is shadowable.
        checkAnswer(sql("SELECT json_object('id' : 7)"), Row("shadowed"))
        checkAnswer(sql("SELECT json_object(*) FROM VALUES ('id', 7) AS t(a, b)"), Row("shadowed"))
        // A clause-bearing form is not a function call, so it stays the built-in constructor.
        checkAnswer(sql("SELECT json_object('id' VALUE 7 RETURNING STRING)"), Row("""{"id":7}"""))
        // A clause-bearing colon form likewise stays the built-in constructor.
        checkAnswer(sql("SELECT json_object('id' : 7 RETURNING STRING)"), Row("""{"id":7}"""))
        checkAnswer(sql("SELECT json_object('id' VALUE NULL ABSENT ON NULL)"), Row("{}"))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("explicit FORMAT JSON stays stock-owned under a compatible shadow") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b STRING) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        checkAnswer(sql("SELECT json_object('id' VALUE '[1]')"), Row("shadowed"))
        checkAnswer(
          sql("SELECT json_object('id' VALUE '[1]' FORMAT JSON)"),
          Row("""{"id":[1]}"""))
        checkAnswer(
          sql("SELECT json_object('id' : '[1]' FORMAT JSON)"),
          Row("""{"id":[1]}"""))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("a comma-form nested JSON producer is spliced raw when unshadowed") {
    // The comma form routes through resolution, but the unshadowed built-in reconstruction still
    // splices a lexically nested constructor raw, matching VALUE.
    checkAnswer(sql("SELECT json_object('a', json_object('b', 1))"), Row("""{"a":{"b":1}}"""))
    checkAnswer(sql("SELECT json_object('a', json_array(1, 2))"), Row("""{"a":[1,2]}"""))
    checkAnswer(
      sql("SELECT json_object('a', json_object('b', 1))"),
      sql("SELECT json_object('a' VALUE json_object('b' VALUE 1))"))
    // A pass-through COLLATE still splices raw; a plain string and an explicit CAST stay quoted.
    checkAnswer(
      sql("SELECT json_object('a', json_object('b', 1) COLLATE UTF8_LCASE)"),
      Row("""{"a":{"b":1}}"""))
    checkAnswer(sql("""SELECT json_object('a', '{"b":1}')"""), Row("""{"a":"{\"b\":1}"}"""))
    checkAnswer(
      sql("SELECT json_object('a', CAST(json_object('b', 1) AS STRING))"),
      Row("""{"a":"{\"b\":1}"}"""))
    // JSON_QUERY is mode-sensitive: KEEP QUOTES splices raw JSON text, OMIT QUOTES returns a
    // decoded (quoted) string. Verify the comma form preserves the distinction, as the VALUE form.
    checkAnswer(
      sql("""SELECT json_object('a', json_query('{"o":{"x":1}}', '$.o'))"""),
      Row("""{"a":{"x":1}}"""))
    checkAnswer(
      sql("""SELECT json_object('a', json_query('{"n":"Ada"}', '$.n' OMIT QUOTES))"""),
      Row("""{"a":"Ada"}"""))
  }

  test("JSON_OBJECT ties each member's raw flag to its own position") {
    // Mix a quoted ordinary string, a raw nested object, and a raw nested array in distinct
    // positions so a grouping/indexing slip would splice or quote a neighbouring member.
    checkAnswer(
      sql("""SELECT json_object('a', '{"x":1}', 'b', json_object('c', 1), 'd', json_array(1))"""),
      Row("""{"a":"{\"x\":1}","b":{"c":1},"d":[1]}"""))
  }

  test("a compatible routine shadows a clause-free call even with a nested producer value") {
    // Argument shape must not remove routine candidates: the routed wrapper hands off plain
    // arguments to function resolution, so a compatible (STRING, STRING) routine shadows the
    // nested-producer call across clause-free comma, VALUE, and colon spellings.
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b STRING) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        checkAnswer(sql("SELECT json_object('a', 'x')"), Row("shadowed"))
        checkAnswer(sql("SELECT json_object('a', json_object('b', 1))"), Row("shadowed"))
        checkAnswer(sql("SELECT json_object('a', json_array(1))"), Row("shadowed"))
        checkAnswer(sql("SELECT json_object('a' VALUE json_object('b' VALUE 1))"), Row("shadowed"))
        checkAnswer(sql("SELECT json_object('a': json_object('b': 1))"), Row("shadowed"))
        checkAnswer(sql("SELECT json_object('a' VALUE builtin.json_object('b', 1))"),
          Row("shadowed"))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("SPARK-59243: a persistent function cannot reference a temp shadow via a nested call") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b STRING) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        // Every routed spelling is a reference to the temp shadow, like the flat call.
        Seq(
          "json_object('a', 'x')",
          "json_object('a', json_object('b', 1))",
          "json_object('a' VALUE json_object('b' VALUE 1))",
          "json_object('a': json_object('b': 1))"
        ).foreach { body =>
          val e = intercept[AnalysisException] {
            sql(s"CREATE FUNCTION perm_json_fn() RETURNS STRING RETURN $body")
          }
          assert(e.getCondition == "INVALID_TEMP_OBJ_REFERENCE", s"for $body")
        }
        // Stock-owned (clause-bearing or FORMAT JSON) bodies do not reference the temp shadow.
        Seq(
          "json_object('a' VALUE 1 NULL ON NULL)" -> """{"a":1}""",
          "json_object('a' VALUE json_object('b' VALUE 1) ABSENT ON NULL)" -> """{"a":{"b":1}}""",
          "json_object('id' VALUE '[1]' FORMAT JSON)" -> """{"id":[1]}"""
        ).foreach { case (body, expected) =>
          sql(s"CREATE OR REPLACE FUNCTION perm_json_fn() RETURNS STRING RETURN $body")
          checkAnswer(sql("SELECT spark_catalog.default.perm_json_fn()"), Row(expected))
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP FUNCTION IF EXISTS perm_json_fn")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("SPARK-59243: a routed nested call is checked for cyclic SQL function references") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b STRING) RETURNS STRING " +
          "RETURN 'x'")
        sql("SET PATH = system.session, system.builtin")
        // Every routed spelling references the same-named function, like the flat call.
        Seq(
          "json_object(a, b)",
          "json_object('a', json_object('b', 1))",
          "json_object('a' VALUE json_object('b' VALUE 1))",
          "json_object('a': json_object('b': 1))"
        ).foreach { body =>
          val e = intercept[AnalysisException] {
            sql("CREATE OR REPLACE TEMPORARY FUNCTION json_object(a STRING, b STRING) " +
              s"RETURNS STRING RETURN $body")
          }
          assert(e.getCondition == "CYCLIC_FUNCTION_REFERENCE", s"for $body")
        }
        // Stock-owned (clause-bearing or FORMAT JSON) bodies are not routine calls.
        Seq(
          "json_object('a' VALUE json_object('b' VALUE 1) NULL ON NULL)" ->
            """{"a":{"b":1}}""",
          "json_object('id' VALUE '[1]' FORMAT JSON)" -> """{"id":[1]}"""
        ).foreach { case (body, expected) =>
          sql("CREATE OR REPLACE TEMPORARY FUNCTION json_object(a STRING, b STRING) " +
            s"RETURNS STRING RETURN $body")
          checkAnswer(sql("SELECT json_object('p', 'q')"), Row(expected))
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("a shadowing routine receives the routed nested producer's JSON text") {
    // The routine returns its value argument, so this observes what the shadow receives.
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b STRING) RETURNS STRING RETURN b")
        sql("SET PATH = system.session, system.builtin")
        checkAnswer(sql("SELECT json_object('a', json_object('b', 1))"), Row("""{"b":1}"""))
        checkAnswer(sql("SELECT json_object('a', json_array(1))"), Row("[1]"))
        checkAnswer(
          sql("SELECT json_object('a' VALUE json_object('b' VALUE 1))"),
          Row("""{"b":1}"""))
        checkAnswer(sql("SELECT json_object('a' VALUE builtin.json_array(1))"), Row("[1]"))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("a nested JSON_OBJECT in key position routes through resolution and can be shadowed") {
    // Keys are never spliced raw, so a nested constructor in key position stays routable (unlike a
    // value-position one) and a shadowing routine applies to it. A RETURNING clause keeps the outer
    // call a direct constructor, so only the inner call's routing is exercised.
    val query = "SELECT json_object(json_object('b', 1) VALUE 2 RETURNING STRING)"
    // Absent a shadowing routine the inner call resolves to the built-in, so the key is its JSON.
    checkAnswer(sql(query), Row("""{"{\"b\":1}":2}"""))
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b INT) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        checkAnswer(sql(query), Row("""{"shadowed":2}"""))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("qualified plain JSON_OBJECT resolves to the built-in constructor") {
    checkAnswer(sql("SELECT builtin.json_object('id', 7)"), Row("""{"id":7}"""))
    checkAnswer(sql("SELECT system.builtin.json_object('id', 7)"), Row("""{"id":7}"""))
    checkAnswer(sql("SELECT builtin.json_object()"), Row("{}"))
  }

  test("SPARK-59243: a nested JSON constructor through a routed JSON_OBJECT call is spliced") {
    // A routed or qualified built-in JSON_OBJECT splices a lexically-nested JSON producer value,
    // matching the direct comma form. The built-in builder derives the splice from the resolved
    // value via `ImplicitlyFormattedAsJson.splicesThroughCollationCast`, so a qualified outer call
    // and an unqualified call over a qualified nested constructor both splice.
    checkAnswer(
      sql("SELECT builtin.json_object('a', json_object('b', 1))"),
      Row("""{"a":{"b":1}}"""))
    checkAnswer(
      sql("SELECT system.builtin.json_object('a', json_object('b', 1))"),
      Row("""{"a":{"b":1}}"""))
    checkAnswer(
      sql("SELECT json_object('a', builtin.json_object('b', 1))"),
      Row("""{"a":{"b":1}}"""))
    checkAnswer(
      sql("SELECT builtin.json_object('a', json_array(1))"),
      Row("""{"a":[1]}"""))
    checkAnswer(
      sql("""SELECT builtin.json_object('a', json_query('{"x":{"y":1}}', '$.x'))"""),
      Row("""{"a":{"y":1}}"""))
    // A user CAST still detaches FORMAT JSON, so the value stays quoted (the routed splice check
    // sees through an analyzer collation Cast, but not a user `CAST(... AS STRING)`).
    checkAnswer(
      sql("SELECT builtin.json_object('a', CAST(json_object('b', 1) AS STRING))"),
      Row("""{"a":"{\"b\":1}"}"""))
  }

  test("SPARK-59243: VALUE/colon JSON_OBJECT splices qualified nested constructors") {
    checkAnswer(
      sql("SELECT json_object('a' VALUE json_object('b' VALUE 1), " +
        "'c' VALUE builtin.json_object('d', 2))"),
      Row("""{"a":{"b":1},"c":{"d":2}}"""))
    checkAnswer(
      sql("SELECT json_object('a': json_object('b': 1), " +
        "'c': builtin.json_object('d', 2))"),
      Row("""{"a":{"b":1},"c":{"d":2}}"""))

    Seq(false, true).foreach { singlePass =>
      withSQLConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString) {
        val analyzed =
          sql("SELECT json_object('a' VALUE builtin.json_object('b', 1) FORMAT JSON)")
            .queryExecution.analyzed
        val outer = analyzed.expressions
          .flatMap(_.collect { case j: JsonObjectExpr => j })
          .find(_.rawJson == Seq(true))
          .get
        assert(!outer.needsValidation.head, s"singlePass=$singlePass")
      }
    }
  }

  test("SPARK-59243: routed/qualified JSON_OBJECT splices under a default collation") {
    // The qualified call resolves through the built-in builder; `ApplyDefaultCollation` wraps the
    // nested producer in a collation Cast, which the builder must see through (an analyzer Cast,
    // not a user one) and splice, matching the unqualified comma form rather than quoting.
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withTable("t") {
        sql(
          """CREATE TABLE t DEFAULT COLLATION UTF8_LCASE AS
            |SELECT builtin.json_object('a', json_object('b', 1)) AS q,
            |       json_object('a', json_object('b', 1)) AS u,
            |       builtin.json_object('a', json_object('b', 1) COLLATE UNICODE) AS c
            |""".stripMargin)
        val expected = """{"a":{"b":1}}"""
        checkAnswer(spark.table("t"), Row(expected, expected, expected))
      }
    }
  }

  test("SPARK-59243: routed/qualified JSON_OBJECT splices under a view default collation") {
    // Reading the view re-analyzes it (under dual-run in tests), so both analyzers must see
    // through their own default-collation casts, including one under a user COLLATE.
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withView("v") {
        sql(
          """CREATE VIEW v DEFAULT COLLATION UTF8_LCASE AS
            |SELECT builtin.json_object('a', json_object('b', 1)) AS q,
            |       json_object('a', json_object('b', 1)) AS u,
            |       json_object('a' VALUE builtin.json_object('b', 1)) AS v,
            |       builtin.json_object('a', json_object('b', 1) COLLATE UNICODE) AS c
            |""".stripMargin)
        val expected = """{"a":{"b":1}}"""
        checkAnswer(spark.table("v"), Row(expected, expected, expected, expected))
        val schema = spark.table("v").schema
        Seq("q", "u", "v").foreach { c =>
          assert(schema(c).dataType === StringType("UTF8_LCASE"), s"for $c")
        }
      }
    }
  }

  test("SPARK-59243: a shadowed nested call keeps the owner's type under a default collation") {
    // A routed wrapper stands for an ordinary call until its owner is known, so the default
    // collation must not cast a shadow's result: each nested spelling matches the flat call.
    withSQLConf(
      SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true",
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      Seq("INT" -> "7" -> 7, "STRING" -> "'shadowed'" -> "shadowed").foreach {
        case ((returnType, body), value) =>
          try {
            sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b STRING) " +
              s"RETURNS $returnType RETURN $body")
            sql("SET PATH = system.session, system.builtin")
            withTable("t") {
              sql(
                """CREATE TABLE t DEFAULT COLLATION UTF8_LCASE AS
                  |SELECT json_object('a', json_object('b', 1)) AS comma,
                  |       json_object('a' VALUE json_object('b' VALUE 1)) AS value,
                  |       json_object('a': json_object('b': 1)) AS colon,
                  |       json_object('a', 'x') AS flat""".stripMargin)
              // Read back on the default path, which reaches the current schema.
              sql("SET PATH = DEFAULT_PATH")
              val schema = spark.table("t").schema
              Seq("comma", "value", "colon").foreach { c =>
                assert(schema(c).dataType === schema("flat").dataType, s"$c for $returnType")
              }
              checkAnswer(spark.table("t"), Row(value, value, value, value))
            }
          } finally {
            sql("SET PATH = DEFAULT_PATH")
            sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
          }
      }
    }
  }

  test("SPARK-59243: a string() cast detaches a nested producer with an explicit collation") {
    // The untagged `string(...)` cast changes the collation here, but it is a user cast, not a
    // default-collation recoloring, so the nested object is quoted on every path.
    val nested = "string(json_object('b', 1 RETURNING STRING COLLATE UTF8_LCASE))"
    val quoted = Row("""{"a":"{\"b\":1}"}""")
    checkAnswer(sql(s"SELECT builtin.json_object('a', $nested)"), quoted)
    checkAnswer(sql(s"SELECT json_object('a', $nested)"), quoted)
    checkAnswer(sql(s"SELECT json_object('a' VALUE $nested NULL ON NULL)"), quoted)
  }

  test("SPARK-59243: routed JSON_OBJECT splice round-trips through canonical SQL") {
    // The analyzed expression's canonical SQL must reparse to the same spliced result.
    Seq(
      "SELECT builtin.json_object('a', json_object('b', 1))",
      "SELECT json_object('a', builtin.json_object('b', 1))").foreach { query =>
      val analyzed = sql(query).queryExecution.analyzed
      val jsonObject = analyzed.expressions.flatMap(_.collect { case j: JsonObjectExpr => j }).head
      checkAnswer(sql(s"SELECT ${jsonObject.sql}"), Row("""{"a":{"b":1}}"""))
    }
  }

  test("SPARK-59243: VALUE/colon JSON_OBJECT in a lambda preserves qualified splicing") {
    checkAnswer(
      sql("SELECT transform(array(1), x -> json_object('a' VALUE json_object('b' VALUE x), " +
        "'c' VALUE builtin.json_object('d', x)))"),
      Row(Seq("""{"a":{"b":1},"c":{"d":1}}""")))
    checkAnswer(
      sql("SELECT transform(array(1), x -> json_object('a': json_object('b': x), " +
        "'c': builtin.json_object('d', x)))"),
      Row(Seq("""{"a":{"b":1},"c":{"d":1}}""")))
  }

  test("an odd number of arguments through a qualified call is rejected by the builder") {
    Seq("builtin.json_object", "system.builtin.json_object").foreach { func =>
      val e = intercept[AnalysisException](sql(s"SELECT $func('a', 1, 'b')"))
      assert(e.getCondition == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION", s"for $func")
    }
  }

  test("invalid: a bare star argument in plain JSON_OBJECT is not expanded") {
    Seq("json_object", "builtin.json_object", "system.builtin.json_object").foreach { func =>
      val e = intercept[AnalysisException] {
        sql(s"SELECT $func(*) FROM VALUES ('id', 7) AS t(a, b)").collect()
      }
      assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"for $func(*)")
    }
  }

  test("invalid: a direct star in VALUE/colon JSON_OBJECT forms is not expanded") {
    // Clause-free VALUE / colon forms route through function resolution, but the stock built-in
    // still rejects a direct member value `*`; only a shadowing routine may receive it expanded.
    Seq(
      "SELECT json_object('a' VALUE *, 'b' VALUE json_object('c' VALUE 1)) " +
        "FROM VALUES ('id', 7) AS t(a, b)",
      "SELECT json_object('a': *, 'b': json_object('c': 1)) " +
        "FROM VALUES ('id', 7) AS t(a, b)"
    ).foreach { query =>
      Seq(false, true).foreach { singlePass =>
        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString) {
          val e = intercept[AnalysisException] {
            sql(query).queryExecution.analyzed
          }
          assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX",
            s"singlePass=$singlePass for $query")
        }
      }
    }
  }

  test("VALUE/colon JSON_OBJECT star expands for a shadow owner") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(k1 STRING, v1 STRING, v2 INT, " +
          "k2 STRING, v3 STRING) RETURNS STRING RETURN 'shadowed-5'")
        sql("SET PATH = system.session, system.builtin")
        Seq(
          "SELECT json_object('a' VALUE *, 'b' VALUE json_object('c' VALUE 1)) " +
            "FROM VALUES ('id', 7) AS t(a, b)",
          "SELECT json_object('a': *, 'b': json_object('c': 1)) " +
            "FROM VALUES ('id', 7) AS t(a, b)"
        ).foreach { query =>
          checkAnswer(sql(query), Row("shadowed-5"))
          // Strict single-pass expands the nested star and routes to the shadow owner too, but the
          // owner is a SQL function the single-pass analyzer does not yet resolve: reaching it
          // (past star expansion and routing) then aborts with ExplicitlyUnsupportedResolverFeature
          // on the SQL function body. Guard-gated modes fall back to the fixed-point result above.
          withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
            intercept[ExplicitlyUnsupportedResolverFeature] {
              sql(query).queryExecution.analyzed
            }
          }
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("json_object is registered as a built-in function") {
    assert(spark.sessionState.catalog.isBuiltinFunction("json_object"))
  }

  test("single-pass: routed JSON_OBJECT resolves and expands a nested star") {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      Seq(
        "SELECT json_object('id', 7)",
        "SELECT json_object('vals', array(*)) FROM VALUES (1, 2) AS t(a, b)",
        // A routed nested producer keeps its raw-splice flag in UnresolvedRoutedJsonObject until
        // the stock built-in path finalizes it.
        "SELECT json_object('a', json_object('b', 1))",
        "SELECT json_object('a' VALUE json_object('b' VALUE 1), " +
          "'c' VALUE builtin.json_object('d', 2))"
      ).foreach { query =>
        // Analyze only: the single-pass analyzer cannot yet run every operator the action path
        // needs, so assert JSON_OBJECT resolves (via the ResolverGuard allowlist) and neither a
        // star nor an unresolved wrapper survives, rather than executing it.
        val analyzed = sql(query).queryExecution.analyzed
        assert(analyzed.resolved, s"for $query")
        assert(!analyzed.exists(_.expressions.exists(_.exists(_.isInstanceOf[Star]))),
          s"star should not survive analysis for $query")
        assert(!analyzed.exists(_.expressions.exists(_.exists(
          _.isInstanceOf[UnresolvedJsonConstructor]))),
          s"wrapper should not survive analysis for $query")
        // The wrapper must preserve rawJson = true, not merely disappear: otherwise the nested
        // object would be quoted instead of spliced raw.
        if (query.contains("json_object('a', json_object('b', 1))")) {
          val objs = analyzed.expressions.flatMap(_.collect { case j: JsonObjectExpr => j })
          val outer = objs.filter(_.members.exists { case (_, v) =>
            v.exists(_.isInstanceOf[JsonObjectExpr])
          })
          assert(outer.map(_.rawJson) == Seq(Seq(true)),
            s"nested producer must be marked raw-spliced for $query")
        }
        if (query.contains("'c' VALUE builtin.json_object")) {
          val objs = analyzed.expressions.flatMap(_.collect { case j: JsonObjectExpr => j })
          val outer = objs.find(_.members.length == 2).get
          assert(outer.rawJson == Seq(true, true),
            s"qualified VALUE member must be marked raw-spliced for $query")
        }
      }
    }
  }

  test("single-pass: a direct star in stock JSON_OBJECT is rejected, not expanded") {
    // The single-pass resolver reaches a distinct branch (RejectStockBuiltin) for a direct
    // `json_object(*)` than the routed/nested-star cases above. Guard it explicitly: if that branch
    // ever expanded the star like an ordinary routine, the default-analyzer rejection would still
    // pass while the forbidden call silently became valid only under single-pass.
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      Seq("json_object", "builtin.json_object", "system.builtin.json_object").foreach { func =>
        val e = intercept[AnalysisException] {
          sql(s"SELECT $func(*) FROM VALUES ('id', 7) AS t(a, b)").queryExecution.analyzed
        }
        assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"for $func(*)")
      }
    }
  }
}
