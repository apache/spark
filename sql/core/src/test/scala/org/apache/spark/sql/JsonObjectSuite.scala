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
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Cast, Collate, JsonConstructorNullBehavior, JsonImplicitFormatCarrier, JsonObjectExpr, Literal}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{CharType, GeometryType, IntegerType, MapType, StringType, VarcharType}

/**
 * End-to-end tests for the SQL:2016 `JSON_OBJECT` constructor function.
 */
class JsonObjectSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

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
    assert(e.getSqlState == "2200E")
  }

  test("a null key is validated before a null value is omitted under ABSENT ON NULL") {
    // ABSENT ON NULL omits members with a null value, but the key is validated first, so a null key
    // still raises JSON_OBJECT_NULL_KEY rather than being silently dropped along with the member.
    val e = intercept[SparkRuntimeException] {
      sql("SELECT json_object(NULL VALUE NULL ABSENT ON NULL)").collect()
    }
    assert(e.getCondition == "JSON_OBJECT_NULL_KEY")
    assert(e.getSqlState == "2200E")
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
        "sqlExpr" -> "\"JSON_OBJECT(ok VALUE 1, 2 VALUE bad NULL ON NULL)\"",
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

  test("JSON_OBJECT is not foldable") {
    // Folding a constant JSON_OBJECT would surface a null-key error at optimization even for rows a
    // filter/join drops, so it stays non-foldable.
    assert(!JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType).foldable)
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
    assert(e.getSqlState == "2200E")
  }

  test("a foldable literal key is rendered once and reused across rows") {
    // JSON_OBJECT caches the rendered name of a foldable non-null key; the same key must still be
    // emitted for every row.
    checkAnswer(
      sql("SELECT json_object('id' VALUE a) FROM VALUES (1), (2) t(a)"),
      Seq(Row("""{"id":1}"""), Row("""{"id":2}""")))
    // A foldable key that evaluates to null is not cached: it must still raise JSON_OBJECT_NULL_KEY
    // per row rather than being silently skipped.
    Seq("NULL", "CAST(NULL AS STRING)").foreach { k =>
      val e = intercept[SparkRuntimeException] {
        sql(s"SELECT json_object($k VALUE 1)").collect()
      }
      assert(e.getCondition == "JSON_OBJECT_NULL_KEY", s"for key $k")
      assert(e.getSqlState == "2200E", s"for key $k")
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
    // The value is never null, but JSON_OBJECT is throwable (a null key raises at eval), so it must
    // report nullable: were it non-nullable, `NullPropagation` would fold `IS [NOT] NULL` and
    // `count(...)` away and skip the eval that must raise JSON_OBJECT_NULL_KEY (see below).
    assert(JsonObjectExpr(
      Seq((Literal("k"), Literal.create(null, IntegerType))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType).nullable)
    assert(sql("SELECT json_object('id' VALUE a) FROM VALUES (1), (2) t(a)")
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

  test("is marked throwable so the optimizer will not push it below a filtering join") {
    // JSON_OBJECT throws on a null key at runtime, so throwable must be true even when its children
    // are not themselves throwable.
    val e = JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Null, StringType)
    assert(e.throwable)
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

  test("a constant-folded raw nested value round-trips through .sql as raw") {
    // ConstantFolding rewrites the nested json_array(1) to the literal '[1]', but rawJson stays
    // frozen true. .sql must emit FORMAT JSON so reparse splices it raw rather than quoting it.
    val jsonObj = sql("SELECT json_object('a' VALUE json_array(1)) AS r")
      .queryExecution.optimizedPlan.expressions
      .flatMap(_.collect { case j: JsonObjectExpr => j }).head
    assert(jsonObj.children(1).isInstanceOf[Literal],
      "the nested constructor should have been constant-folded to a literal")
    assert(jsonObj.sql.contains("FORMAT JSON"))
    checkAnswer(sql(s"SELECT ${jsonObj.sql} AS r"), Row("""{"a":[1]}"""))
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
        checkAnswer(sql("SELECT json_object(*) FROM VALUES ('id', 7) AS t(a, b)"), Row("shadowed"))
        // A clause-bearing form is not a function call, so it stays the built-in constructor.
        checkAnswer(sql("SELECT json_object('id' VALUE 7 RETURNING STRING)"), Row("""{"id":7}"""))
        checkAnswer(sql("SELECT json_object('id' VALUE NULL ABSENT ON NULL)"), Row("{}"))
        // A nested JSON constructor stays on the direct-construction path, so it is not shadowed
        // and its parse-time raw splice is preserved.
        checkAnswer(
          sql("SELECT json_object('a' VALUE json_object('b' VALUE 1))"),
          Row("""{"a":{"b":1}}"""))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_object")
      }
    }
  }

  test("a comma-form nested JSON producer is spliced raw when unshadowed") {
    // The comma form routes through resolution, but the unshadowed built-in reconstruction still
    // splices a lexically nested constructor raw (via JsonImplicitFormatCarrier), matching VALUE.
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
  }

  test("a compatible routine shadows a comma-form call even with a nested producer value") {
    // The finding: argument shape must not remove routine candidates. A (STRING, STRING) routine is
    // compatible with the nested-producer call (the nested value is STRING), so the clause-free
    // comma form resolves to the routine, not the built-in -- the carrier is transparent to
    // overload selection.
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_object(a STRING, b STRING) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        checkAnswer(sql("SELECT json_object('a', 'x')"), Row("shadowed"))
        // Previously the nested-producer form bypassed the routine (built-in); now it is shadowed.
        checkAnswer(sql("SELECT json_object('a', json_object('b', 1))"), Row("shadowed"))
        checkAnswer(sql("SELECT json_object('a', json_array(1))"), Row("shadowed"))
        // The dedicated VALUE form's nested value stays on the direct path, so it is not shadowed
        // and its parse-time raw splice is preserved.
        checkAnswer(
          sql("SELECT json_object('a' VALUE json_object('b' VALUE 1))"),
          Row("""{"a":{"b":1}}"""))
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

  test("a nested JSON-producing argument through a routed JSON_OBJECT call is quoted") {
    // A routed call carries no lexical FORMAT JSON, so a nested JSON constructor argument is quoted
    // as a plain value, unlike the JSON_OBJECT(...) grammar which splices it. Splicing through a
    // routed/qualified call is left as a follow-up (SPARK-59243).
    checkAnswer(
      sql("SELECT builtin.json_object('a', json_object('b', 1))"),
      Row("""{"a":"{\"b\":1}"}"""))
    checkAnswer(
      sql("SELECT json_object('a', builtin.json_object('b', 1))"),
      Row("""{"a":"{\"b\":1}"}"""))
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

  test("json_object is registered as a built-in function") {
    assert(spark.sessionState.catalog.isBuiltinFunction("json_object"))
  }

  test("single-pass: routed JSON_OBJECT resolves and expands a nested star") {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      Seq(
        "SELECT json_object('id', 7)",
        "SELECT json_object('vals', array(*)) FROM VALUES (1, 2) AS t(a, b)",
        // A routed comma-form nested producer carries a JsonImplicitFormatCarrier (a
        // TaggingExpression the ResolverGuard admits) that the built-in unwraps.
        "SELECT json_object('a', json_object('b', 1))"
      ).foreach { query =>
        // Analyze only: the single-pass analyzer cannot yet run every operator the action path
        // needs, so assert the routed call resolves (via the ResolverGuard allowlist) and neither a
        // star nor an un-unwrapped carrier survives, rather than executing it.
        val analyzed = sql(query).queryExecution.analyzed
        assert(analyzed.resolved, s"for $query")
        assert(!analyzed.exists(_.expressions.exists(_.exists(_.isInstanceOf[Star]))),
          s"star should not survive analysis for $query")
        assert(!analyzed.exists(_.expressions.exists(_.exists(
          _.isInstanceOf[JsonImplicitFormatCarrier]))),
          s"carrier should not survive analysis for $query")
      }
    }
  }
}
