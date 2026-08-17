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
import org.apache.spark.sql.catalyst.expressions.{Cast, Collate, JsonConstructorNullBehavior, JsonObjectExpr, Literal}
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

  test("construct object using comma-separated key-value syntax") {
    checkAnswer(
      sql("SELECT json_object('id', 7, 'name', 'Ada')"),
      Row("""{"id":7,"name":"Ada"}"""))
  }

  test("an odd number of arguments in the comma syntax is rejected") {
    // The comma form requires paired key/value arguments; a dangling key ('name') has no value.
    // JSON_OBJECT is a non-reserved keyword, so when the constructor grammar cannot match, the call
    // falls back to an ordinary function-call parse -- which then fails to resolve, rather than the
    // key being silently dropped. Either way the query is rejected.
    val e = intercept[AnalysisException] {
      sql("SELECT json_object('id', 7, 'name')")
    }
    assert(e.getCondition == "UNRESOLVED_ROUTINE")
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
        Seq((Literal("k"), Literal(1))), Seq(false), JsonConstructorNullBehavior.Null, returning)
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
      Seq(false), JsonConstructorNullBehavior.Null, StringType)
    bad.checkInputDataTypes() match {
      case DataTypeMismatch(sub, _) => assert(sub == "CANNOT_CONVERT_TO_JSON")
      case other => fail(s"expected DataTypeMismatch, got $other")
    }
    // A spatial type appearing only as a MAP KEY is fine: JacksonGenerator writes map keys via
    // toString, so the value-type guard must not over-reject it.
    val ok = JsonObjectExpr(
      Seq((Literal("k"), Literal.create(null, MapType(GeometryType(4326), IntegerType)))),
      Seq(false), JsonConstructorNullBehavior.Null, StringType)
    assert(ok.checkInputDataTypes().isSuccess)
  }

  test("SQL renders an explicit collated RETURNING and omits only the default") {
    val collated = JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(false), JsonConstructorNullBehavior.Null,
      StringType("UTF8_LCASE"))
    assert(collated.sql.contains("RETURNING STRING COLLATE UTF8_LCASE"))
    // The omitted default is the companion StringType (by reference) and renders no RETURNING.
    val default = JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(false), JsonConstructorNullBehavior.Null, StringType)
    assert(default.sql == "JSON_OBJECT('k' VALUE 1)")
  }

  test("SQL renders a raw nested value as a bare constructor even after collation wrapping") {
    val inner = JsonObjectExpr(
      Seq((Literal("b"), Literal(1))), Seq(false), JsonConstructorNullBehavior.Null, StringType)
    // Simulate the default-collation rule wrapping the raw nested value in a Cast. rawJson stays
    // frozen true; .sql must render the bare constructor so reparse re-derives raw splicing (there
    // is no value-level FORMAT JSON marker in JSON_OBJECT).
    val wrapped = JsonObjectExpr(
      Seq((Literal("a"), Cast(inner, StringType("UTF8_LCASE")))), Seq(true),
      JsonConstructorNullBehavior.Null, StringType)
    assert(wrapped.sql == "JSON_OBJECT('a' VALUE JSON_OBJECT('b' VALUE 1))")
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
      Seq((Literal("b"), Literal(1))), Seq(false), JsonConstructorNullBehavior.Null, StringType)
    val quoted = JsonObjectExpr(
      Seq((Literal("a"), inner)), Seq(false), JsonConstructorNullBehavior.Null, StringType)
    assert(quoted.sql == "JSON_OBJECT('a' VALUE CAST(JSON_OBJECT('b' VALUE 1) AS STRING))")
    checkAnswer(sql(s"SELECT ${quoted.sql}"), Row("""{"a":"{\"b\":1}"}"""))
  }

  test("JSON_OBJECT is not foldable") {
    // Folding a constant JSON_OBJECT would (a) surface a null-key error at optimization even for
    // rows a filter/join drops, and (b) fold a nested raw JSON_OBJECT value to a string literal,
    // which .sql could no longer render as a bare constructor (JSON_OBJECT has no value-level
    // FORMAT JSON marker). So it stays non-foldable.
    assert(!JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(false),
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
    // RETURNING collation must survive the view's default collation. Pin the fixed-point analyzer:
    // the single-pass resolver does not yet resolve a TimeZoneAware JSON constructor's timezone
    // when re-resolving a view (a pre-existing gap independent of collation); the CTAS test above
    // already exercises the single-pass path via the dual-run analyzer.
    withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
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

  test("is not nullable: JSON_OBJECT always produces a JSON text value") {
    // Even an empty constructor yields `{}`, and the STRING -> STRING RETURNING cast cannot produce
    // null, so the result is never null regardless of member nullability.
    assert(!JsonObjectExpr(
      Seq((Literal("k"), Literal.create(null, IntegerType))), Seq(false),
      JsonConstructorNullBehavior.Null, StringType).nullable)
    assert(sql("SELECT json_object('id' VALUE a) FROM VALUES (1), (2) t(a)")
      .schema.head.nullable === false)
  }

  test("is marked throwable so the optimizer will not push it below a filtering join") {
    // JSON_OBJECT throws on a null key at runtime, so throwable must be true even when its children
    // are not themselves throwable.
    val e = JsonObjectExpr(
      Seq((Literal("k"), Literal(1))), Seq(false), JsonConstructorNullBehavior.Null, StringType)
    assert(e.throwable)
  }

  test("throwable keeps a null-key predicate above a filtering join") {
    // The optimizer must not push a throwable predicate below the join (PushPredicateThroughJoin
    // only pushes non-throwable conditions), so a null-key row the join eliminates is never
    // evaluated and does not throw. Were the constructor not throwable, the predicate would push to
    // the probe side and throw on the eliminated row.
    //
    // The predicate compares the result to a literal rather than using `IS NOT NULL`: JSON_OBJECT
    // is non-nullable, so `IS NOT NULL` would be folded to `true` and the constructor dropped
    // from the plan entirely, leaving nothing to push (or throw). A comparison keeps the
    // constructor in the predicate so the pushdown behavior is actually exercised.
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

  test("SQL renders a COLLATE-wrapped raw nested value as a bare constructor") {
    // A raw value behind a pass-through Collate renders as the bare constructor so reparse
    // re-derives raw splicing (the collation does not affect the spliced-raw bytes).
    val inner = JsonObjectExpr(
      Seq((Literal("b"), Literal(1))), Seq(false), JsonConstructorNullBehavior.Null, StringType)
    val wrapped = JsonObjectExpr(
      Seq((Literal("a"), Collate(inner, Literal("UTF8_BINARY")))), Seq(true),
      JsonConstructorNullBehavior.Null, StringType)
    assert(wrapped.sql == "JSON_OBJECT('a' VALUE JSON_OBJECT('b' VALUE 1))")
    checkAnswer(sql(s"SELECT ${wrapped.sql}"), Row("""{"a":{"b":1}}"""))
  }
}
