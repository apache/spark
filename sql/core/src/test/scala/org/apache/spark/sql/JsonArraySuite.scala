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
import org.apache.spark.sql.catalyst.expressions.{Cast, Collate, JsonArray, JsonConstructorNullBehavior, JsonQuery, JsonQueryBehavior, JsonQueryQuotes, JsonQueryWrapper, Literal, ResolvedCollation}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{CharType, GeographyType, GeometryType, IntegerType, MapType, StringType, VarcharType}

/**
 * Test suite for the `JSON_ARRAY` ANSI SQL:2016 constructor function.
 */
class JsonArraySuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  test("JSON_ARRAY with simple scalar values") {
    checkAnswer(
      sql("SELECT JSON_ARRAY(1, 'x', true)"),
      Row("""[1,"x",true]"""))
  }

  test("JSON_ARRAY with NULL elements - ABSENT ON NULL (default)") {
    checkAnswer(
      sql("SELECT JSON_ARRAY(1, NULL, 3)"),
      Row("[1,3]"))
  }

  test("JSON_ARRAY with NULL elements - NULL ON NULL") {
    checkAnswer(
      sql("SELECT JSON_ARRAY(1, NULL, 3 NULL ON NULL)"),
      Row("[1,null,3]"))
  }

  test("JSON_ARRAY with NULL elements - explicit ABSENT ON NULL") {
    // Exercise the explicit `ABSENT ON NULL` grammar branch (the default is implicit absent, so
    // this spelling is otherwise untested); it drops NULL elements just like the default.
    checkAnswer(
      sql("SELECT JSON_ARRAY(1, NULL, 3 ABSENT ON NULL)"),
      Row("[1,3]"))
    checkAnswer(
      sql("SELECT JSON_ARRAY(1, NULL, 3 ABSENT ON NULL RETURNING STRING)"),
      Row("[1,3]"))
  }

  test("JSON_ARRAY with empty list") {
    checkAnswer(
      sql("SELECT JSON_ARRAY()"),
      Row("[]"))
  }

  test("JSON_ARRAY with floating point numbers") {
    checkAnswer(
      sql("SELECT JSON_ARRAY(1.5, 2.7)"),
      Row("[1.5,2.7]"))
  }

  test("JSON_ARRAY with mixed types") {
    checkAnswer(
      sql("SELECT JSON_ARRAY(1, 'text', 3.14, true, false)"),
      Row("""[1,"text",3.14,true,false]"""))
  }

  test("JSON_ARRAY with all NULLs and ABSENT ON NULL") {
    checkAnswer(
      sql("SELECT JSON_ARRAY(NULL, NULL)"),
      Row("[]"))
  }

  test("JSON_ARRAY with RETURNING STRING (explicit)") {
    checkAnswer(
      sql("SELECT JSON_ARRAY(1, 2, 3 RETURNING STRING)"),
      Row("[1,2,3]"))
  }

  test("JSON_ARRAY with both NULL ON NULL and RETURNING clauses") {
    // The grammar allows `... ON NULL` and `RETURNING` together, in that order; exercise both.
    checkAnswer(
      sql("SELECT JSON_ARRAY(1, NULL, 3 NULL ON NULL RETURNING STRING)"),
      Row("[1,null,3]"))
  }

  test("JSON_ARRAY over non-foldable columns exercises row-wise eval") {
    val df = Seq((1, "a", true), (2, "b", false)).toDF("i", "s", "b")
    checkAnswer(
      df.selectExpr("JSON_ARRAY(i, s, b)"),
      Seq(Row("""[1,"a",true]"""), Row("""[2,"b",false]""")))
  }

  test("JSON_ARRAY renders decimals and dates via Jackson, not toString") {
    checkAnswer(
      sql("SELECT JSON_ARRAY(CAST(1.50 AS DECIMAL(5,2)), DATE'2020-01-02')"),
      Row("""[1.50,"2020-01-02"]"""))
  }

  test("JSON_ARRAY renders a TIMESTAMP via to_json's writer in the session time zone") {
    // The constructor is TimeZoneAware and shares to_json's writer, so a TIMESTAMP element must
    // render identically to to_json of the singleton array, formatted in the session time zone.
    // Assert agreement with that writer (rather than pinning a fragile format string), and that the
    // rendering tracks the session time zone by differing between two zones.
    def render(tz: String): String = withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
      val out =
        sql("SELECT JSON_ARRAY(TIMESTAMP'2020-01-02 03:04:05')").collect().head.getString(0)
      val expected =
        sql("SELECT to_json(array(TIMESTAMP'2020-01-02 03:04:05'))").collect().head.getString(0)
      assert(out == expected, s"for tz=$tz")
      out
    }
    assert(render("UTC") != render("America/Los_Angeles"))
  }

  test("JSON_ARRAY renders array and map elements as JSON structures, like to_json") {
    // The docs state array/map/struct arguments render via the same writer as to_json (as nested
    // JSON structures, not quoted strings). Cover arrays and maps explicitly (structs are covered
    // by the ignoreNullFields test); a nested array element serializes to [1,2], a map to {"k":1}.
    checkAnswer(
      sql("SELECT JSON_ARRAY(array(1, 2), map('k', 1))"),
      Row("""[[1,2],{"k":1}]"""))
    checkAnswer(
      sql("SELECT JSON_ARRAY(array(array(1), array(2, 3)))"),
      Row("[[[1],[2,3]]]"))
  }

  test("JSON_ARRAY strings are escaped") {
    checkAnswer(
      sql("""SELECT JSON_ARRAY('a"b', 'c\td')"""),
      Row("""["a\"b","c\td"]"""))
  }

  test("nested JSON_ARRAY is spliced raw, not re-quoted (implicit FORMAT JSON)") {
    checkAnswer(
      sql("SELECT JSON_ARRAY(JSON_ARRAY(1, 2), 3)"),
      Row("[[1,2],3]"))
    checkAnswer(
      sql("SELECT JSON_ARRAY(JSON_ARRAY(1))"),
      Row("[[1]]"))
  }

  test("explicit FORMAT JSON splices a string verbatim; a plain string is quoted") {
    // A plain string element is quoted and escaped like any other string value...
    checkAnswer(sql("""SELECT JSON_ARRAY('[1,2]')"""), Row("""["[1,2]"]"""))
    // ...while FORMAT JSON marks it as already-JSON text, spliced in verbatim.
    checkAnswer(sql("""SELECT JSON_ARRAY('[1,2]' FORMAT JSON)"""), Row("[[1,2]]"))
    checkAnswer(
      sql("""SELECT JSON_ARRAY('{"a":1}' FORMAT JSON, 'x')"""),
      Row("""[{"a":1},"x"]"""))
  }

  test("splicing is decided from the source, not the optimized plan shape") {
    // A JSON_ARRAY result surfaced as a column is a plain STRING and must be quoted -- even though
    // CollapseProject may inline the inner JSON_ARRAY into the outer argument position. The FORMAT
    // JSON decision is frozen from the lexical argument at parse time, so it is independent of that
    // inlining: the result is ["[0]"], never [[0]]. Use a non-foldable producer (JSON_ARRAY(id)):
    // a foldable one is "cheap" and CollapseProject inlines it even when referenced twice, so both
    // cases would otherwise cover the same shape.
    def projectCount(df: DataFrame): Int =
      df.queryExecution.optimizedPlan.collect { case _: Project => () }.size

    // Single reference: CollapseProject inlines the inner JSON_ARRAY into the outer argument, so
    // the producer Project collapses away.
    val inlined = sql("SELECT JSON_ARRAY(a) AS r FROM (SELECT JSON_ARRAY(id) AS a FROM range(1)) t")
    assert(projectCount(inlined) == 1)
    checkAnswer(inlined, Row("""["[0]"]"""))

    // Referencing the non-foldable alias twice blocks inlining, so the producer Project survives.
    val notInlined =
      sql("SELECT JSON_ARRAY(a) AS r, a FROM (SELECT JSON_ARRAY(id) AS a FROM range(1)) t")
    assert(projectCount(notInlined) == 2)
    // Same splice decision (quoted) despite the different plan shape.
    checkAnswer(notInlined, Row("""["[0]"]""", "[0]"))
  }

  test("JSON_ARRAY column with NULL under both ON NULL modes") {
    val df = Seq(Some(1), None).toDF("i")
    checkAnswer(
      df.selectExpr("JSON_ARRAY(i)"),
      Seq(Row("[1]"), Row("[]")))
    checkAnswer(
      df.selectExpr("JSON_ARRAY(i NULL ON NULL)"),
      Seq(Row("[1]"), Row("[null]")))
  }

  test("nested JSON_ARRAY with a collated STRING RETURNING is still spliced raw") {
    // The inner array carries implicit FORMAT JSON regardless of its (collated) result collation,
    // so it is spliced raw as [[1],2], not re-quoted as ["[1]",2].
    checkAnswer(
      sql("SELECT JSON_ARRAY(JSON_ARRAY(1 RETURNING STRING COLLATE UTF8_LCASE), 2)"),
      Row("[[1],2]"))
  }

  test("a nested constructor wrapped in a postfix COLLATE is still spliced raw") {
    // `... COLLATE c` wraps the nested constructor in a value-preserving Collate. The implicit
    // FORMAT JSON must be seen through that wrapper, so the inner array is spliced ([[1]]), not
    // treated as a plain string and quoted (["[1]"]).
    checkAnswer(
      sql("SELECT JSON_ARRAY(JSON_ARRAY(1) COLLATE UTF8_LCASE)"),
      Row("[[1]]"))
    checkAnswer(
      sql("SELECT JSON_ARRAY(JSON_ARRAY(1, 2) COLLATE UTF8_LCASE, 3)"),
      Row("[[1,2],3]"))
  }

  test("a nested JSON_QUERY is spliced under KEEP QUOTES and quoted under OMIT QUOTES") {
    // JSON_QUERY emits JSON text under the default KEEP QUOTES, so a lexically nested JSON_QUERY
    // carries implicit FORMAT JSON and is spliced raw: the matched object is [{"x":1}], not the
    // quoted string ["{\"x\":1}"].
    checkAnswer(
      sql("""SELECT JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a'))"""),
      Row("""[{"x":1}]"""))
    checkAnswer(
      sql("""SELECT JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a'), 2)"""),
      Row("""[{"x":1},2]"""))
    // OMIT QUOTES returns the matched scalar string's decoded content (Ada, not "Ada") -- an
    // ordinary string -- so it takes the quoted path: ["Ada"], never the invalid splice [Ada].
    checkAnswer(
      sql("""SELECT JSON_ARRAY(JSON_QUERY('{"n":"Ada"}', '$.n' OMIT QUOTES))"""),
      Row("""["Ada"]"""))
  }

  test("FORMAT JSON on a non-string argument is rejected at analysis") {
    val e = intercept[AnalysisException] {
      sql("SELECT JSON_ARRAY(123 FORMAT JSON)").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_FORMAT_JSON_INPUT")
  }

  test("explicit FORMAT JSON with valid but whitespaced JSON is spliced verbatim") {
    // Validation only checks well-formedness; the original text (including insignificant
    // whitespace) is spliced as-is, not re-serialized.
    checkAnswer(sql("""SELECT JSON_ARRAY('[1,  2]' FORMAT JSON)"""), Row("[[1,  2]]"))
    checkAnswer(sql("""SELECT JSON_ARRAY('  true ' FORMAT JSON)"""), Row("[  true ]"))
  }

  test("explicit FORMAT JSON with a malformed value is rejected at runtime") {
    // A single string-typed argument passes analysis, but a value that is not exactly one
    // well-formed JSON value would corrupt the surrounding array, so it fails at eval.
    Seq(
      "'1,2'",            // two values, not one -- would splice as [1,2]
      "'{\"a\":1'",       // truncated object
      "'[1,'",            // truncated array
      "'not json'",       // bare word
      "''").foreach { arg => // empty string carries no JSON value
      val e = intercept[SparkRuntimeException] {
        sql(s"SELECT JSON_ARRAY($arg FORMAT JSON)").collect()
      }
      assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE", s"for argument $arg")
    }
  }

  test("malformed FORMAT JSON error truncates a long value to a bounded preview") {
    // A large malformed payload must not be inlined whole into the error message. The preview is
    // capped (100 chars) and the full length is reported instead.
    val long = "z" * 500 // not valid JSON (bare word) and longer than the preview cap
    val e = intercept[SparkRuntimeException] {
      sql(s"SELECT JSON_ARRAY('$long' FORMAT JSON)").collect()
    }
    assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE")
    val msg = e.getMessage
    assert(msg.contains("(500 characters)"), msg)
    assert(!msg.contains("z" * 101), "the full value must not be inlined; preview is capped")
  }

  test("explicit FORMAT JSON validates per-row over non-foldable columns") {
    val df = Seq("[1,2]", "1,2").toDF("s")
    val e = intercept[SparkRuntimeException] {
      df.selectExpr("JSON_ARRAY(s FORMAT JSON)").collect()
    }
    assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE")
  }

  test("explicit FORMAT JSON over a nullable column follows ON NULL, validating only non-nulls") {
    // A nullable string column: NULL rows must be handled by ON NULL (dropped / kept as JSON null)
    // before any validation, and only the non-null rows are validated as JSON text.
    val df = Seq(Some("[1,2]"), None, Some("{\"a\":1}")).toDF("s")
    checkAnswer(
      df.selectExpr("JSON_ARRAY(s FORMAT JSON)"),
      Seq(Row("[[1,2]]"), Row("[]"), Row("""[{"a":1}]""")))
    checkAnswer(
      df.selectExpr("JSON_ARRAY(s FORMAT JSON NULL ON NULL)"),
      Seq(Row("[[1,2]]"), Row("[null]"), Row("""[{"a":1}]""")))
    // A non-null but malformed row still fails; the NULL row does not shield it.
    val bad = Seq(None, Some("1,2")).toDF("s")
    val e = intercept[SparkRuntimeException] {
      bad.selectExpr("JSON_ARRAY(s FORMAT JSON NULL ON NULL)").collect()
    }
    assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE")
  }

  test("SQL round-trips FORMAT JSON and neutralizes an inlined implicit-JSON child") {
    val inner = JsonArray(
      Seq(Literal(1)), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    // A nested constructor left in an implicit (formatJson = true, trusted) position round-trips
    // as-is: reparse re-derives implicit FORMAT JSON.
    val spliced = JsonArray(
      Seq(inner), Seq(true), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(spliced.sql == "JSON_ARRAY(JSON_ARRAY(1))")
    // But a constructor inlined into a quoted (formatJson = false) position must be wrapped so
    // reparse keeps it quoted -- otherwise ["[1]"] would round-trip to [[1]].
    val quoted = JsonArray(
      Seq(inner), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(quoted.sql == "JSON_ARRAY(CAST(JSON_ARRAY(1) AS STRING))")
  }

  test("emitted SQL reparses and evaluates with raw-vs-quoted semantics preserved") {
    // The .sql renderings above are round-trip contracts: reparsing and evaluating them must
    // reproduce the original splicing. A bare nested constructor stays spliced; a cast-neutralized
    // one stays quoted.
    checkAnswer(sql("SELECT JSON_ARRAY(JSON_ARRAY(1))"), Row("[[1]]"))
    checkAnswer(sql("SELECT JSON_ARRAY(CAST(JSON_ARRAY(1) AS STRING))"), Row("""["[1]"]"""))
    // An explicit FORMAT JSON string literal round-trips through the emitted SQL too.
    val spliced = JsonArray(
      Seq(Literal("[1,2]")), Seq(true), Seq(true), JsonConstructorNullBehavior.Absent, StringType)
    assert(spliced.sql == "JSON_ARRAY('[1,2]' FORMAT JSON)")
    checkAnswer(sql(s"SELECT ${spliced.sql}"), Row("[[1,2]]"))
  }

  test("SQL forces FORMAT JSON for a spliced value whose child is not a bare constructor") {
    // A spliced element whose direct child is a wrapper (e.g. a Collate around a nested
    // constructor) must render an explicit `FORMAT JSON`, not rely on reparse re-deriving implicit
    // JSON through the wrapper's rendering: `Collate.sql` renders function-style
    // (collate(child, c)), which reparse would not recognize as an implicit nested constructor.
    val inner = JsonArray(
      Seq(Literal(1)), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    val collated = JsonArray(
      Seq(Collate(inner, ResolvedCollation("UTF8_LCASE"))),
      Seq(true), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(collated.sql.contains("FORMAT JSON"),
      s"expected FORMAT JSON to force the splice, got: ${collated.sql}")
  }

  test("SQL round-trips a nested JSON_QUERY per its quote mode") {
    def jsonQuery(quotes: JsonQueryQuotes): JsonQuery = JsonQuery(
      Literal("""{"a":{"x":1}}"""), "$.a", StringType, JsonQueryWrapper.Without, quotes,
      JsonQueryBehavior.Null, JsonQueryBehavior.Null)
    // KEEP QUOTES emits JSON text, so a nested JSON_QUERY left in an implicit (spliced) position
    // round-trips as-is: reparse re-derives the implicit FORMAT JSON.
    val keep = jsonQuery(JsonQueryQuotes.Keep)
    val splicedKeep = JsonArray(
      Seq(keep), Seq(true), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(splicedKeep.sql == """JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a'))""")
    // A KEEP QUOTES JSON_QUERY inlined into a quoted position must be neutralized with a cast so
    // reparse keeps it quoted rather than re-deriving implicit FORMAT JSON.
    val quotedKeep = JsonArray(
      Seq(keep), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(quotedKeep.sql == """JSON_ARRAY(CAST(JSON_QUERY('{"a":{"x":1}}', '$.a') AS STRING))""")
    // OMIT QUOTES emits an ordinary string, so it is not implicit: in a quoted position it renders
    // as-is, and in a spliced position it must render an explicit FORMAT JSON (it does not
    // round-trip implicitly).
    val omit = jsonQuery(JsonQueryQuotes.Omit)
    val quotedOmit = JsonArray(
      Seq(omit), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(quotedOmit.sql == """JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a' OMIT QUOTES))""")
    val splicedOmit = JsonArray(
      Seq(omit), Seq(true), Seq(true), JsonConstructorNullBehavior.Absent, StringType)
    assert(
      splicedOmit.sql ==
        """JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a' OMIT QUOTES) FORMAT JSON)""")
  }

  test("SQL preserves an explicit collated RETURNING on a spliced nested JSON_QUERY") {
    // A nested JSON_QUERY with an explicit `RETURNING STRING COLLATE UTF8_BINARY` renders via
    // `JsonQuery.sql`. That clause is a distinct StringType instance that compares `==` to the
    // default companion, so a value-equality check would drop it; reference identity keeps it, so
    // the emitted SQL round-trips faithfully instead of losing the user-written collation.
    val query = JsonQuery(
      Literal("""{"a":{"x":1}}"""), "$.a", StringType("UTF8_BINARY"), JsonQueryWrapper.Without,
      JsonQueryQuotes.Keep, JsonQueryBehavior.Null, JsonQueryBehavior.Null)
    val spliced = JsonArray(
      Seq(query), Seq(true), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(
      spliced.sql ==
        """JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a' RETURNING STRING COLLATE UTF8_BINARY))""")
    // Reparsing and evaluating the emitted SQL reproduces the raw splice.
    checkAnswer(sql(s"SELECT ${spliced.sql}"), Row("""[{"x":1}]"""))
  }

  test("SQL renders an explicit collated RETURNING and omits only the default") {
    val collated = JsonArray(
      Seq(Literal(1)), Seq(false), Seq(false),
      JsonConstructorNullBehavior.Absent, StringType("UTF8_LCASE"))
    assert(collated.sql.contains("RETURNING STRING COLLATE UTF8_LCASE"))
    // The omitted default is the companion StringType (by reference) and renders no RETURNING.
    val default = JsonArray(
      Seq(Literal(1)), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(default.sql == "JSON_ARRAY(1)")
  }

  test("a constant JSON_ARRAY is foldable unless it has an explicit FORMAT JSON") {
    assert(JsonArray(
      Seq(Literal(1), Literal("x")), Seq(false, false), Seq(false, false),
      JsonConstructorNullBehavior.Absent, StringType).foldable)
    // An explicit FORMAT JSON value is validated at eval and can throw, so it must not be folded
    // (which would move the error to optimization time, even for rows a filter would drop).
    assert(!JsonArray(
      Seq(Literal("[1]")), Seq(true), Seq(true),
      JsonConstructorNullBehavior.Absent, StringType).foldable)
  }

  test("an explicit FORMAT JSON is not evaluated for rows a filter drops") {
    // Because such a JSON_ARRAY is not foldable, its validation stays at runtime: a row the WHERE
    // removes never triggers the malformed-JSON error (constant folding would have thrown eagerly).
    checkAnswer(
      sql("SELECT JSON_ARRAY('1,2' FORMAT JSON) AS x FROM VALUES (1) t(a) WHERE a > 100"),
      Seq.empty)
    // A surviving row still errors.
    val e = intercept[SparkRuntimeException] {
      sql("SELECT JSON_ARRAY('1,2' FORMAT JSON) AS x FROM VALUES (1) t(a)").collect()
    }
    assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE")
  }

  test("IS NULL checks over malformed FORMAT JSON still evaluate the constructor") {
    // JsonArray is conservatively nullable when it can throw, so NullPropagation must not fold
    // these predicates to literals before the FORMAT JSON validation runs.
    Seq("IS NULL", "IS NOT NULL").foreach { predicate =>
      val e = intercept[SparkRuntimeException] {
        sql(s"SELECT JSON_ARRAY('1,2' FORMAT JSON) $predicate").collect()
      }
      assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE", s"for predicate $predicate")
    }
  }

  test("CHAR/VARCHAR RETURNING is normalized to STRING regardless of preserveCharVarcharTypeInfo") {
    Seq("CHAR(2)", "VARCHAR(2)").foreach { returning =>
      Seq("true", "false").foreach { preserve =>
        withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> preserve) {
          assert(
            sql(s"SELECT JSON_ARRAY(1 RETURNING $returning)").schema.head.dataType === StringType,
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
            |SELECT json_array(1) AS a,
            |       json_array(1 RETURNING STRING COLLATE UTF8_BINARY) AS b""".stripMargin)
        val schema = spark.table("t").schema
        // Omitted RETURNING (default STRING) follows the table's default collation.
        assert(schema("a").dataType === StringType("UTF8_LCASE"))
        // Explicit RETURNING ... COLLATE is the user's choice and must not be overwritten.
        assert(schema("b").dataType === StringType("UTF8_BINARY"))
      }
    }
  }

  test("default collation recurses into a nested JSON_ARRAY value") {
    // The rule casts each DefaultStringProducingExpression, recursing through a nested constructor
    // (the flat cases above only cover a top-level constructor). This CTAS runs the default
    // analyzer (single-pass included). Confirm the schema collation and that raw splicing still
    // produces well-formed nested JSON at runtime.
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withTable("t") {
        sql(
          """CREATE TABLE t DEFAULT COLLATION UTF8_LCASE AS
            |SELECT json_array(json_array(1)) AS a""".stripMargin)
        assert(spark.table("t").schema("a").dataType === StringType("UTF8_LCASE"))
        checkAnswer(spark.table("t"), Row("[[1]]"))
      }
    }
  }

  test("view default collation preserves an explicit collated RETURNING") {
    // Exercises the CREATE VIEW resolution path (in addition to the CTAS path above): the explicit
    // RETURNING collation must survive the view's default collation. Runs under dual-run so the
    // single-pass resolver's default-collation coercion (which wraps the constructor in a Cast to
    // the view collation) is exercised for parity with the fixed-point analyzer.
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withView("v") {
        sql(
          """CREATE VIEW v DEFAULT COLLATION UTF8_LCASE AS
            |SELECT json_array(1) AS a,
            |       json_array(1 RETURNING STRING COLLATE UTF8_BINARY) AS b""".stripMargin)
        val schema = spark.table("v").schema
        assert(schema("a").dataType === StringType("UTF8_LCASE"))
        assert(schema("b").dataType === StringType("UTF8_BINARY"))
      }
    }
  }

  test("throwable is set only when the constructor can actually throw at eval") {
    // An explicit FORMAT JSON value is validated at eval and can throw on malformed text, so the
    // constructor must be throwable even when its children are not.
    assert(JsonArray(
      Seq(Literal("[1]")), Seq(true), Seq(true),
      JsonConstructorNullBehavior.Absent, StringType).throwable)
    // A plain JSON_ARRAY with no explicit FORMAT JSON and no throwable children cannot throw
    // (RETURNING is STRING -> STRING), so it stays non-throwable and remains eligible for predicate
    // pushdown.
    assert(!JsonArray(
      Seq(Literal(1)), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
      .throwable)
    // A nested (implicit FORMAT JSON) constructor emits well-formed JSON by construction and is not
    // validated (needsValidation = false), so it alone does not make the outer throwable -- even
    // though it sits in a spliced (formatJson = true) position.
    val nested = JsonArray(
      Seq(Literal(1)), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(!JsonArray(
      Seq(nested), Seq(true), Seq(false), JsonConstructorNullBehavior.Absent, StringType).throwable)
  }

  test("frozen needsValidation survives an analyzer cast around a trusted nested constructor") {
    // ApplyDefaultCollation / DefaultCollationTypeCoercion may wrap a trusted nested constructor in
    // a Cast under a non-default object/view collation. The parse-time needsValidation = false must
    // survive that rewrite (rather than being re-derived from the now-Cast child), so the outer
    // stays foldable and non-throwable and does not spuriously validate the (trusted) nested output
    // per row.
    val nested = JsonArray(
      Seq(Literal(1)), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    val castWrapped = Cast(nested, StringType("UTF8_LCASE"))
    val outer = JsonArray(
      Seq(castWrapped), Seq(true), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(outer.foldable, "trusted nested value must stay foldable even when Cast-wrapped")
    assert(!outer.throwable, "trusted nested value must not become throwable when Cast-wrapped")
  }

  test("throwable keeps a FORMAT JSON predicate above a filtering join") {
    // The optimizer must not push a throwable predicate below the join (PushPredicateThroughJoin
    // only pushes non-throwable conditions), so a malformed-JSON row the join eliminates is never
    // evaluated and does not throw. Were the constructor not throwable, the predicate would push to
    // the probe side and throw on the eliminated row.
    withTempView("t", "u") {
      Seq((1, "[1]"), (2, "1,2")).toDF("id", "s").createOrReplaceTempView("t")
      Seq(1).toDF("id").createOrReplaceTempView("u")
      // id=2 carries malformed FORMAT JSON text but does not join u, so it is dropped first and its
      // predicate is never evaluated (it was not pushed below the join).
      checkAnswer(
        sql("""SELECT t.id FROM t JOIN u ON t.id = u.id
              |WHERE JSON_ARRAY(t.s FORMAT JSON) = '[[1]]'""".stripMargin),
        Row(1))
      // With the malformed row surviving the join, evaluation still throws.
      Seq(2).toDF("id").createOrReplaceTempView("u")
      val e = intercept[SparkRuntimeException] {
        sql("""SELECT t.id FROM t JOIN u ON t.id = u.id
              |WHERE JSON_ARRAY(t.s FORMAT JSON) = '[[1]]'""".stripMargin).collect()
      }
      assert(e.getCondition == "INVALID_JSON_FORMAT_JSON_VALUE")
    }
  }

  test("a spatial-typed element is rejected at analysis") {
    // GEOMETRY / GEOGRAPHY are AtomicTypes that JacksonUtils.verifyType accepts but
    // JacksonGenerator cannot serialize, so JSON_ARRAY rejects them up front, not at runtime.
    Seq(GeometryType(4326), GeographyType(4326)).foreach { dt =>
      val expr = JsonArray(
        Seq(Literal.create(null, dt)), Seq(false), Seq(false),
        JsonConstructorNullBehavior.Absent, StringType)
      expr.checkInputDataTypes() match {
        case DataTypeMismatch(errorSubClass, _) =>
          assert(errorSubClass == "CANNOT_CONVERT_TO_JSON", s"for $dt")
        case other => fail(s"expected DataTypeMismatch for $dt, got $other")
      }
    }
  }

  test("a spatial type is accepted when it appears only as a map key") {
    // JacksonGenerator writes map keys via toString, so a spatial *key* is serializable; only map
    // values (and struct fields / array elements / top-level) go through a typed writer. The guard
    // must therefore mirror verifyType and not over-reject a spatial map key.
    val ok = JsonArray(
      Seq(Literal.create(null, MapType(GeometryType(4326), IntegerType))),
      Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(ok.checkInputDataTypes().isSuccess)
    // But a spatial map *value* is still rejected.
    val bad = JsonArray(
      Seq(Literal.create(null, MapType(StringType, GeometryType(4326)))),
      Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(bad.checkInputDataTypes().isFailure)
  }

  test("NULL FORMAT JSON is accepted and follows ON NULL handling") {
    // An untyped NULL under FORMAT JSON must not be rejected at analysis: eval handles nulls before
    // it would ever splice, so it behaves like any other NULL element.
    checkAnswer(sql("SELECT JSON_ARRAY(NULL FORMAT JSON)"), Row("[]"))
    checkAnswer(sql("SELECT JSON_ARRAY(NULL FORMAT JSON NULL ON NULL)"), Row("[null]"))
    checkAnswer(sql("SELECT JSON_ARRAY(1, NULL FORMAT JSON, 3 NULL ON NULL)"), Row("[1,null,3]"))
  }

  test("value accepts an unparenthesized predicate expression") {
    // jsonArrayValue is parsed as a full `expression`, so predicates work without parentheses.
    checkAnswer(sql("SELECT JSON_ARRAY(1 IS NULL, 2 > 1)"), Row("[false,true]"))
  }

  test("widening the value to expression does not change documented forms") {
    // Design-doc examples where a value abuts ON NULL / FORMAT JSON must parse and evaluate
    // identically after widening valueExpression -> expression.
    checkAnswer(sql("SELECT JSON_ARRAY(1, NULL, 3 NULL ON NULL)"), Row("[1,null,3]"))
    checkAnswer(sql("SELECT JSON_ARRAY('[1,2]' FORMAT JSON)"), Row("[[1,2]]"))
    checkAnswer(sql("SELECT JSON_ARRAY(1, 'x', true)"), Row("""[1,"x",true]"""))
  }

  test("a non-string RETURNING type is rejected at analysis") {
    val e = intercept[AnalysisException] {
      sql("SELECT JSON_ARRAY(1 RETURNING INT)").collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_RETURNING_TYPE")
  }

  test("a directly-constructed JsonArray with a CHAR/VARCHAR RETURNING is rejected") {
    // The parser normalizes CHAR/VARCHAR RETURNING to STRING, but a raw CharType/VarcharType from
    // direct Catalyst construction would advertise a length JSON_ARRAY does not enforce.
    Seq(VarcharType(2), CharType(2)).foreach { returning =>
      val expr = JsonArray(
        Seq(Literal(1)), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, returning)
      expr.checkInputDataTypes() match {
        case DataTypeMismatch(errorSubClass, _) =>
          assert(errorSubClass == "INVALID_JSON_RETURNING_TYPE", s"for $returning")
        case other => fail(s"expected DataTypeMismatch for $returning, got $other")
      }
    }
  }

  test("JSON_ARRAY of a struct follows to_json null-field handling (ignoreNullFields)") {
    // Nested struct field nulls are governed by spark.sql.jsonGenerator.ignoreNullFields, exactly
    // as to_json -- JSON_ARRAY intentionally reuses that JSON writer. The (NULL | ABSENT) ON NULL
    // clause controls only top-level array elements, not fields inside a struct element.
    val q = "SELECT JSON_ARRAY(named_struct('a', 1, 'b', CAST(NULL AS INT)))"
    withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "true") {
      checkAnswer(sql(q), Row("""[{"a":1}]"""))
    }
    withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "false") {
      checkAnswer(sql(q), Row("""[{"a":1,"b":null}]"""))
    }
  }

}
