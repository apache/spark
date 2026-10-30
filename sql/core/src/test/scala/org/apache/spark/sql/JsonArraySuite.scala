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
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchFunctionException, NoSuchNamespaceException, Star}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.analysis.resolver.ExplicitlyUnsupportedResolverFeature
import org.apache.spark.sql.catalyst.expressions.{
  ApplyFunctionExpression,
  Cast,
  Collate,
  JsonArray,
  JsonConstructorNullBehavior,
  JsonQuery,
  JsonQueryBehavior,
  JsonQueryQuotes,
  JsonQueryWrapper,
  Literal,
  ResolvedCollation,
  UnresolvedJsonConstructor
}
import org.apache.spark.sql.catalyst.plans.logical.{Project, Range}
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.functions.{
  BoundFunction,
  ScalarFunction,
  UnboundFunction
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  CharType,
  DataType,
  GeographyType,
  GeometryType,
  IntegerType,
  MapType,
  StringType,
  StructType,
  VarcharType
}
import org.apache.spark.unsafe.types.UTF8String

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

  test("a function-style string() cast detaches implicit FORMAT JSON like CAST(... AS STRING)") {
    // A user CAST to STRING quotes the fragment (detaches implicit FORMAT JSON): `isImplicitlyJson`
    // does not look through the `Cast`, so the element is quoted, not spliced. The function-style
    // alias string(x) means CAST(x AS STRING) and ends up identical, but for a different reason at
    // the parse-time routing decision, where it is still an `UnresolvedFunction` (it only becomes a
    // `Cast` once the alias resolves): `isImplicitlyJson` recognizes only a grammar-built nested
    // constructor, so the outer call sees no implicit-JSON element, routes, and quotes it.
    checkAnswer(sql("SELECT JSON_ARRAY(CAST(JSON_ARRAY(1) AS STRING))"), Row("""["[1]"]"""))
    checkAnswer(sql("SELECT JSON_ARRAY(string(JSON_ARRAY(1)))"), Row("""["[1]"]"""))
    // The equivalence also holds through the routed path for other JSON-producing children.
    checkAnswer(
      sql("""SELECT JSON_ARRAY(string(JSON_QUERY('{"a":{"x":1}}', '$.a')))"""),
      Row("""["{\"x\":1}"]"""))
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

  test("a nested constructor wrapped in redundant parentheses is still spliced raw") {
    // Parentheses produce a ParenthesizedExpressionContext that AstBuilder unwraps to the inner
    // Catalyst expression directly (no wrapper node). The implicit FORMAT JSON must be seen through
    // that parser context (as through Collate above), so the inner array is spliced ([[1]]), not
    // treated as a plain string and quoted (["[1]"]).
    checkAnswer(sql("SELECT JSON_ARRAY((JSON_ARRAY(1)))"), Row("[[1]]"))
    checkAnswer(sql("SELECT JSON_ARRAY((JSON_ARRAY(1, 2)), 3)"), Row("[[1,2],3]"))
    checkAnswer(
      sql("""SELECT JSON_ARRAY((JSON_QUERY('{"a":{"x":1}}', '$.a')))"""),
      Row("""[{"x":1}]"""))
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
    // Both levels carry a default `RETURNING STRING`: a bare nested splice reparses as a routable
    // (shadowable) clause-free call, so each level needs the clause to stay bound to the built-in.
    assert(spliced.sql == "JSON_ARRAY(JSON_ARRAY(1 RETURNING STRING) RETURNING STRING)")
    // But a constructor inlined into a quoted (formatJson = false) position must be wrapped so
    // reparse keeps it quoted -- otherwise ["[1]"] would round-trip to [[1]].
    val quoted = JsonArray(
      Seq(inner), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(quoted.sql ==
      "JSON_ARRAY(CAST(JSON_ARRAY(1 RETURNING STRING) AS STRING) RETURNING STRING)")
  }

  test("emitted SQL reparses and evaluates with raw-vs-quoted semantics preserved") {
    // The .sql renderings above are round-trip contracts: reparsing and evaluating the actual
    // emitted SQL -- including any default RETURNING STRING clauses it carries -- must reproduce
    // the original splicing. A bare nested constructor stays spliced; a cast-neutralized one stays
    // quoted.
    val inner = JsonArray(
      Seq(Literal(1)), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    val spliced = JsonArray(
      Seq(inner), Seq(true), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    checkAnswer(sql(s"SELECT ${spliced.sql}"), Row("[[1]]"))
    val quoted = JsonArray(
      Seq(inner), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    checkAnswer(sql(s"SELECT ${quoted.sql}"), Row("""["[1]"]"""))
    // An explicit FORMAT JSON string literal round-trips through the emitted SQL too.
    val splicedLiteral = JsonArray(
      Seq(Literal("[1,2]")), Seq(true), Seq(true), JsonConstructorNullBehavior.Absent, StringType)
    assert(splicedLiteral.sql == "JSON_ARRAY('[1,2]' FORMAT JSON)")
    checkAnswer(sql(s"SELECT ${splicedLiteral.sql}"), Row("[[1,2]]"))
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
    // KEEP QUOTES emits JSON text, so a nested JSON_QUERY stays spliced: reparse re-derives the
    // implicit FORMAT JSON. Each clause-free level carries a default RETURNING STRING.
    val keep = jsonQuery(JsonQueryQuotes.Keep)
    val splicedKeep = JsonArray(
      Seq(keep), Seq(true), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(splicedKeep.sql ==
      """JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a' RETURNING STRING) RETURNING STRING)""")
    // A KEEP QUOTES JSON_QUERY inlined into a quoted position must be neutralized with a cast so
    // reparse keeps it quoted rather than re-deriving implicit FORMAT JSON.
    val quotedKeep = JsonArray(
      Seq(keep), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(quotedKeep.sql ==
      """JSON_ARRAY(CAST(JSON_QUERY('{"a":{"x":1}}', """ +
        """'$.a' RETURNING STRING) AS STRING) RETURNING STRING)""")
    // OMIT QUOTES emits an ordinary string, so it is not implicit: in a quoted position it renders
    // as-is, and in a spliced position it must render an explicit FORMAT JSON (it does not
    // round-trip implicitly).
    val omit = jsonQuery(JsonQueryQuotes.Omit)
    val quotedOmit = JsonArray(
      Seq(omit), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    assert(quotedOmit.sql ==
      """JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a' OMIT QUOTES) RETURNING STRING)""")
    val splicedOmit = JsonArray(
      Seq(omit), Seq(true), Seq(true), JsonConstructorNullBehavior.Absent, StringType)
    assert(
      splicedOmit.sql ==
        """JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a' OMIT QUOTES) FORMAT JSON)""")
    // Each emitted rendering must reparse and evaluate the same as its clause-free equivalent, so
    // any default RETURNING STRING it carries is semantically inert (splicedOmit, whose spliced
    // element forces the direct grammar branch, carries none).
    checkAnswer(sql(s"SELECT ${splicedKeep.sql}"),
      sql("""SELECT JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a'))""").collect().toSeq)
    checkAnswer(sql(s"SELECT ${quotedKeep.sql}"),
      sql("""SELECT JSON_ARRAY(CAST(JSON_QUERY('{"a":{"x":1}}', '$.a') AS STRING))""")
        .collect().toSeq)
    checkAnswer(sql(s"SELECT ${quotedOmit.sql}"),
      sql("""SELECT JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a' OMIT QUOTES))""").collect().toSeq)
    checkAnswer(sql(s"SELECT ${splicedOmit.sql}"),
      sql("""SELECT JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', '$.a' OMIT QUOTES) FORMAT JSON)""")
        .collect().toSeq)
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
        """JSON_ARRAY(JSON_QUERY('{"a":{"x":1}}', """ +
          """'$.a' RETURNING STRING COLLATE UTF8_BINARY) RETURNING STRING)""")
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
    // A clause-free built-in renders the default `RETURNING STRING` so its canonical SQL stays
    // bound to the built-in on reparse.
    assert(default.sql == "JSON_ARRAY(1 RETURNING STRING)")
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

  test("plain call goes through routine resolution and can be shadowed via SET PATH") {
    // `withUserDefinedFunction` is unusable here: its cleanup asserts the name no longer resolves,
    // but `json_array` is now a registered built-in, so drop the temporary routine explicitly.
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_array(a INT, b STRING) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("CREATE TEMPORARY FUNCTION json_value(a STRING, b STRING) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("CREATE TEMPORARY FUNCTION json_query(a STRING, b STRING) RETURNS STRING " +
          "RETURN 'shadowed'")
        sql("CREATE TEMPORARY FUNCTION json_exists(a STRING, b STRING) RETURNS BOOLEAN " +
          "RETURN false")
        sql("SET PATH = system.session, system.builtin")
        // A plain call is an ordinary function call, so the temporary routine (ahead of
        // system.builtin on the path) shadows the built-in constructor.
        checkAnswer(sql("SELECT json_array(1, 'x')"), Row("shadowed"))
        checkAnswer(sql("SELECT json_array(*) FROM VALUES (1, 'x') AS t(a, b)"), Row("shadowed"))
        // The clause-bearing form is not a function call, so it stays the built-in constructor.
        checkAnswer(sql("SELECT json_array('x' NULL ON NULL)"), Row("""["x"]"""))
        // A nested clause-free call routes too, so it is shadowed like a flat call (see the
        // SPARK-59243 nested-shadowing test). Qualifying every level keeps the built-in splice.
        checkAnswer(sql("SELECT builtin.json_array(builtin.json_array(1))"), Row("[[1]]"))
        checkAnswer(
          sql("""SELECT builtin.json_array(builtin.json_query('{"a":{"x":1}}', '$.a'))"""),
          Row("""[{"x":1}]"""))
        // A JSON_QUERY written as a direct constructor argument stays the stock built-in under a
        // json_query shadow, so the stock-owned outer call still splices it.
        checkAnswer(
          sql("""SELECT json_array(json_query('{"a":{"x":1}}', '$.a') NULL ON NULL)"""),
          Row("""[{"x":1}]"""))
        // Plain scalar and predicate children are still ordinary function calls. Use an explicit
        // outer NULL clause to keep the parent on the direct path while the children are shadowed.
        checkAnswer(
          sql("""SELECT json_array(json_value('{"a":"x"}', '$.a') NULL ON NULL)"""),
          Row("""["shadowed"]"""))
        checkAnswer(
          sql("""SELECT json_array(json_exists('{"a":1}', '$.a') NULL ON NULL)"""),
          Row("[false]"))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_array")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_value")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_query")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_exists")
      }
    }
  }

  test("SPARK-59685: default-clause JSON_ARRAY canonical SQL reparses to the built-in under a " +
      "shadowing PATH") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_array(a INT) RETURNS STRING RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        // A built-in JSON_ARRAY whose only clause is the default RETURNING STRING: the clause makes
        // it the built-in even under the shadow, but its canonical `sql` would drop the default.
        val jsonArray = sql("SELECT json_array(1 RETURNING STRING)")
          .queryExecution.analyzed.expressions
          .flatMap(_.collect { case ja: JsonArray => ja }).head
        // The rendering must reparse back to the built-in, not the same-named routine on the PATH.
        val reparsed = sql(s"SELECT ${jsonArray.sql}")
        assert(reparsed.queryExecution.analyzed.expressions
          .exists(_.exists(_.isInstanceOf[JsonArray])),
          s"canonical SQL bound the shadow instead of the built-in: ${jsonArray.sql}")
        checkAnswer(reparsed, Row("[1]"))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_array")
      }
    }
  }

  test("SPARK-59685: zero-arg default JSON_ARRAY canonical SQL reparses to the built-in under a " +
      "shadowing PATH") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_array() RETURNS STRING RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        // A zero-arg built-in JSON_ARRAY renders the distinct clause-only `JSON_ARRAY( RETURNING
        // STRING)`: it has no values, so the default clause is its sole marker on reparse.
        val jsonArray = sql("SELECT json_array(RETURNING STRING)")
          .queryExecution.analyzed.expressions
          .flatMap(_.collect { case ja: JsonArray => ja }).head
        // The rendering must reparse back to the built-in, not the same-named routine on the PATH.
        val reparsed = sql(s"SELECT ${jsonArray.sql}")
        assert(reparsed.queryExecution.analyzed.expressions
          .exists(_.exists(_.isInstanceOf[JsonArray])),
          s"canonical SQL bound the shadow instead of the built-in: ${jsonArray.sql}")
        checkAnswer(reparsed, Row("[]"))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_array")
      }
    }
  }

  test("SPARK-59685: a default JSON_ARRAY keeps a clean auto-generated column name") {
    val name = sql("SELECT json_array(1)").schema.head.name
    assert(!name.contains("RETURNING"), s"column name leaked the ownership clause: $name")
  }

  test("SPARK-59685: a nested JSON constructor keeps a clean auto-generated column name") {
    // Children are rendered in place for display names, so a nested constructor neither leaks the
    // round-trip RETURNING clause nor gains a synthetic FORMAT JSON that the outer would emit if
    // the child were flattened to a plain attribute first.
    val splicedArray = sql("SELECT json_array(json_array(1, 2), 3)").schema.head.name
    assert(splicedArray === "JSON_ARRAY(JSON_ARRAY(1, 2), 3)", s"unexpected name: $splicedArray")
    val nestedQuery = sql("""SELECT json_array(json_query('{"a":1}', '$.a'))""").schema.head.name
    assert(!nestedQuery.contains("RETURNING") && !nestedQuery.contains("FORMAT JSON"),
      s"nested name leaked a round-trip-only clause: $nestedQuery")
  }

  test("qualified plain JSON_ARRAY resolves to the built-in constructor") {
    checkAnswer(sql("SELECT builtin.json_array(1, 'x')"), Row("""[1,"x"]"""))
    checkAnswer(sql("SELECT system.builtin.json_array(1, 'x')"), Row("""[1,"x"]"""))
  }

  test("SPARK-59243: a nested JSON constructor through a routed JSON_ARRAY call is spliced") {
    // A routed or qualified built-in JSON_ARRAY splices a lexically-nested JSON constructor
    // argument, matching the JSON_ARRAY(...) grammar (`json_array(json_array(1))` -> `[[1]]`).
    // The built-in builder derives the FORMAT JSON flag from the resolved argument via
    // `ImplicitlyFormattedAsJson.splicesThroughCollationCast`, so a qualified outer call and an
    // unqualified call over a qualified nested constructor both splice, matching the fully
    // unqualified grammar form.
    checkAnswer(sql("SELECT builtin.json_array(json_array(1))"), Row("[[1]]"))
    checkAnswer(sql("SELECT system.builtin.json_array(json_array(1))"), Row("[[1]]"))
    checkAnswer(sql("SELECT builtin.json_array(json_array(1), 2)"), Row("[[1],2]"))
    checkAnswer(sql("SELECT json_array(builtin.json_array(1))"), Row("[[1]]"))
    checkAnswer(sql("SELECT json_array(json_array(0), builtin.json_array(1))"), Row("[[0],[1]]"))
    checkAnswer(
      sql("""SELECT builtin.json_array(json_query('{"a":{"x":1}}', '$.a'))"""),
      Row("""[{"x":1}]"""))
    // A user CAST still detaches FORMAT JSON, so the element stays quoted (the routed splice check
    // sees through an analyzer collation Cast, but not a user `CAST(... AS STRING)`).
    checkAnswer(sql("SELECT builtin.json_array(CAST(json_array(1) AS STRING))"), Row("""["[1]"]"""))
    // A user cast that changes the collation is told apart from an analyzer collation Cast only by
    // its USER_SPECIFIED_CAST tag, and must still detach.
    checkAnswer(
      sql("SELECT builtin.json_array(CAST(json_array(1) AS STRING COLLATE UTF8_LCASE))"),
      Row("""["[1]"]"""))
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withTable("t") {
        sql("CREATE TABLE t DEFAULT COLLATION UTF8_LCASE AS " +
          "SELECT builtin.json_array(CAST(json_array(1) AS STRING)) AS a")
        checkAnswer(spark.table("t"), Row("""["[1]"]"""))
      }
    }
  }

  test("SPARK-59243: clause-bearing JSON_ARRAY splices qualified nested constructors") {
    // Clause-bearing direct forms do not route through function resolution, but still need to
    // recover the qualified nested constructor after children resolve.
    checkAnswer(sql("SELECT json_array(builtin.json_array(1) NULL ON NULL)"), Row("[[1]]"))
    checkAnswer(sql("SELECT json_array(builtin.json_array(1) RETURNING STRING)"), Row("[[1]]"))
    checkAnswer(
      sql("SELECT json_array(builtin.json_array(1) FORMAT JSON NULL ON NULL)"),
      Row("[[1]]"))
    checkAnswer(
      sql("SELECT json_array(CAST(builtin.json_array(1) AS STRING) NULL ON NULL)"),
      Row("""["[1]"]"""))

    Seq(false, true).foreach { singlePass =>
      withSQLConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString) {
        val analyzed =
          sql("SELECT json_array(builtin.json_array(1) FORMAT JSON NULL ON NULL)")
            .queryExecution.analyzed
        assert(analyzed.resolved, s"singlePass=$singlePass")
        val arrays = analyzed.expressions.flatMap(_.collect { case j: JsonArray => j })
        val outer = arrays.find(_.nullBehavior == JsonConstructorNullBehavior.Null).get
        assert(outer.formatJson == Seq(true), s"singlePass=$singlePass")
        assert(outer.needsValidation == Seq(false), s"singlePass=$singlePass")
      }
    }
  }

  test("SPARK-59243: routed JSON_ARRAY splice round-trips through canonical SQL") {
    // The analyzed expression's canonical SQL must reparse to the same spliced result.
    Seq(
      "SELECT builtin.json_array(json_array(1))",
      "SELECT json_array(builtin.json_array(1))").foreach { query =>
      val analyzed = sql(query).queryExecution.analyzed
      val jsonArray = analyzed.expressions.flatMap(_.collect { case j: JsonArray => j }).head
      checkAnswer(sql(s"SELECT ${jsonArray.sql}"), Row("[[1]]"))
    }
  }

  test("SPARK-59243: spliced nested JSON_ARRAY canonical SQL stays built-in under a shadow") {
    // Canonical SQL of a resolved built-in must reparse to the built-in even under a shadow: the
    // nested splice now routes, so each clause-free level carries an ownership clause.
    val inner = JsonArray(
      Seq(Literal(1)), Seq(false), Seq(false), JsonConstructorNullBehavior.Absent, StringType)
    val canonical = JsonArray(
      Seq(inner), Seq(true), Seq(false), JsonConstructorNullBehavior.Absent, StringType).sql
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_array(a STRING) RETURNS STRING RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        checkAnswer(sql(s"SELECT $canonical"), Row("[[1]]"))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_array")
      }
    }
  }

  test("SPARK-59243: an explicit collation-changing string() cast detaches splicing") {
    // `string(... COLLATE c)` is an explicit cast (via the cast alias) that detaches FORMAT JSON,
    // so the nested constructor is quoted, not spliced. The analyzer-collation-cast unwrap in
    // `splicesThroughCollationCast` must not treat it as an `ApplyDefaultCollation` cast.
    checkAnswer(
      sql("SELECT builtin.json_array(string(json_array(1) COLLATE UTF8_LCASE))"),
      Row("""["[1]"]"""))
    // Also when the cast itself changes the collation: it is still a user cast, not a
    // default-collation recoloring, on both the routed and the direct grammar paths.
    val nested = "string(json_array(1 RETURNING STRING COLLATE UTF8_LCASE))"
    checkAnswer(sql(s"SELECT builtin.json_array($nested)"), Row("""["[1]"]"""))
    checkAnswer(sql(s"SELECT json_array($nested NULL ON NULL)"), Row("""["[1]"]"""))
  }

  test("SPARK-59243: a nested clause-free JSON_ARRAY call is shadowed like a flat call") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_array(a STRING) RETURNS STRING RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        // The inner json_array(1) is a grammar element (built-in) producing "[1]"; the outer
        // clause-free call routes, so the temp shadows it.
        checkAnswer(sql("SELECT json_array(json_array(1))"), Row("shadowed"))
        // Strict single-pass routes the outer clause-free call to the shadow owner too, but the
        // owner is a SQL function, which the single-pass analyzer does not yet resolve: it reaches
        // that owner (proving the routing) and then aborts with an
        // ExplicitlyUnsupportedResolverFeature on the SQL function body. The guard-gated modes fall
        // back to fixed-point, which produces the shadow result asserted above.
        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          intercept[ExplicitlyUnsupportedResolverFeature] {
            sql("SELECT json_array(json_array(1))").queryExecution.analyzed
          }
        }
        // Qualifying every level selects Spark's constructor and splices.
        checkAnswer(sql("SELECT builtin.json_array(builtin.json_array(1))"), Row("[[1]]"))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_array")
      }
    }
  }

  test("SPARK-59243: nested constructors splice in subqueries, grouping, and INSERT VALUES") {
    // The finalization rule runs over every operator, not just a top-level Project.
    checkAnswer(sql("SELECT (SELECT builtin.json_array(json_array(1)))"), Row("[[1]]"))
    checkAnswer(
      sql("SELECT 1 WHERE '[[1]]' IN (SELECT json_array(builtin.json_array(1)))"),
      Row(1))
    checkAnswer(
      sql("SELECT json_array(json_array(count(*))) FROM VALUES (1), (2) AS t(a)"),
      Row("[[2]]"))
    // HAVING and ORDER BY are checked separately: combining them with GROUP BY hits an unrelated
    // dual-run plan mismatch for any select-list expression.
    val grouped = "FROM VALUES (1), (2), (2) AS t(a) GROUP BY a"
    checkAnswer(
      sql(s"SELECT builtin.json_array(json_array(a)) AS j $grouped " +
        "HAVING builtin.json_array(json_array(count(*))) = '[[2]]'"),
      Row("[[2]]"))
    checkAnswer(
      sql(s"SELECT builtin.json_array(json_array(a)) AS j $grouped " +
        "ORDER BY json_array(builtin.json_array(a))"),
      Seq(Row("[[1]]"), Row("[[2]]")))
    withTable("t") {
      sql("CREATE TABLE t (j STRING) USING parquet")
      sql("INSERT INTO t VALUES (builtin.json_array(json_array(1))), (json_object('a', " +
        "builtin.json_object('b', 1)))")
      checkAnswer(sql("SELECT j FROM t"), Seq(Row("[[1]]"), Row("""{"a":{"b":1}}""")))
    }
  }

  test("SPARK-59243: a persistent function cannot reference a temp shadow via a nested call") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_array(a STRING) RETURNS STRING RETURN 'shadowed'")
        sql("SET PATH = system.session, system.builtin")
        // The routed nested call is a reference to the temp shadow, like the flat call.
        Seq("json_array(1)", "json_array(json_array(1))").foreach { body =>
          val e = intercept[AnalysisException] {
            sql(s"CREATE FUNCTION perm_json_fn() RETURNS STRING RETURN $body")
          }
          assert(e.getCondition == "INVALID_TEMP_OBJ_REFERENCE", s"for $body")
        }
        // Stock-owned (clause-bearing or FORMAT JSON) bodies do not reference the temp shadow.
        Seq(
          "json_array(1 NULL ON NULL)" -> "[1]",
          "json_array(json_array(1) NULL ON NULL)" -> "[[1]]",
          "json_array('[1]' FORMAT JSON)" -> "[[1]]"
        ).foreach { case (body, expected) =>
          sql(s"CREATE OR REPLACE FUNCTION perm_json_fn() RETURNS STRING RETURN $body")
          checkAnswer(sql("SELECT spark_catalog.default.perm_json_fn()"), Row(expected))
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP FUNCTION IF EXISTS perm_json_fn")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_array")
      }
    }
  }

  test("invalid: a bare star argument in plain JSON_ARRAY is not expanded") {
    Seq("json_array", "builtin.json_array", "system.builtin.json_array").foreach { func =>
      val e = intercept[AnalysisException] {
        sql(s"SELECT $func(*) FROM VALUES (1, 'x') AS t(a, b)").collect()
      }
      assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"for $func(*)")
    }
  }

  test("invalid: a bare star argument in clause-bearing JSON_ARRAY is not expanded") {
    val e = intercept[AnalysisException] {
      sql("SELECT json_array(* NULL ON NULL) FROM VALUES (1, 'x') AS t(a, b)").collect()
    }
    assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX")
  }

  test("JSON_ARRAY expands a star nested in a sibling constructor (array(*))") {
    // Only a direct star element (bare `*` or qualified `t.*`) is rejected. A star nested in
    // `array(...)` belongs to that call and is expanded there, exactly as `array(array(*))` would,
    // then JSON_ARRAY wraps the result.
    checkAnswer(
      sql("SELECT json_array(array(*)) FROM VALUES (1, 2) AS t(a, b)"),
      Row("[[1,2]]"))
    // Clause-bearing form (a direct-construction JsonArray node) behaves the same.
    checkAnswer(
      sql("SELECT json_array(array(*) NULL ON NULL) FROM VALUES (1, 2) AS t(a, b)"),
      Row("[[1,2]]"))
    // The outer wrapper routes through UnresolvedRoutedJsonArray because of the nested constructor
    // sibling; the nested star still belongs to array(*) and must expand before the outer direct
    // star guard runs.
    checkAnswer(
      sql("SELECT json_array(array(*), json_array(1)) FROM VALUES (1, 2) AS t(a, b)"),
      Row("[[1,2],[1]]"))
    // Alongside count(*): the array's star expands, count(*) is rewritten, neither is rejected.
    checkAnswer(
      sql("SELECT json_array(count(*), array(max(a))) FROM VALUES (1), (2) AS t(a)"),
      Row("[2,[2]]"))
  }

  test("single-pass: JSON_ARRAY expands a star nested in array(*)") {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      Seq(
        "SELECT json_array(array(*)) FROM VALUES (1, 2) AS t(a, b)",
        "SELECT json_array(array(*) NULL ON NULL) FROM VALUES (1, 2) AS t(a, b)",
        "SELECT json_array(array(*), json_array(1)) FROM VALUES (1, 2) AS t(a, b)"
      ).foreach { query =>
        // Analyze only: the single-pass analyzer cannot yet analyze or resolve every operator the
        // action path needs, so assert the nested star is neither rejected nor left unexpanded
        // rather than running it.
        val analyzed = sql(query).queryExecution.analyzed
        assert(analyzed.resolved, s"for $query")
        assert(!analyzed.exists(_.expressions.exists(_.exists(_.isInstanceOf[Star]))),
          s"star should not survive analysis for $query")
        if (query.contains("json_array(1)")) {
          val arrays = analyzed.expressions.flatMap(_.collect { case j: JsonArray => j })
          val outer = arrays.find(_.values.length == 2).get
          assert(outer.formatJson == Seq(false, true),
            s"nested constructor sibling should still be spliced for $query")
        }
      }
    }
  }

  test("single-pass rejects a bare star in plain and clause-bearing JSON_ARRAY built-ins") {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      Seq(
        "SELECT json_array(*) FROM VALUES (1, 'x') AS t(a, b)",
        "SELECT json_array(* NULL ON NULL) FROM VALUES (1, 'x') AS t(a, b)"
      ).foreach { query =>
        val e = intercept[AnalysisException] {
          spark.sql(query).queryExecution.analyzed
        }
        assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"for $query")
      }
    }
  }

  test("JSON_ARRAY accepts count(*): it is normalized to count(1), not star-expanded") {
    // count(*) is rewritten to count(1) rather than star-expanded, so it stays a valid aggregate
    // argument to the JSON_ARRAY built-in. The star pre-check must not reject the nested star.
    checkAnswer(
      sql("SELECT json_array(count(*)) FROM VALUES (1), (2), (3) AS t(a)"),
      Row("[3]"))
    // Alongside another aggregate argument.
    checkAnswer(
      sql("SELECT json_array(count(*), max(a)) FROM VALUES (1), (2), (3) AS t(a)"),
      Row("[3,3]"))
    // The clause-bearing form (a direct-construction JsonArray node) accepts it too.
    checkAnswer(
      sql("SELECT json_array(count(*) NULL ON NULL) FROM VALUES (1), (2), (3) AS t(a)"),
      Row("[3]"))
    // Qualified built-in references resolve to the same built-in and behave the same.
    checkAnswer(
      sql("SELECT builtin.json_array(count(*)) FROM VALUES (1), (2), (3) AS t(a)"),
      Row("[3]"))
  }

  test("single-pass: JSON_ARRAY accepts count(*)") {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      Seq(
        "SELECT json_array(count(*)) FROM VALUES (1), (2), (3) AS t(a)",
        "SELECT json_array(count(*) NULL ON NULL) FROM VALUES (1), (2), (3) AS t(a)"
      ).foreach { query =>
        // Analyze only: the single-pass analyzer cannot yet analyze or resolve every operator the
        // action path needs, so we assert the query resolves without
        // INVALID_USAGE_OF_STAR_OR_REGEX rather than running it.
        val analyzed = sql(query).queryExecution.analyzed
        assert(analyzed.resolved, s"for $query")
        assert(!analyzed.exists(_.expressions.exists(_.exists(_.isInstanceOf[Star]))),
          s"star should not survive analysis for $query")
      }
    }
  }

  test("single-pass: nested clause-free JSON_ARRAY resolves with qualified sibling splice") {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      val analyzed =
        sql("SELECT json_array(json_array(0), builtin.json_array(1))").queryExecution.analyzed
      assert(analyzed.resolved)
      val arrays = analyzed.expressions.flatMap(_.collect { case j: JsonArray => j })
      val outer = arrays.find(_.values.length == 2).get
      assert(outer.formatJson == Seq(true, true))
    }
  }

  test("SPARK-59243: nested JSON_ARRAY in a lambda preserves qualified sibling splicing") {
    checkAnswer(
      sql("SELECT transform(array(1), x -> json_array(json_array(x), builtin.json_array(x)))"),
      Row(Seq("[[1],[1]]")))
  }

  test("invalid: a bare star next to count(*) is still rejected in JSON_ARRAY") {
    // count(*) is excluded from the star check, but a bare `*` element still would be expanded and
    // must be rejected, even when it sits next to a count(*).
    Seq(false, true).foreach { singlePass =>
      withSQLConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString) {
        val e = intercept[AnalysisException] {
          sql("SELECT json_array(count(*), *) FROM VALUES (1, 'x') AS t(a, b)")
            .queryExecution.analyzed
        }
        assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"singlePass=$singlePass")
      }
    }
  }

  test("SPARK-59243: a bare star in a nested clause-free JSON_ARRAY is rejected") {
    // An implicit-JSON sibling (`json_array(1)`) routes the outer call through
    // `UnresolvedRoutedJsonArray`; the bare `*` is rejected there, like the flat built-in rather
    // than being expanded.
    Seq(false, true).foreach { singlePass =>
      withSQLConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString) {
        val e = intercept[AnalysisException] {
          sql("SELECT json_array(*, json_array(1)) FROM VALUES (1, 'x') AS t(a, b)")
            .queryExecution.analyzed
        }
        assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"singlePass=$singlePass")
      }
    }
  }

  test("SPARK-59243: a routed nested call is checked for cyclic SQL function references") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      try {
        sql("CREATE TEMPORARY FUNCTION json_array(a STRING) RETURNS STRING RETURN 'x'")
        sql("SET PATH = system.session, system.builtin")
        // A routed nested call references the same-named function, like the flat call.
        Seq("json_array(a)", "json_array(json_array(1))").foreach { body =>
          val e = intercept[AnalysisException] {
            sql("CREATE OR REPLACE TEMPORARY FUNCTION json_array(a STRING) " +
              s"RETURNS STRING RETURN $body")
          }
          assert(e.getCondition == "CYCLIC_FUNCTION_REFERENCE", s"for $body")
        }
        // Stock-owned (clause-bearing or FORMAT JSON) bodies are not routine calls.
        Seq(
          "json_array(json_array(1) NULL ON NULL)" -> "[[1]]",
          "json_array('[1]' FORMAT JSON)" -> "[[1]]"
        ).foreach { case (body, expected) =>
          sql("CREATE OR REPLACE TEMPORARY FUNCTION json_array(a STRING) " +
            s"RETURNS STRING RETURN $body")
          checkAnswer(sql("SELECT json_array('ignored')"), Row(expected))
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS json_array")
      }
    }
  }

  test("plain JSON_ARRAY with star can be shadowed by a persistent function in PATH") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      withDatabase("path_json_array") {
        sql("CREATE DATABASE path_json_array")
        sql("CREATE FUNCTION path_json_array.json_array(a INT, b STRING) RETURNS STRING " +
          "RETURN 'persistent'")
        try {
          sql("SET PATH = spark_catalog.path_json_array, system.builtin")
          checkAnswer(
            sql("SELECT json_array(*) FROM VALUES (1, 'x') AS t(a, b)"),
            Row("persistent"))
        } finally {
          sql("SET PATH = DEFAULT_PATH")
          sql("DROP FUNCTION IF EXISTS path_json_array.json_array")
        }
      }
    }
  }

  test("nested JSON_ARRAY with star can be shadowed by a V2 function in PATH") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      "spark.sql.catalog.path_json_array_cat" -> classOf[InMemoryCatalog].getName) {
      val catalog = spark.sessionState.catalogManager.catalog("path_json_array_cat")
        .asInstanceOf[InMemoryCatalog]
      catalog.createFunction(
        Identifier.of(Array("fns"), "json_array"),
        UnboundJsonArrayShadowFunction)
      try {
        sql("SET PATH = path_json_array_cat.fns, system.builtin")
        val query = "SELECT json_array(*, json_array(1)) FROM VALUES (1, 'x') AS t(a, b)"
        // Default mode runs the dual-run comparison, so this also asserts the single-pass resolver
        // routes to the V2 owner identically to fixed-point before the result is produced.
        checkAnswer(sql(query), Row("v2 persistent"))
        // Analyze only under strict single-pass: the collect() action path needs operators the
        // single-pass analyzer cannot yet resolve, so assert the direct star expanded, the wrapper
        // resolved, and the call bound to the V2 shadow rather than executing it.
        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val analyzed = sql(query).queryExecution.analyzed
          assert(analyzed.resolved)
          assert(!analyzed.exists(_.expressions.exists(_.exists(_.isInstanceOf[Star]))))
          assert(!analyzed.exists(_.expressions.exists(_.exists(
            _.isInstanceOf[UnresolvedJsonConstructor]))))
          val shadowCalls = analyzed.expressions.flatMap(_.collect {
            case a: ApplyFunctionExpression if a.function.isInstanceOf[JsonArrayShadowFunction] => a
          })
          assert(shadowCalls.length == 1,
            s"the outer call should bind to the V2 shadow, plan:\n$analyzed")
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        catalog.clearFunctions()
      }
    }
  }

  test("plain JSON_ARRAY(*) with a temp table function shadow reports NOT_A_SCALAR_FUNCTION") {
    // A temp *table* function ahead of system.builtin makes scalar resolution terminal at that
    // entry (scalar-miss/table-hit -> NOT_A_SCALAR_FUNCTION), so the bare-* guard must not fire:
    // json_array(*) yields the same NOT_A_SCALAR_FUNCTION as json_array(1).
    val tableRegistry = spark.sessionState.tableFunctionRegistry
    val tempIdent = FunctionIdentifier(
      "json_array",
      Some(CatalogManager.SESSION_NAMESPACE),
      Some(CatalogManager.SYSTEM_CATALOG_NAME))
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      try {
        tableRegistry.createOrReplaceTempFunction(
          "json_array", _ => Range(0, 1, 1, 1), "scala_udf")
        sql("SET PATH = system.session, system.builtin")
        Seq(false, true).foreach { singlePass =>
          withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString) {
            Seq("SELECT json_array(*) FROM VALUES (1, 'x') AS t(a, b)", "SELECT json_array(1)")
              .foreach { query =>
                val e = intercept[AnalysisException] {
                  sql(query).queryExecution.analyzed
                }
                assert(e.getCondition == "NOT_A_SCALAR_FUNCTION",
                  s"singlePass=$singlePass for $query")
              }
          }
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        tableRegistry.dropFunction(tempIdent)
      }
    }
  }

  test("two-part builtin JSON_ARRAY respects persistentCatalogFirst before rejecting star") {
    withDatabase("builtin") {
      sql("CREATE DATABASE builtin")
      sql("CREATE FUNCTION builtin.json_array(a INT, b STRING) RETURNS STRING " +
        "RETURN 'persistent'")
      try {
        val query = "SELECT builtin.json_array(*) FROM VALUES (1, 'x') AS t(a, b)"
        withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "false") {
          val e = intercept[AnalysisException] {
            sql(query).collect()
          }
          assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX")
        }
        withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
          checkAnswer(sql(query), Row("persistent"))
        }
      } finally {
        sql("DROP FUNCTION IF EXISTS builtin.json_array")
      }
    }
  }

  test("routed JSON_ARRAY star guard recovers when the shadow probe hits a missing namespace") {
    // A PATH catalog whose functionExists surfaces NoSuchNamespaceException must not fail analysis:
    // persistentFunctionExists swallows it so resolution reaches system.builtin, which rejects the
    // direct star. Both analyzer paths share the probe.
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      "spark.sql.catalog.missing_ns_cat" -> classOf[MissingNamespaceFunctionCatalog].getName) {
      try {
        sql("SET PATH = missing_ns_cat.some_ns, system.builtin")
        Seq(false, true).foreach { singlePass =>
          withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString) {
            val e = intercept[AnalysisException] {
              sql("SELECT json_array(*) FROM VALUES (1, 'x') AS t(a, b)").queryExecution.analyzed
            }
            assert(e.getCondition == "INVALID_USAGE_OF_STAR_OR_REGEX", s"singlePass=$singlePass")
          }
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
      }
    }
  }

  test("view-context shadow probe expands identifiers through the view's frozen catalog") {
    // The shadow probe must mirror `resolveFunctionCandidate`'s identifier expansion. A permanent
    // view freezes its creation catalog (spark_catalog). When the view is read while a different
    // catalog is current, the star pre-check for `builtin.json_array(*)` must resolve `builtin`
    // under the view's frozen catalog -- not the reader's current catalog -- so the persistent
    // `spark_catalog.builtin.json_array` shadows the built-in and the star is NOT rejected.
    // Probing the reader's current catalog (the pre-fix behavior) misses the persistent function
    // and wrongly rejects the star.
    withSQLConf(
      "spark.sql.catalog.other_cat" -> classOf[InMemoryCatalog].getName,
      SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
      withDatabase("builtin") {
        sql("CREATE DATABASE builtin")
        sql("CREATE FUNCTION builtin.json_array(a INT, b STRING) RETURNS STRING " +
          "RETURN 'persistent'")
        try {
          sql("SET CATALOG spark_catalog")
          sql("CREATE VIEW spark_catalog.default.json_array_shadow_view AS " +
            "SELECT builtin.json_array(*) AS r FROM VALUES (1, 'x') AS t(a, b)")
          // Read the view while a different catalog is current: resolution must still find the
          // persistent function under the view's frozen spark_catalog.
          sql("SET CATALOG other_cat")
          checkAnswer(
            sql("SELECT r FROM spark_catalog.default.json_array_shadow_view"),
            Row("persistent"))
        } finally {
          sql("SET CATALOG spark_catalog")
          sql("DROP VIEW IF EXISTS spark_catalog.default.json_array_shadow_view")
          sql("DROP FUNCTION IF EXISTS builtin.json_array")
        }
      }
    }
  }

  test("SPARK-59600: a routed JSON_ARRAY shadow dropped between preprocessing and resolution " +
      "fails cleanly instead of resolving to the built-in") {
    // Star preprocessing selects the persistent shadow (functionExists = true) and expands the
    // direct star for it; by resolution time the shadow is gone (loadFunction throws). Binding the
    // selected owner makes resolution fail on that candidate rather than falling through to the
    // stock built-in, which -- with the star already expanded away -- would otherwise silently
    // accept json_array(*). A deterministic stand-in for a concurrent DROP FUNCTION between phases.
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      "spark.sql.catalog.vanishing_cat" -> classOf[VanishingShadowFunctionCatalog].getName) {
      try {
        sql("SET PATH = vanishing_cat.some_ns, system.builtin")
        Seq(false, true).foreach { singlePass =>
          withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString) {
            Seq(
              "SELECT json_array(*) FROM VALUES (1, 'x') AS t(a, b)",
              "SELECT json_array(*, json_array(1)) FROM VALUES (1, 'x') AS t(a, b)"
            ).foreach { query =>
              val e = intercept[AnalysisException] {
                sql(query).queryExecution.analyzed
              }
              assert(e.getCondition == "UNRESOLVED_ROUTINE",
                s"singlePass=$singlePass for $query")
            }
          }
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
      }
    }
  }

  test("default collation recurses into a nested JSON_ARRAY value") {
    // Col a (nested clause-free call, routed via `UnresolvedRoutedJsonArray`) and col b (flat
    // routed built-in) both adopt the table default UTF8_LCASE collation.
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withTable("t") {
        sql(
          """CREATE TABLE t DEFAULT COLLATION UTF8_LCASE AS
            |SELECT json_array(json_array(1)) AS a,
            |       builtin.json_array(1) AS b""".stripMargin)
        assert(spark.table("t").schema("a").dataType === StringType("UTF8_LCASE"))
        assert(spark.table("t").schema("b").dataType === StringType("UTF8_LCASE"))
        checkAnswer(spark.table("t"), Row("[[1]]", "[1]"))
      }
    }
  }

  test("SPARK-59243: routed/qualified nested JSON_ARRAY splices under a default collation") {
    // Both columns resolve through the built-in builder (not the direct grammar path): col a via
    // the qualified outer call, col b via the qualified nested call. `ApplyDefaultCollation` wraps
    // the nested producer in a collation Cast; the builder must still see through it (an analyzer
    // Cast, not a user one) and splice, so the result matches the UTF8_BINARY case rather than
    // quoting.
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withTable("t") {
        sql(
          """CREATE TABLE t DEFAULT COLLATION UTF8_LCASE AS
            |SELECT builtin.json_array(json_array(1)) AS a,
            |       json_array(builtin.json_array(1)) AS b,
            |       json_array(json_array(0), builtin.json_array(1) COLLATE UNICODE) AS c,
            |       builtin.json_array(json_array(1) COLLATE UNICODE) AS d""".stripMargin)
        checkAnswer(spark.table("t"), Row("[[1]]", "[[1]]", "[[0],[1]]", "[[1]]"))
      }
    }
  }

  test("SPARK-59243: routed/qualified nested JSON_ARRAY splices under a view default collation") {
    // Reading the view re-analyzes it (under dual-run in tests), so both analyzers must see
    // through their own default-collation casts, including one under a user COLLATE.
    withSQLConf(SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true") {
      withView("v") {
        sql(
          """CREATE VIEW v DEFAULT COLLATION UTF8_LCASE AS
            |SELECT builtin.json_array(json_array(1)) AS a,
            |       json_array(builtin.json_array(1)) AS b,
            |       json_array(json_array(1)) AS c,
            |       builtin.json_array(json_array(1) COLLATE UNICODE) AS d""".stripMargin)
        checkAnswer(spark.table("v"), Row("[[1]]", "[[1]]", "[[1]]", "[[1]]"))
        val schema = spark.table("v").schema
        Seq("a", "b", "c").foreach { c =>
          assert(schema(c).dataType === StringType("UTF8_LCASE"), s"for $c")
        }
      }
    }
  }

  test("SPARK-59243: a shadowed nested call keeps the owner's type under a default collation") {
    // A routed wrapper stands for an ordinary call until its owner is known, so the default
    // collation must not cast a shadow's result: the nested call matches the flat call.
    withSQLConf(
      SQLConf.OBJECT_LEVEL_COLLATIONS_ENABLED.key -> "true",
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "second") {
      Seq(
        // The inner call is a direct constructor argument, so it stays stock (`[1]`, length 3).
        "INT" -> "length(a)" -> Row(3, 1),
        "STRING" -> "'shadowed'" -> Row("shadowed", "shadowed")
      ).foreach { case ((returnType, body), expected) =>
        try {
          sql(s"CREATE TEMPORARY FUNCTION json_array(a STRING) RETURNS $returnType RETURN $body")
          sql("SET PATH = system.session, system.builtin")
          withTable("t") {
            sql("CREATE TABLE t DEFAULT COLLATION UTF8_LCASE AS " +
              "SELECT json_array(json_array(1)) AS nested, json_array(1) AS flat")
            // Read back on the default path, which reaches the current schema.
            sql("SET PATH = DEFAULT_PATH")
            val schema = spark.table("t").schema
            assert(schema("nested").dataType === schema("flat").dataType, s"for $returnType")
            checkAnswer(spark.table("t"), expected)
          }
        } finally {
          sql("SET PATH = DEFAULT_PATH")
          sql("DROP TEMPORARY FUNCTION IF EXISTS json_array")
        }
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

  test("SPARK-59243: deeply nested JSON constructors finalize in a bounded number of passes") {
    // ResolveFunctions finalizes nested wrappers in one pass, so depth must not scale
    // analyzer iterations: nest well past a deliberately low maxIterations for both constructors.
    val depth = 60
    withSQLConf(SQLConf.ANALYZER_MAX_ITERATIONS.key -> "30") {
      val array = s"${"JSON_ARRAY(" * depth}1${")" * depth}"
      checkAnswer(sql(s"SELECT $array"), Row(("[" * depth) + "1" + ("]" * depth)))
      val obj = s"${"JSON_OBJECT('k' VALUE " * depth}1${")" * depth}"
      checkAnswer(sql(s"SELECT $obj"), Row(("{\"k\":" * depth) + "1" + ("}" * depth)))
    }
  }

  test("SPARK-59243: ordinary functions between JSON constructor levels resolve in one pass") {
    // The wrapper materializes in the same bottom-up pass as the calls around it, so alternating
    // a function and a stock-owned constructor does not add an iteration per level. `length`
    // keeps every level's value fixed: `[3]` and `{"k":7}`.
    val depth = 40
    withSQLConf(SQLConf.ANALYZER_MAX_ITERATIONS.key -> "20") {
      val array = "JSON_ARRAY(length(" * depth + "JSON_ARRAY(1 RETURNING STRING)" +
        ") RETURNING STRING)" * depth
      checkAnswer(sql(s"SELECT $array"), Row("[3]"))
      val obj = "JSON_OBJECT('k' VALUE length(" * depth + "JSON_OBJECT('k' VALUE 1 NULL ON NULL)" +
        ") NULL ON NULL)" * depth
      checkAnswer(sql(s"SELECT $obj"), Row("""{"k":7}"""))
    }
  }

  test("SPARK-59243: a late-resolving child adds no analyzer iteration to a JSON constructor") {
    // `date_format` resolves only once ResolveTimeZone runs, late in an iteration. The constructor
    // must then materialize before aliases are assigned, so it costs no more Resolution iterations
    // than an ordinary function over the same child.
    def resolveAliasesInvocations(selectExpr: String): Long = {
      val df = sql(s"SELECT $selectExpr FROM VALUES (TIMESTAMP'2024-01-01 00:00:00') AS t(ts)")
      df.queryExecution.analyzed
      df.queryExecution.tracker.rules.collectFirst {
        case (name, summary) if name.endsWith("ResolveAliases") => summary.numInvocations
      }.get
    }
    val baseline = resolveAliasesInvocations("concat('d', date_format(ts, 'yyyy'))")
    Seq(
      "JSON_OBJECT('d' VALUE date_format(ts, 'yyyy') NULL ON NULL)",
      "JSON_ARRAY(date_format(ts, 'yyyy') RETURNING STRING)",
      "JSON_ARRAY(CAST(ts AS STRING) NULL ON NULL)"
    ).foreach { e =>
      assert(resolveAliasesInvocations(e) <= baseline, s"for $e")
    }
  }

}

/**
 * A [[org.apache.spark.sql.connector.catalog.FunctionCatalog]] whose `loadFunction` reports a
 * missing namespace, so the default `functionExists` propagates [[NoSuchNamespaceException]] --
 * used to exercise the shadow probe's namespace-error recovery.
 */
class MissingNamespaceFunctionCatalog extends InMemoryCatalog {
  override def loadFunction(ident: Identifier): UnboundFunction =
    throw new NoSuchNamespaceException(ident.namespace)
}

/**
 * A [[org.apache.spark.sql.connector.catalog.FunctionCatalog]] that reports a function as existing
 * but fails to load it -- a deterministic stand-in for a shadow dropped between star preprocessing
 * and routine resolution.
 */
class VanishingShadowFunctionCatalog extends InMemoryCatalog {
  override def functionExists(ident: Identifier): Boolean = true
  override def loadFunction(ident: Identifier): UnboundFunction =
    throw new NoSuchFunctionException(ident)
}

object UnboundJsonArrayShadowFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size != 3) {
      throw new UnsupportedOperationException(
        s"expected expanded star plus nested value, got $inputType")
    }
    JsonArrayShadowFunction(inputType.fields.map(_.dataType).toSeq)
  }

  override def description(): String = "test-only json_array shadow"
  override def name(): String = "json_array"
}

// The input types are held as a Seq (not Array) so the case class has value-based equality and
// toString: the dual-run analyzer binds the function once per pass and compares the two plans, and
// an Array field would render its identity hash and spuriously mismatch.
case class JsonArrayShadowFunction(inputTypesSeq: Seq[DataType])
    extends ScalarFunction[UTF8String] {
  override def inputTypes(): Array[DataType] = inputTypesSeq.toArray
  override def resultType(): DataType = StringType
  override def name(): String = "json_array_shadow"
  override def canonicalName(): String = name()
  override def produceResult(input: InternalRow): UTF8String =
    UTF8String.fromString("v2 persistent")
}
