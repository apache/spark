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
package org.apache.spark.sql.catalyst.parser

import java.sql.{Date, Timestamp}
import java.time.{Duration, LocalDateTime, Period}
import java.util.concurrent.TimeUnit

import scala.language.implicitConversions

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, _}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Last}
import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.TimestampTypes
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{DayTimeIntervalType => DT, YearMonthIntervalType => YM}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Test basic expression parsing.
 * If the type of an expression is supported it should be tested here.
 *
 * Please note that some of the expressions test don't have to be sound expressions, only their
 * structure needs to be valid. Unsound expressions should be caught by the Analyzer or
 * CheckAnalysis classes.
 */
class ExpressionParserSuite extends AnalysisTest {
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  implicit def stringToUTF8Str(str: String): UTF8String = UTF8String.fromString(str)

  val defaultParser = CatalystSqlParser

  def assertEqual(
      sqlCommand: String,
      e: Expression,
      parser: ParserInterface = defaultParser): Unit = {
    compareExpressions(parser.parseExpression(sqlCommand), e)
  }

  private def parseException(sqlText: String): SparkThrowable = {
    super.parseException(defaultParser.parseExpression)(sqlText)
  }

  def assertEval(
      sqlCommand: String,
      expect: Any,
      parser: ParserInterface = defaultParser): Unit = {
    assert(parser.parseExpression(sqlCommand).eval() === expect)
  }

  test("star expressions") {
    // Global Star
    assertEqual("*", UnresolvedStar(None))

    // Targeted Star
    assertEqual("a.b.*", UnresolvedStar(Option(Seq("a", "b"))))
  }

  // NamedExpression (Alias/Multialias)
  test("named expressions") {
    // No Alias
    val r0 = $"a"
    assertEqual("a", r0)

    // Single Alias.
    val r1 = $"a" as "b"
    assertEqual("a as b", r1)
    assertEqual("a b", r1)

    // Multi-Alias
    assertEqual("a as (b, c)", MultiAlias($"a", Seq("b", "c")))
    assertEqual("a() (b, c)", MultiAlias($"a".function(), Seq("b", "c")))

    // Numeric literals without a space between the literal qualifier and the alias, should not be
    // interpreted as such. An unresolved reference should be returned instead.
    // TODO add the JIRA-ticket number.
    assertEqual("1SL", $"1SL")

    // Aliased star is allowed.
    assertEqual("a.* b", UnresolvedStar(Option(Seq("a"))) as "b")
  }

  test("binary logical expressions") {
    // And
    assertEqual("a and b", $"a" && $"b")

    // Or
    assertEqual("a or b", $"a" || $"b")

    // Combination And/Or check precedence
    assertEqual("a and b or c and d", ($"a" && $"b") || ($"c" && $"d"))
    assertEqual("a or b or c and d", $"a" || $"b" || ($"c" && $"d"))

    // Multiple AND/OR get converted into a balanced tree
    assertEqual("a or b or c or d or e or f", (($"a" || $"b") || $"c") || (($"d" || $"e") || $"f"))
    assertEqual("a and b and c and d and e and f", (($"a" && $"b") && $"c")
      && (($"d" && $"e") && $"f"))
  }

  test("long binary logical expressions") {
    def testVeryBinaryExpression(op: String, clazz: Class[_]): Unit = {
      val sql = (1 to 1000).map(x => s"$x == $x").mkString(op)
      val e = defaultParser.parseExpression(sql)
      assert(e.collect { case _: EqualTo => true }.size === 1000)
      assert(e.collect { case x if clazz.isInstance(x) => true }.size === 999)
    }
    testVeryBinaryExpression(" AND ", classOf[And])
    testVeryBinaryExpression(" OR ", classOf[Or])
  }

  test("not expressions") {
    assertEqual("not a", !$"a")
    assertEqual("!a", !$"a")
    assertEqual("not true > true", Not(GreaterThan(true, true)))
  }

  test("exists expression") {
    assertEqual(
      "exists (select 1 from b where b.x = a.x)",
      Exists(table("b").where($"b.x" === $"a.x").select(1)))
  }

  test("comparison expressions") {
    assertEqual("a = b", $"a" === $"b")
    assertEqual("a == b", $"a" === $"b")
    assertEqual("a <=> b", $"a" <=> $"b")
    assertEqual("a <> b", $"a" =!= $"b")
    assertEqual("a != b", $"a" =!= $"b")
    assertEqual("a < b", $"a" < $"b")
    assertEqual("a <= b", $"a" <= $"b")
    assertEqual("a !> b", $"a" <= $"b")
    assertEqual("a > b", $"a" > $"b")
    assertEqual("a >= b", $"a" >= $"b")
    assertEqual("a !< b", $"a" >= $"b")
  }

  test("between expressions") {
    assertEqual("a between b and c", $"a" >= $"b" && $"a" <= $"c")
    assertEqual("a not between b and c", !($"a" >= $"b" && $"a" <= $"c"))
  }

  test("in expressions") {
    assertEqual("a in (b, c, d)", $"a" in ($"b", $"c", $"d"))
    assertEqual("a not in (b, c, d)", !($"a" in ($"b", $"c", $"d")))
  }

  test("in sub-query") {
    assertEqual(
      "a in (select b from c)",
      InSubquery(Seq($"a"), ListQuery(table("c").select($"b"))))

    assertEqual(
      "(a, b, c) in (select d, e, f from g)",
      InSubquery(Seq($"a", $"b", $"c"), ListQuery(table("g").select($"d", $"e", $"f"))))

    assertEqual(
      "(a, b) in (select c from d)",
      InSubquery(Seq($"a", $"b"), ListQuery(table("d").select($"c"))))

    assertEqual(
      "(a) in (select b from c)",
      InSubquery(Seq($"a"), ListQuery(table("c").select($"b"))))
  }

  test("like expressions") {
    assertEqual("a like 'pattern%'", $"a" like "pattern%")
    assertEqual("a not like 'pattern%'", !($"a" like "pattern%"))
    assertEqual("a rlike 'pattern%'", $"a" rlike "pattern%")
    assertEqual("a not rlike 'pattern%'", !($"a" rlike "pattern%"))
    assertEqual("a regexp 'pattern%'", $"a" rlike "pattern%")
    assertEqual("a not regexp 'pattern%'", !($"a" rlike "pattern%"))
  }

  test("like escape expressions") {
    val message = "Escape string must contain only one character."
    assertEqual("a like 'pattern%' escape '#'", $"a".like("pattern%", '#'))
    assertEqual("a like 'pattern%' escape '\"'", $"a".like("pattern%", '\"'))

    checkError(
      exception = parseException("a like 'pattern%' escape '##'"),
      errorClass = "_LEGACY_ERROR_TEMP_0017",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "like 'pattern%' escape '##'",
        start = 2,
        stop = 28))

    checkError(
      exception = parseException("a like 'pattern%' escape ''"),
      errorClass = "_LEGACY_ERROR_TEMP_0017",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "like 'pattern%' escape ''",
        start = 2,
        stop = 26))

    assertEqual("a not like 'pattern%' escape '#'", !($"a".like("pattern%", '#')))
    assertEqual("a not like 'pattern%' escape '\"'", !($"a".like("pattern%", '\"')))

    checkError(
      exception = parseException("a not like 'pattern%' escape '\"/'"),
      errorClass = "_LEGACY_ERROR_TEMP_0017",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "not like 'pattern%' escape '\"/'",
        start = 2,
        stop = 32))

    checkError(
      exception = parseException("a not like 'pattern%' escape ''"),
      errorClass = "_LEGACY_ERROR_TEMP_0017",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "not like 'pattern%' escape ''",
        start = 2,
        stop = 30))
  }

  test("like expressions with ESCAPED_STRING_LITERALS = true") {
    withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
      val parser = new CatalystSqlParser()
      assertEqual("a rlike '^\\x20[\\x20-\\x23]+$'", $"a" rlike "^\\x20[\\x20-\\x23]+$", parser)
      assertEqual("a rlike 'pattern\\\\'", $"a" rlike "pattern\\\\", parser)
      assertEqual("a rlike 'pattern\\t\\n'", $"a" rlike "pattern\\t\\n", parser)
    }
  }

  test("(NOT) LIKE (ANY | SOME | ALL) expressions") {
    Seq("any", "some").foreach { quantifier =>
      assertEqual(s"a like $quantifier ('foo%', 'b%')", $"a" likeAny("foo%", "b%"))
      assertEqual(s"a not like $quantifier ('foo%', 'b%')", $"a" notLikeAny("foo%", "b%"))
      assertEqual(s"not (a like $quantifier ('foo%', 'b%'))", !($"a" likeAny("foo%", "b%")))
    }
    assertEqual("a like all ('foo%', 'b%')", $"a" likeAll("foo%", "b%"))
    assertEqual("a not like all ('foo%', 'b%')", $"a" notLikeAll("foo%", "b%"))
    assertEqual("not (a like all ('foo%', 'b%'))", !($"a" likeAll("foo%", "b%")))

    Seq("any", "some", "all").foreach { quantifier =>
      checkError(
        exception = parseException(s"a like $quantifier()"),
        errorClass = "_LEGACY_ERROR_TEMP_0064",
        parameters = Map("msg" -> "Expected something between '(' and ')'."),
        context = ExpectedContext(
          fragment = s"like $quantifier()",
          start = 2,
          stop = 8 + quantifier.length))
    }
  }

  test("is null expressions") {
    assertEqual("a is null", $"a".isNull)
    assertEqual("a is not null", $"a".isNotNull)
    assertEqual("a = b is null", ($"a" === $"b").isNull)
    assertEqual("a = b is not null", ($"a" === $"b").isNotNull)
  }

  test("is distinct expressions") {
    assertEqual("a is distinct from b", !($"a" <=> $"b"))
    assertEqual("a is not distinct from b", $"a" <=> $"b")
  }

  test("binary arithmetic expressions") {
    // Simple operations
    assertEqual("a * b", $"a" * $"b")
    assertEqual("a / b", $"a" / $"b")
    assertEqual("a DIV b", $"a" div $"b")
    assertEqual("a % b", $"a" % $"b")
    assertEqual("a + b", $"a" + $"b")
    assertEqual("a - b", $"a" - $"b")
    assertEqual("a & b", $"a" & $"b")
    assertEqual("a ^ b", $"a" ^ $"b")
    assertEqual("a | b", $"a" | $"b")

    // Check precedences
    assertEqual(
      "a * t | b ^ c & d - e + f % g DIV h / i * k",
      $"a" * $"t" | ($"b" ^ ($"c" & ($"d" - $"e" + (($"f" % $"g" div $"h") / $"i" * $"k")))))
  }

  test("unary arithmetic expressions") {
    assertEqual("+a", +$"a")
    assertEqual("-a", -$"a")
    assertEqual("~a", ~$"a")
    assertEqual("-+~~a", -( +(~(~$"a"))))
  }

  test("cast expressions") {
    // Note that DataType parsing is tested elsewhere.
    assertEqual("cast(a as int)", $"a".cast(IntegerType))
    assertEqual("cast(a as timestamp)", $"a".cast(TimestampType))
    assertEqual("cast(a as array<int>)", $"a".cast(ArrayType(IntegerType)))
    assertEqual("cast(cast(a as int) as long)", $"a".cast(IntegerType).cast(LongType))
  }

  test("function expressions") {
    assertEqual("foo()", $"foo".function())
    assertEqual("foo.bar()",
      UnresolvedFunction(FunctionIdentifier("bar", Some("foo")), Seq.empty, isDistinct = false))
    assertEqual("foo(*)", $"foo".function(star()))
    assertEqual("count(*)", $"count".function(1))
    assertEqual("foo(a, b)", $"foo".function($"a", $"b"))
    assertEqual("foo(all a, b)", $"foo".function($"a", $"b"))
    assertEqual("foo(distinct a, b)", $"foo".distinctFunction($"a", $"b"))
    assertEqual("grouping(distinct a, b)", $"grouping".distinctFunction($"a", $"b"))
    assertEqual("`select`(all a, b)", $"select".function($"a", $"b"))
    checkError(
      exception = parseException("foo(a x)"),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'x'", "hint" -> ": extra input 'x'"))
  }

  private def lv(s: Symbol) = UnresolvedNamedLambdaVariable(Seq(s.name))

  test("lambda functions") {
    assertEqual("x -> x + 1", LambdaFunction(lv(Symbol("x")) + 1, Seq(lv(Symbol("x")))))
    assertEqual("(x, y) -> x + y", LambdaFunction(lv(Symbol("x")) + lv(Symbol("y")),
      Seq(lv(Symbol("x")), lv(Symbol("y")))))
  }

  test("window function expressions") {
    val func = $"foo".function(star())
    def windowed(
        partitioning: Seq[Expression] = Seq.empty,
        ordering: Seq[SortOrder] = Seq.empty,
        frame: WindowFrame = UnspecifiedFrame): Expression = {
      WindowExpression(func, WindowSpecDefinition(partitioning, ordering, frame))
    }

    // Basic window testing.
    assertEqual("foo(*) over w1", UnresolvedWindowExpression(func, WindowSpecReference("w1")))
    assertEqual("foo(*) over ()", windowed())
    assertEqual("foo(*) over (partition by a, b)", windowed(Seq($"a", $"b")))
    assertEqual("foo(*) over (distribute by a, b)", windowed(Seq($"a", $"b")))
    assertEqual("foo(*) over (cluster by a, b)", windowed(Seq($"a", $"b")))
    assertEqual("foo(*) over (order by a desc, b asc)",
      windowed(Seq.empty, Seq($"a".desc, $"b".asc)))
    assertEqual("foo(*) over (sort by a desc, b asc)",
      windowed(Seq.empty, Seq($"a".desc, $"b".asc)))
    assertEqual("foo(*) over (partition by a, b order by c)",
      windowed(Seq($"a", $"b"), Seq($"c".asc)))
    assertEqual("foo(*) over (distribute by a, b sort by c)",
      windowed(Seq($"a", $"b"), Seq($"c".asc)))

    // Test use of expressions in window functions.
    assertEqual(
      "sum(product + 1) over (partition by ((product) + (1)) order by 2)",
      WindowExpression($"sum".function($"product" + 1),
        WindowSpecDefinition(Seq($"product" + 1), Seq(Literal(2).asc), UnspecifiedFrame)))
    assertEqual(
      "sum(product + 1) over (partition by ((product / 2) + 1) order by 2)",
      WindowExpression($"sum".function($"product" + 1),
        WindowSpecDefinition(Seq($"product" / 2 + 1), Seq(Literal(2).asc), UnspecifiedFrame)))
  }

  test("range/rows window function expressions") {
    val func = $"foo".function(star())
    def windowed(
        partitioning: Seq[Expression] = Seq.empty,
        ordering: Seq[SortOrder] = Seq.empty,
        frame: WindowFrame = UnspecifiedFrame): Expression = {
      WindowExpression(func, WindowSpecDefinition(partitioning, ordering, frame))
    }

    val frameTypes = Seq(("rows", RowFrame), ("range", RangeFrame))
    val boundaries = Seq(
      // No between combinations
      ("unbounded preceding", UnboundedPreceding, CurrentRow),
      ("2147483648 preceding", -Literal(2147483648L), CurrentRow),
      ("10 preceding", -Literal(10), CurrentRow),
      ("3 + 1 preceding", -Add(Literal(3), Literal(1)), CurrentRow),
      ("0 preceding", -Literal(0), CurrentRow),
      ("current row", CurrentRow, CurrentRow),
      ("0 following", Literal(0), CurrentRow),
      ("3 + 1 following", Add(Literal(3), Literal(1)), CurrentRow),
      ("10 following", Literal(10), CurrentRow),
      ("2147483649 following", Literal(2147483649L), CurrentRow),
      ("unbounded following", UnboundedFollowing, CurrentRow), // Will fail during analysis

      // Between combinations
      ("between unbounded preceding and 5 following",
        UnboundedPreceding, Literal(5)),
      ("between unbounded preceding and 3 + 1 following",
        UnboundedPreceding, Add(Literal(3), Literal(1))),
      ("between unbounded preceding and 2147483649 following",
        UnboundedPreceding, Literal(2147483649L)),
      ("between unbounded preceding and current row", UnboundedPreceding, CurrentRow),
      ("between 2147483648 preceding and current row", -Literal(2147483648L), CurrentRow),
      ("between 10 preceding and current row", -Literal(10), CurrentRow),
      ("between 3 + 1 preceding and current row", -Add(Literal(3), Literal(1)), CurrentRow),
      ("between 0 preceding and current row", -Literal(0), CurrentRow),
      ("between current row and current row", CurrentRow, CurrentRow),
      ("between current row and 0 following", CurrentRow, Literal(0)),
      ("between current row and 5 following", CurrentRow, Literal(5)),
      ("between current row and 3 + 1 following", CurrentRow, Add(Literal(3), Literal(1))),
      ("between current row and 2147483649 following", CurrentRow, Literal(2147483649L)),
      ("between current row and unbounded following", CurrentRow, UnboundedFollowing),
      ("between 2147483648 preceding and unbounded following",
        -Literal(2147483648L), UnboundedFollowing),
      ("between 10 preceding and unbounded following",
        -Literal(10), UnboundedFollowing),
      ("between 3 + 1 preceding and unbounded following",
        -Add(Literal(3), Literal(1)), UnboundedFollowing),
      ("between 0 preceding and unbounded following", -Literal(0), UnboundedFollowing),

      // Between partial and full range
      ("between 10 preceding and 5 following", -Literal(10), Literal(5)),
      ("between unbounded preceding and unbounded following",
        UnboundedPreceding, UnboundedFollowing)
    )
    frameTypes.foreach {
      case (frameTypeSql, frameType) =>
        boundaries.foreach {
          case (boundarySql, begin, end) =>
            val query = s"foo(*) over (partition by a order by b $frameTypeSql $boundarySql)"
            val expr = windowed(Seq($"a"), Seq($"b".asc),
              SpecifiedWindowFrame(frameType, begin, end))
            assertEqual(query, expr)
        }
    }

    // We cannot use an arbitrary expression.
    checkError(
      exception = parseException("foo(*) over (partition by a order by b rows exp(b) preceding)"),
      errorClass = "_LEGACY_ERROR_TEMP_0064",
      parameters = Map("msg" -> "Frame bound value must be a literal."),
      context = ExpectedContext(
        fragment = "exp(b) preceding",
        start = 44,
        stop = 59))
  }

  test("row constructor") {
    // Note that '(a)' will be interpreted as a nested expression.
    assertEqual("(a, b)", CreateStruct(Seq($"a", $"b")))
    assertEqual("(a, b, c)", CreateStruct(Seq($"a", $"b", $"c")))
    assertEqual("(a as b, b as c)", CreateStruct(Seq($"a" as "b", $"b" as "c")))
  }

  test("scalar sub-query") {
    assertEqual(
      "(select max(val) from tbl) > current",
      ScalarSubquery(table("tbl").select($"max".function($"val"))) > $"current")
    assertEqual(
      "a = (select b from s)",
      $"a" === ScalarSubquery(table("s").select($"b")))
  }

  test("case when") {
    assertEqual("case a when 1 then b when 2 then c else d end",
      CaseKeyWhen($"a", Seq(1, $"b", 2, $"c", $"d")))
    assertEqual("case (a or b) when true then c when false then d else e end",
      CaseKeyWhen($"a" || $"b", Seq(true, $"c", false, $"d", $"e")))
    assertEqual("case 'a'='a' when true then 1 end",
      CaseKeyWhen("a" ===  "a", Seq(true, 1)))
    assertEqual("case when a = 1 then b when a = 2 then c else d end",
      CaseWhen(Seq(($"a" === 1, $"b".expr), ($"a" === 2, $"c".expr)), $"d"))
    assertEqual("case when (1) + case when a > b then c else d end then f else g end",
      CaseWhen(Seq((Literal(1)
        + CaseWhen(Seq(($"a" > $"b", $"c".expr)), $"d".expr), $"f".expr)), $"g"))
  }

  test("dereference") {
    assertEqual("a.b", UnresolvedAttribute("a.b"))
    assertEqual("`select`.b", UnresolvedAttribute("select.b"))
    assertEqual("(a + b).b", ($"a" + $"b").getField("b")) // This will fail analysis.
    assertEqual(
      "struct(a, b).b",
      namedStruct(Literal("a"), $"a", Literal("b"), $"b").getField("b"))
  }

  test("reference") {
    // Regular
    assertEqual("a", $"a")

    // Starting with a digit.
    assertEqual("1a", $"1a")

    // Quoted using a keyword.
    assertEqual("`select`", $"select")

    // Unquoted using an unreserved keyword.
    assertEqual("columns", $"columns")
  }

  test("subscript") {
    assertEqual("a[b]", $"a".getItem($"b"))
    assertEqual("a[1 + 1]", $"a".getItem(Literal(1) + 1))
    assertEqual("`c`.a[b]", UnresolvedAttribute("c.a").getItem($"b"))
  }

  test("parenthesis") {
    assertEqual("(a)", $"a")
    assertEqual("r * (a + b)", $"r" * ($"a" + $"b"))
  }

  test("type constructors") {
    def checkTimestampNTZAndLTZ(): Unit = {
      // Timestamp with local time zone
      assertEqual("tImEstAmp_LTZ '2016-03-11 20:54:00.000'",
        Literal(Timestamp.valueOf("2016-03-11 20:54:00.000")))
      checkError(
        exception = parseException("timestamP_LTZ '2016-33-11 20:54:00.000'"),
        errorClass = "_LEGACY_ERROR_TEMP_0019",
        parameters = Map("valueType" -> "TIMESTAMP_LTZ", "value" -> "2016-33-11 20:54:00.000"),
        context = ExpectedContext(
          fragment = "timestamP_LTZ '2016-33-11 20:54:00.000'",
          start = 0,
          stop = 38))

      // Timestamp without time zone
      assertEqual("tImEstAmp_Ntz '2016-03-11 20:54:00.000'",
        Literal(LocalDateTime.parse("2016-03-11T20:54:00.000")))
      checkError(
        exception = parseException("tImEstAmp_Ntz '2016-33-11 20:54:00.000'"),
        errorClass = "_LEGACY_ERROR_TEMP_0019",
        parameters = Map("valueType" -> "TIMESTAMP_NTZ", "value" -> "2016-33-11 20:54:00.000"),
        context = ExpectedContext(
          fragment = "tImEstAmp_Ntz '2016-33-11 20:54:00.000'",
          start = 0,
          stop = 38))
    }

    // Dates.
    assertEqual("dAte '2016-03-11'", Literal(Date.valueOf("2016-03-11")))
    checkError(
      exception = parseException("DAtE 'mar 11 2016'"),
      errorClass = "_LEGACY_ERROR_TEMP_0019",
      parameters = Map("valueType" -> "DATE", "value" -> "mar 11 2016"),
      context = ExpectedContext(
        fragment = "DAtE 'mar 11 2016'",
        start = 0,
        stop = 17))

    // Timestamps.
    assertEqual("tImEstAmp '2016-03-11 20:54:00.000'",
      Literal(Timestamp.valueOf("2016-03-11 20:54:00.000")))
    checkError(
      exception = parseException("timestamP '2016-33-11 20:54:00.000'"),
      errorClass = "_LEGACY_ERROR_TEMP_0019",
      parameters = Map("valueType" -> "TIMESTAMP", "value" -> "2016-33-11 20:54:00.000"),
      context = ExpectedContext(
        fragment = "timestamP '2016-33-11 20:54:00.000'",
        start = 0,
        stop = 34))

    checkTimestampNTZAndLTZ()
    withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> TimestampTypes.TIMESTAMP_NTZ.toString) {
      assertEqual("tImEstAmp '2016-03-11 20:54:00.000'",
        Literal(LocalDateTime.parse("2016-03-11T20:54:00.000")))

      checkError(
        exception = parseException("timestamP '2016-33-11 20:54:00.000'"),
        errorClass = "_LEGACY_ERROR_TEMP_0019",
        parameters = Map("valueType" -> "TIMESTAMP", "value" -> "2016-33-11 20:54:00.000"),
        context = ExpectedContext(
          fragment = "timestamP '2016-33-11 20:54:00.000'",
          start = 0,
          stop = 34))

      // If the timestamp string contains time zone, return a timestamp with local time zone literal
      assertEqual("tImEstAmp '1970-01-01 00:00:00.000 +01:00'",
        Literal(-3600000000L, TimestampType))

      // The behavior of TIMESTAMP_NTZ and TIMESTAMP_LTZ is independent of SQLConf.TIMESTAMP_TYPE
      checkTimestampNTZAndLTZ()
    }
    // Interval.
    val ymIntervalLiteral = Literal.create(Period.of(1, 2, 0), YearMonthIntervalType())
    assertEqual("InterVal 'interval 1 year 2 month'", ymIntervalLiteral)
    assertEqual("INTERVAL '1 year 2 month'", ymIntervalLiteral)
    checkError(
      exception = parseException("Interval 'interval 1 yearsss 2 monthsss'"),
      errorClass = "_LEGACY_ERROR_TEMP_0020",
      parameters = Map("value" -> "interval 1 yearsss 2 monthsss"),
      context = ExpectedContext(
        fragment = "Interval 'interval 1 yearsss 2 monthsss'",
        start = 0,
        stop = 39))

    assertEqual("-interval '1 year 2 month'", UnaryMinus(ymIntervalLiteral))
    val dtIntervalLiteral = Literal.create(
      Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4).plusMillis(5).plusNanos(6000))
    assertEqual("InterVal 'interval 1 day 2 hour 3 minute 4.005006 second'", dtIntervalLiteral)
    assertEqual("INTERVAL '1 day 2 hour 3 minute 4.005006 second'", dtIntervalLiteral)
    checkError(
      exception = parseException("Interval 'interval 1 daysss 2 hoursss'"),
      errorClass = "_LEGACY_ERROR_TEMP_0020",
      parameters = Map("value" -> "interval 1 daysss 2 hoursss"),
      context = ExpectedContext(
        fragment = "Interval 'interval 1 daysss 2 hoursss'",
        start = 0,
        stop = 37))

    assertEqual("-interval '1 day 2 hour 3 minute 4.005006 second'", UnaryMinus(dtIntervalLiteral))
    checkError(
      exception = parseException("INTERVAL '1 year 2 second'"),
      errorClass = "_LEGACY_ERROR_TEMP_0029",
      parameters = Map("literal" -> "INTERVAL '1 year 2 second'"),
      context = ExpectedContext(
        fragment = "INTERVAL '1 year 2 second'",
        start = 0,
        stop = 25))

    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      val intervalLiteral = Literal(IntervalUtils.stringToInterval("interval 3 month 1 hour"))
      assertEqual("InterVal 'interval 3 month 1 hour'", intervalLiteral)
      assertEqual("INTERVAL '3 month 1 hour'", intervalLiteral)
      checkError(
        exception = parseException("Interval 'interval 3 monthsss 1 hoursss'"),
        errorClass = "_LEGACY_ERROR_TEMP_0020",
        parameters = Map("value" -> "interval 3 monthsss 1 hoursss"),
        context = ExpectedContext(
          fragment = "Interval 'interval 3 monthsss 1 hoursss'",
          start = 0,
          stop = 39))
      assertEqual(
        "-interval '3 month 1 hour'",
        UnaryMinus(Literal(IntervalUtils.stringToInterval("interval 3 month 1 hour"))))
      val intervalStrWithAllUnits = "1 year 3 months 2 weeks 2 days 1 hour 3 minutes 2 seconds " +
        "100 millisecond 200 microseconds"
      assertEqual(
        s"interval '$intervalStrWithAllUnits'",
        Literal(IntervalUtils.stringToInterval(intervalStrWithAllUnits)))
    }

    // Binary.
    assertEqual("X'A'", Literal(Array(0x0a).map(_.toByte)))
    assertEqual("x'A10C'", Literal(Array(0xa1, 0x0c).map(_.toByte)))
    checkError(
      exception = parseException("x'A1OC'"),
      errorClass = "_LEGACY_ERROR_TEMP_0022",
      parameters = Map("msg" -> "contains illegal character for hexBinary: A1OC"),
      context = ExpectedContext(
        fragment = "x'A1OC'",
        start = 0,
        stop = 6))

    checkError(
      exception = parseException("GEO '(10,-6)'"),
      errorClass = "_LEGACY_ERROR_TEMP_0021",
      parameters = Map("valueType" -> "GEO"),
      context = ExpectedContext(
        fragment = "GEO '(10,-6)'",
        start = 0,
        stop = 12))
  }

  test("literals") {
    def testDecimal(value: String): Unit = {
      assertEqual(value, Literal(BigDecimal(value).underlying))
    }

    // NULL
    assertEqual("null", Literal(null))

    // Boolean
    assertEqual("trUe", Literal(true))
    assertEqual("False", Literal(false))

    // Integral should have the narrowest possible type
    assertEqual("787324", Literal(787324))
    assertEqual("7873247234798249234", Literal(7873247234798249234L))
    testDecimal("78732472347982492793712334")

    // Decimal
    testDecimal("7873247234798249279371.2334")

    // SPARK-29956: Scientific Decimal is parsed as Double by default.
    assertEqual("9.0e1", Literal(90.toDouble))
    assertEqual(".9e+2", Literal(90.toDouble))
    assertEqual("0.9e+2", Literal(90.toDouble))

    // Scientific Decimal with suffix BD should still be parsed as Decimal
    assertEqual("900e-1BD", Literal(BigDecimal("900e-1").underlying()))
    assertEqual("900.0E-1BD", Literal(BigDecimal("900.0E-1").underlying()))
    assertEqual("9.e+1BD", Literal(BigDecimal("9.e+1").underlying()))
    checkError(
      exception = parseException(".e3"),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'.'", "hint" -> ": extra input '.'"))

    // Tiny Int Literal
    assertEqual("10Y", Literal(10.toByte))
    checkError(
      exception = parseException("1000Y"),
      errorClass = "_LEGACY_ERROR_TEMP_0023",
      parameters = Map(
        "rawStrippedQualifier" -> "1000",
        "minValue" -> Byte.MinValue.toString,
        "maxValue" -> Byte.MaxValue.toString,
        "typeName" -> "tinyint"),
      context = ExpectedContext(
        fragment = "1000Y",
        start = 0,
        stop = 4))

    // Small Int Literal
    assertEqual("10S", Literal(10.toShort))
    checkError(
      exception = parseException("40000S"),
      errorClass = "_LEGACY_ERROR_TEMP_0023",
      parameters = Map(
        "rawStrippedQualifier" -> "40000",
        "minValue" -> Short.MinValue.toString,
        "maxValue" -> Short.MaxValue.toString,
        "typeName" -> "smallint"),
      context = ExpectedContext(
        fragment = "40000S",
        start = 0,
        stop = 5))

    // Long Int Literal
    assertEqual("10L", Literal(10L))
    checkError(
      exception = parseException("78732472347982492793712334L"),
      errorClass = "_LEGACY_ERROR_TEMP_0023",
      parameters = Map(
        "rawStrippedQualifier" -> "78732472347982492793712334",
        "minValue" -> Long.MinValue.toString,
        "maxValue" -> Long.MaxValue.toString,
        "typeName" -> "bigint"),
      context = ExpectedContext(
        fragment = "78732472347982492793712334L",
        start = 0,
        stop = 26))

    // Double Literal
    assertEqual("10.0D", Literal(10.0D))
    checkError(
      exception = parseException("-1.8E308D"),
      errorClass = "_LEGACY_ERROR_TEMP_0023",
      parameters = Map(
        "rawStrippedQualifier" -> "-1.8E308",
        "minValue" -> BigDecimal(Double.MinValue).toString,
        "maxValue" -> BigDecimal(Double.MaxValue).toString,
        "typeName" -> "double"),
      context = ExpectedContext(
        fragment = "-1.8E308D",
        start = 0,
        stop = 8))
    checkError(
      exception = parseException("1.8E308D"),
      errorClass = "_LEGACY_ERROR_TEMP_0023",
      parameters = Map(
        "rawStrippedQualifier" -> "1.8E308",
        "minValue" -> BigDecimal(Double.MinValue).toString,
        "maxValue" -> BigDecimal(Double.MaxValue).toString,
        "typeName" -> "double"),
      context = ExpectedContext(
        fragment = "1.8E308D",
        start = 0,
        stop = 7))

    // BigDecimal Literal
    assertEqual("90912830918230182310293801923652346786BD",
      Literal(BigDecimal("90912830918230182310293801923652346786").underlying()))
    assertEqual("123.0E-28BD", Literal(BigDecimal("123.0E-28").underlying()))
    assertEqual("123.08BD", Literal(BigDecimal("123.08").underlying()))
    checkError(
      exception = parseException("1.20E-38BD"),
      errorClass = "_LEGACY_ERROR_TEMP_0061",
      parameters = Map("msg" -> "decimal can only support precision up to 38."),
      context = ExpectedContext(
        fragment = "1.20E-38BD",
        start = 0,
        stop = 9))
  }

  test("SPARK-30252: Decimal should set zero scale rather than negative scale by default") {
    assertEqual("123.0BD", Literal(Decimal(BigDecimal("123.0")), DecimalType(4, 1)))
    assertEqual("123BD", Literal(Decimal(BigDecimal("123")), DecimalType(3, 0)))
    assertEqual("123E10BD", Literal(Decimal(BigDecimal("123E10")), DecimalType(13, 0)))
    assertEqual("123E+10BD", Literal(Decimal(BigDecimal("123E+10")), DecimalType(13, 0)))
    assertEqual("123E-10BD", Literal(Decimal(BigDecimal("123E-10")), DecimalType(10, 10)))
    assertEqual("1.23E10BD", Literal(Decimal(BigDecimal("1.23E10")), DecimalType(11, 0)))
    assertEqual("-1.23E10BD", Literal(Decimal(BigDecimal("-1.23E10")), DecimalType(11, 0)))
  }

  test("SPARK-29956: scientific decimal should be parsed as Decimal in legacy mode") {
    def testDecimal(value: String, parser: ParserInterface): Unit = {
      assertEqual(value, Literal(BigDecimal(value).underlying), parser)
    }
    withSQLConf(SQLConf.LEGACY_EXPONENT_LITERAL_AS_DECIMAL_ENABLED.key -> "true") {
      val parser = new CatalystSqlParser()
      testDecimal("9e1", parser)
      testDecimal("9e-1", parser)
      testDecimal("-9e1", parser)
      testDecimal("9.0e1", parser)
      testDecimal(".9e+2", parser)
      testDecimal("0.9e+2", parser)
    }
  }

  test("strings") {
    Seq(true, false).foreach { escape =>
      withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> escape.toString) {
        val parser = new CatalystSqlParser()

        // tests that have same result whatever the conf is
        // Single Strings.
        assertEqual("\"hello\"", "hello", parser)
        assertEqual("'hello'", "hello", parser)

        // Multi-Strings.
        assertEqual("\"hello\" 'world'", "helloworld", parser)
        assertEqual("'hello' \" \" 'world'", "hello world", parser)

        // 'LIKE' string literals. Notice that an escaped '%' is the same as an escaped '\' and a
        // regular '%'; to get the correct result you need to add another escaped '\'.
        // TODO figure out if we shouldn't change the ParseUtils.unescapeSQLString method?
        assertEqual("'pattern%'", "pattern%", parser)
        assertEqual("'no-pattern\\%'", "no-pattern\\%", parser)

        // tests that have different result regarding the conf
        if (escape) {
          // When SQLConf.ESCAPED_STRING_LITERALS is enabled, string literal parsing falls back to
          // Spark 1.6 behavior.

          // 'LIKE' string literals.
          assertEqual("'pattern\\\\%'", "pattern\\\\%", parser)
          assertEqual("'pattern\\\\\\%'", "pattern\\\\\\%", parser)

          // Escaped characters.
          // Unescape string literal "'\\0'" for ASCII NUL (X'00') doesn't work
          // when ESCAPED_STRING_LITERALS is enabled.
          // It is parsed literally.
          assertEqual("'\\0'", "\\0", parser)

          // Note: Single quote follows 1.6 parsing behavior
          // when ESCAPED_STRING_LITERALS is enabled.
          checkError(
            exception = parseException("'\''"),
            errorClass = "PARSE_SYNTAX_ERROR",
            parameters = Map("error" -> "'''", "hint" -> ": extra input '''"))

          // The unescape special characters (e.g., "\\t") for 2.0+ don't work
          // when ESCAPED_STRING_LITERALS is enabled. They are parsed literally.
          assertEqual("'\\\"'", "\\\"", parser) // Double quote
          assertEqual("'\\b'", "\\b", parser) // Backspace
          assertEqual("'\\n'", "\\n", parser) // Newline
          assertEqual("'\\r'", "\\r", parser) // Carriage return
          assertEqual("'\\t'", "\\t", parser) // Tab character

          // The unescape Octals for 2.0+ don't work when ESCAPED_STRING_LITERALS is enabled.
          // They are parsed literally.
          assertEqual("'\\110\\145\\154\\154\\157\\041'", "\\110\\145\\154\\154\\157\\041", parser)
          // The unescape Unicode for 2.0+ doesn't work when ESCAPED_STRING_LITERALS is enabled.
          // They are parsed literally.
          assertEqual("'\\u0057\\u006F\\u0072\\u006C\\u0064\\u0020\\u003A\\u0029'",
            "\\u0057\\u006F\\u0072\\u006C\\u0064\\u0020\\u003A\\u0029", parser)
        } else {
          // Default behavior

          // 'LIKE' string literals.
          assertEqual("'pattern\\\\%'", "pattern\\%", parser)
          assertEqual("'pattern\\\\\\%'", "pattern\\\\%", parser)

          // Escaped characters.
          // See: http://dev.mysql.com/doc/refman/5.7/en/string-literals.html
          assertEqual("'\\0'", "\u0000", parser) // ASCII NUL (X'00')
          assertEqual("'\\''", "\'", parser) // Single quote
          assertEqual("'\\\"'", "\"", parser) // Double quote
          assertEqual("'\\b'", "\b", parser) // Backspace
          assertEqual("'\\n'", "\n", parser) // Newline
          assertEqual("'\\r'", "\r", parser) // Carriage return
          assertEqual("'\\t'", "\t", parser) // Tab character
          assertEqual("'\\Z'", "\u001A", parser) // ASCII 26 - CTRL + Z (EOF on windows)

          // Octals
          assertEqual("'\\110\\145\\154\\154\\157\\041'", "Hello!", parser)

          // Unicode
          assertEqual("'\\u0057\\u006F\\u0072\\u006C\\u0064\\u0020\\u003A\\u0029'", "World :)",
            parser)
        }

      }
    }
  }

  val ymIntervalUnits = Seq("year", "month")
  val dtIntervalUnits = Seq("week", "day", "hour", "minute", "second", "millisecond", "microsecond")

  def ymIntervalLiteral(u: String, s: String): Literal = {
    val period = u match {
      case "year" => Period.ofYears(Integer.parseInt(s))
      case "month" => Period.ofMonths(Integer.parseInt(s))
    }
    Literal.create(period, YearMonthIntervalType(YM.stringToField(u)))
  }

  def dtIntervalLiteral(u: String, s: String): Literal = {
    val value = if (u == "second") {
      (BigDecimal(s) * NANOS_PER_SECOND).toLong
    } else {
      java.lang.Long.parseLong(s)
    }
    val (duration, field) = u match {
      case "week" => (Duration.ofDays(value * 7), DT.DAY)
      case "day" => (Duration.ofDays(value), DT.DAY)
      case "hour" => (Duration.ofHours(value), DT.HOUR)
      case "minute" => (Duration.ofMinutes(value), DT.MINUTE)
      case "second" => (Duration.ofNanos(value), DT.SECOND)
      case "millisecond" => (Duration.ofMillis(value), DT.SECOND)
      case "microsecond" => (Duration.ofNanos(value * NANOS_PER_MICROS), DT.SECOND)
    }
    Literal.create(duration, DayTimeIntervalType(field))
  }

  def legacyIntervalLiteral(u: String, s: String): Literal = {
    Literal(IntervalUtils.stringToInterval(s + " " + u.toString))
  }

  test("intervals") {
    def checkIntervals(intervalValue: String, expected: Literal): Unit = {
      Seq(
        "" -> expected,
        "-" -> UnaryMinus(expected)
      ).foreach { case (sign, expectedLiteral) =>
        assertEqual(s"${sign}interval $intervalValue", expectedLiteral)
      }
    }

    // Empty interval statement
    checkError(
      exception = parseException("interval"),
      errorClass = "_LEGACY_ERROR_TEMP_0025",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "interval",
        start = 0,
        stop = 7))

    // Single Intervals.
    val forms = Seq("", "s")
    val values = Seq("0", "10", "-7", "21")

    ymIntervalUnits.foreach { unit =>
      forms.foreach { form =>
        values.foreach { value =>
          val expected = ymIntervalLiteral(unit, value)
          checkIntervals(s"$value $unit$form", expected)
          checkIntervals(s"'$value' $unit$form", expected)
        }
      }
    }

    dtIntervalUnits.foreach { unit =>
      forms.foreach { form =>
        values.foreach { value =>
          val expected = dtIntervalLiteral(unit, value)
          checkIntervals(s"$value $unit$form", expected)
          checkIntervals(s"'$value' $unit$form", expected)
        }
      }
    }

    // Hive nanosecond notation.
    checkIntervals("13.123456789 seconds", dtIntervalLiteral("second", "13.123456789"))

    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      (ymIntervalUnits ++ dtIntervalUnits).foreach { unit =>
        forms.foreach { form =>
          values.foreach { value =>
            val expected = legacyIntervalLiteral(unit, value)
            checkIntervals(s"$value $unit$form", expected)
            checkIntervals(s"'$value' $unit$form", expected)
          }
        }
      }
      checkIntervals(
        "-13.123456789 second",
        Literal(new CalendarInterval(
          0,
          0,
          DateTimeTestUtils.secFrac(-13, -123, -456))))
      checkIntervals(
        "13.123456 second",
        Literal(new CalendarInterval(
          0,
          0,
          DateTimeTestUtils.secFrac(13, 123, 456))))
      checkIntervals("1.001 second",
        Literal(IntervalUtils.stringToInterval("1 second 1 millisecond")))
    }

    // Non Existing unit
    checkError(
      exception = parseException("interval 10 nanoseconds"),
      errorClass = "_LEGACY_ERROR_TEMP_0062",
      parameters = Map(
        "msg" -> "Error parsing ' 10 nanoseconds' to interval, invalid unit 'nanoseconds'"),
      context = ExpectedContext(
        fragment = "10 nanoseconds",
        start = 9,
        stop = 22))

    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      // Year-Month intervals.
      val yearMonthValues = Seq("123-10", "496-0", "-2-3", "-123-0", "\t -1-2\t")
      yearMonthValues.foreach { value =>
        val result = Literal(IntervalUtils.fromYearMonthString(value))
        checkIntervals(s"'$value' year to month", result)
      }

      // Day-Time intervals.
      val datTimeValues = Seq(
        "99 11:22:33.123456789",
        "-99 11:22:33.123456789",
        "10 9:8:7.123456789",
        "1 0:0:0",
        "-1 0:0:0",
        "1 0:0:1",
        "\t 1 0:0:1 ")
      datTimeValues.foreach { value =>
        val result = Literal(IntervalUtils.fromDayTimeString(value))
        checkIntervals(s"'$value' day to second", result)
      }

      // Hour-Time intervals.
      val hourTimeValues = Seq(
        "11:22:33.123456789",
        "9:8:7.123456789",
        "-19:18:17.123456789",
        "0:0:0",
        "0:0:1")
      hourTimeValues.foreach { value =>
        val result = Literal(IntervalUtils.fromDayTimeString(
          value, DayTimeIntervalType.HOUR, DayTimeIntervalType.SECOND))
        checkIntervals(s"'$value' hour to second", result)
      }
    }

    // Unknown FROM TO intervals
    checkError(
      exception = parseException("interval '10' month to second"),
      errorClass = "_LEGACY_ERROR_TEMP_0028",
      parameters = Map("from" -> "month", "to" -> "second"),
      context = ExpectedContext(
        fragment = "'10' month to second",
        start = 9,
        stop = 28))

    // Composed intervals.
    checkIntervals(
      "10 years 3 months", Literal.create(Period.of(10, 3, 0), YearMonthIntervalType()))
    checkIntervals(
      "8 days 2 hours 3 minutes 21 seconds",
      Literal.create(Duration.ofDays(8).plusHours(2).plusMinutes(3).plusSeconds(21)))

    Seq(true, false).foreach { legacyEnabled =>
      withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> legacyEnabled.toString) {
        val intervalStr = "3 monThs 4 dayS 22 sEcond 1 millisecond"
        if (legacyEnabled) {
          checkIntervals(intervalStr, Literal(new CalendarInterval(3, 4, 22001000L)))
        } else {
          checkError(
            exception = parseException(s"interval $intervalStr"),
            errorClass = "_LEGACY_ERROR_TEMP_0029",
            parameters = Map("literal" -> "interval 3 monThs 4 dayS 22 sEcond 1 millisecond"),
            context = ExpectedContext(
              fragment = s"interval $intervalStr",
              start = 0,
              stop = 47))
        }
      }
    }
  }

  test("composed expressions") {
    assertEqual("1 + r.r As q", (Literal(1) + UnresolvedAttribute("r.r")).as("q"))
    assertEqual("1 - f('o', o(bar))", Literal(1) - $"f".function("o", $"o".function($"bar")))
    checkError(
      exception = parseException("1 - f('o', o(bar)) hello * world"),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'*'", "hint" -> ""))
  }

  test("SPARK-17364, fully qualified column name which starts with number") {
    assertEqual("123_", UnresolvedAttribute("123_"))
    assertEqual("1a.123_", UnresolvedAttribute("1a.123_"))
    // ".123" should not be treated as token of type DECIMAL_VALUE
    assertEqual("a.123A", UnresolvedAttribute("a.123A"))
    // ".123E3" should not be treated as token of type SCIENTIFIC_DECIMAL_VALUE
    assertEqual("a.123E3_column", UnresolvedAttribute("a.123E3_column"))
    // ".123D" should not be treated as token of type DOUBLE_LITERAL
    assertEqual("a.123D_column", UnresolvedAttribute("a.123D_column"))
    // ".123BD" should not be treated as token of type BIGDECIMAL_LITERAL
    assertEqual("a.123BD_column", UnresolvedAttribute("a.123BD_column"))
  }

  test("SPARK-17832 function identifier contains backtick") {
    val complexName = FunctionIdentifier("`ba`r", Some("`fo`o"))
    assertEqual(complexName.quotedString, UnresolvedAttribute(Seq("`fo`o", "`ba`r")))
    checkError(
      exception = parseException(complexName.unquotedString),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'.'", "hint" -> ""))

    // Function identifier contains continuous backticks should be treated correctly.
    val complexName2 = FunctionIdentifier("ba``r", Some("fo``o"))
    assertEqual(complexName2.quotedString, UnresolvedAttribute(Seq("fo``o", "ba``r")))
  }

  test("SPARK-19526 Support ignore nulls keywords for first and last") {
    assertEqual("first(a ignore nulls)", First($"a", true).toAggregateExpression())
    assertEqual("first(a)", First($"a", false).toAggregateExpression())
    assertEqual("last(a ignore nulls)", Last($"a", true).toAggregateExpression())
    assertEqual("last(a)", Last($"a", false).toAggregateExpression())
  }

  test("timestamp literals") {
    DateTimeTestUtils.outstandingZoneIds.foreach { zid =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId) {
        def toMicros(time: LocalDateTime): Long = {
          val seconds = time.atZone(zid).toInstant.getEpochSecond
          TimeUnit.SECONDS.toMicros(seconds)
        }
        assertEval(
          sqlCommand = "TIMESTAMP '2019-01-14 20:54:00.000'",
          expect = toMicros(LocalDateTime.of(2019, 1, 14, 20, 54)))
        assertEval(
          sqlCommand = "Timestamp '2000-01-01T00:55:00'",
          expect = toMicros(LocalDateTime.of(2000, 1, 1, 0, 55)))
        // Parsing of the string does not depend on the SQL config because the string contains
        // time zone offset already.
        assertEval(
          sqlCommand = "TIMESTAMP '2019-01-16 20:50:00.567000+01:00'",
          expect = 1547668200567000L)
      }
    }
  }

  test("date literals") {
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
        assertEval("DATE '2019-01-14'", 17910)
        assertEval("DATE '2019-01'", 17897)
        assertEval("DATE '2019'", 17897)
      }
    }
  }

  test("current date/timestamp braceless expressions") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
      assertEqual("current_date", CurrentDate())
      assertEqual("current_timestamp", CurrentTimestamp())
    }

    def testNonAnsiBehavior(): Unit = {
      assertEqual("current_date", UnresolvedAttribute.quoted("current_date"))
      assertEqual("current_timestamp", UnresolvedAttribute.quoted("current_timestamp"))
    }
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
      testNonAnsiBehavior()
    }
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "false") {
      testNonAnsiBehavior()
    }
  }

  test("SPARK-36736: (NOT) ILIKE (ANY | SOME | ALL) expressions") {
    Seq("any", "some").foreach { quantifier =>
      assertEqual(s"a ilike $quantifier ('FOO%', 'b%')", lower($"a") likeAny("foo%", "b%"))
      assertEqual(s"a not ilike $quantifier ('foo%', 'B%')", lower($"a") notLikeAny("foo%", "b%"))
      assertEqual(s"not (a ilike $quantifier ('FOO%', 'B%'))", !(lower($"a") likeAny("foo%", "b%")))
    }
    assertEqual("a ilike all ('Foo%', 'b%')", lower($"a") likeAll("foo%", "b%"))
    assertEqual("a not ilike all ('foo%', 'B%')", lower($"a") notLikeAll("foo%", "b%"))
    assertEqual("not (a ilike all ('foO%', 'b%'))", !(lower($"a") likeAll("foo%", "b%")))

    Seq("any", "some", "all").foreach { quantifier =>
      checkError(
        exception = parseException(s"a ilike $quantifier()"),
        errorClass = "_LEGACY_ERROR_TEMP_0064",
        parameters = Map("msg" -> "Expected something between '(' and ')'."),
        context = ExpectedContext(
          fragment = s"ilike $quantifier()",
          start = 2,
          stop = 9 + quantifier.length))
    }
  }
}
