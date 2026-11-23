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

package org.apache.spark.sql.catalyst.analysis

import java.time.{Duration, Instant, LocalDate, LocalDateTime}

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExamplePointUDT
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, CurrentDate, CurrentTimestamp, DayOfMonth, Exists, Expression, GapFillGenerator, If, IsNull, Literal, LocalTimestamp, OuterReference, OuterScopeReference}
import org.apache.spark.sql.catalyst.expressions.st.ST_AsBinary
import org.apache.spark.sql.catalyst.optimizer.ComputeCurrentTime
import org.apache.spark.sql.catalyst.plans.logical.{GapFillColumn, GapFillMethod, GapFillOutputColumn, Generate, LocalRelation, LogicalPlan, Project, UnresolvedGapFill}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_HOUR
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class ResolveGapFillSuite extends AnalysisTest with QueryErrorsBase {

  private def unresolved(child: LogicalPlan): UnresolvedGapFill =
    UnresolvedGapFill(
      partitionColumns = Nil,
      timeColumn = $"ts",
      binWidthExpr = Literal(5L * 60L * 1000000L, DayTimeIntervalType()),
      originExpr = None,
      rangeStartExpr = None,
      rangeEndExpr = None,
      fillColumns = Nil,
      maxGapExpr = None,
      binStart = None,
      isGenerated = None,
      child = child)

  private val resolvedInput = LocalRelation($"ts".timestampNTZ)
  private val missingTable = UnresolvedRelation(Seq("missing"))

  private def expectNotSupported(plan: LogicalPlan): Unit =
    checkError(
      exception = intercept[AnalysisException](ResolveGapFill.apply(plan)),
      condition = "UNSUPPORTED_FEATURE.GAP_FILL",
      sqlState = "0A000",
      parameters = Map.empty[String, String])

  private val input = LocalRelation(
    $"p".string, $"ts".timestampNTZ, $"v".int, $"w".string, $"s".struct($"a".int), $"d".double,
    AttributeReference("m", MapType(StringType, IntegerType))(), $"day".date, $"ltz".timestamp)

  private def minutes(n: Long): Literal = Literal(Duration.ofMinutes(n))
  private def ntz(s: String): Literal = Literal(LocalDateTime.parse(s))

  /** A GAP FILL over `input`, partitioned by `p`, ordered by `ts`, with 15 minute bins. */
  private def gapFill(
      partitionColumns: Seq[String] = Seq("p"),
      timeColumn: String = "ts",
      fill: Seq[(String, GapFillMethod)] = Nil,
      binWidth: Expression = minutes(15)): UnresolvedGapFill =
    unresolved(input).copy(
      partitionColumns = partitionColumns.map(UnresolvedAttribute.quotedString),
      timeColumn = UnresolvedAttribute.quotedString(timeColumn),
      fillColumns = fill.map { case (c, method) =>
        GapFillColumn(UnresolvedAttribute.quotedString(c), method)
      },
      binWidthExpr = binWidth)

  private def analyze(plan: LogicalPlan): LogicalPlan =
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      getAnalyzer.executeAndCheck(plan, new QueryPlanningTracker)
    }

  private def generatorOf(plan: LogicalPlan): GapFillGenerator =
    analyze(plan).collectFirst { case Generate(g: GapFillGenerator, _, _, _, _, _) => g }.get

  /** Evaluates the values of `generator` as the optimizer does, with the current time `instant`. */
  private def evalAt(generator: GapFillGenerator, instant: String): GapFillGenerator =
    GapFillResolution.evalCurrentTimeValues(ComputeCurrentTime.applyForExpression(
      generator, Instant.parse(instant)).asInstanceOf[GapFillGenerator])

  private def expectError(
      plan: LogicalPlan,
      condition: String,
      parameters: Map[String, String]): Unit =
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      assertAnalysisErrorCondition(plan, condition, parameters)
    }

  test("GAP FILL waits for its child to resolve") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      // A missing table must still win, so the node is left alone until its child resolves.
      val waiting = unresolved(missingTable)
      assert(ResolveGapFill.apply(waiting) == waiting)
      assertAnalysisErrorCondition(
        waiting, "TABLE_OR_VIEW_NOT_FOUND", Map("relationName" -> "`missing`"))
      assertAnalysisSuccess(unresolved(resolvedInput))
    }
  }

  test("an unresolved clause column gets the unresolved column error") {
    expectError(gapFill(timeColumn = "nope"), "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`nope`", "proposal" -> "`p`, `d`, `m`, `s`, `ts`"))
    // A function name that the input does not have is not a column either.
    expectError(gapFill(partitionColumns = Seq("current_user")),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`current_user`", "proposal" -> "`ts`, `ltz`, `s`, `d`, `day`"))
    expectError(gapFill(fill = Seq("user" -> GapFillMethod.ForwardFill)),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`user`", "proposal" -> "`s`, `d`, `m`, `p`, `ts`"))
    // An input column with the name of a function is a column.
    val named = LocalRelation($"current_user".string, $"ts".timestampNTZ)
    val generator = generatorOf(unresolved(named).copy(
      partitionColumns = Seq(UnresolvedAttribute.quotedString("current_user"))))
    assert(generator.partitionColumns == named.output.take(1))
  }

  test("a type error in a clause value gets the type error") {
    expectError(gapFill(binWidth = Literal(1) + Literal(true)),
      "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES",
      Map("sqlExpr" -> "\"(1 + true)\"", "left" -> "\"INT\"", "right" -> "\"BOOLEAN\""))
  }

  test("error order: a type error in an earlier clause before an unresolved FILL column") {
    expectError(
      gapFill(fill = Seq("nope" -> GapFillMethod.Null), binWidth = Literal(1) + Literal(true)),
      "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES",
      Map("sqlExpr" -> "\"(1 + true)\"", "left" -> "\"INT\"", "right" -> "\"BOOLEAN\""))
  }

  test("GAP FILL is rejected at once when the operator is disabled") {
    // The gate does not wait for the child, so a missing table also gets this error.
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "false") {
      expectNotSupported(unresolved(missingTable))
      expectNotSupported(unresolved(resolvedInput))
      assertAnalysisErrorCondition(
        unresolved(missingTable), "UNSUPPORTED_FEATURE.GAP_FILL", Map.empty)
      // In the full analyzer the clause columns are resolved before this rule runs. The gate
      // still comes first, also before every GAP FILL error.
      assertAnalysisErrorCondition(gapFill(), "UNSUPPORTED_FEATURE.GAP_FILL", Map.empty)
      assertAnalysisErrorCondition(
        gapFill(timeColumn = "v"), "UNSUPPORTED_FEATURE.GAP_FILL", Map.empty)
    }
  }

  test("only the partition columns pass through, in input order with new nullable columns") {
    val t = LocalRelation($"v".int.notNull, $"p".string.notNull, $"ts".timestampNTZ.notNull)
      .subquery("t")
    val plan = unresolved(t).copy(
      partitionColumns = Seq($"p"),
      binStart = Some(GapFillOutputColumn(None)),
      isGenerated = Some(GapFillOutputColumn(Some("gen"))))
    val analyzed = analyze(plan)
    val Project(projectList, generate @ Generate(generator: GapFillGenerator, _, false, None, _, _))
      = (analyzed: @unchecked)
    val Seq(v, p, ts) = generate.child.output

    assert(projectList.map(_.name) == Seq("v", "p", "ts", "bin_start", "gen"))
    assert(generate.requiredChildOutput == Seq(p))
    assert(analyzed.output(1) == p)
    val Seq(newV, _, newTs, newBinStart, newIsGenerated) = analyzed.output
    Seq(v -> newV, ts -> newTs).foreach { case (in, out) =>
      assert(out.exprId != in.exprId)
      assert(out.dataType == in.dataType && out.nullable && out.qualifier == Seq("t"))
    }
    assert(newBinStart.dataType == TimestampNTZType && newBinStart.nullable)
    assert(newBinStart.qualifier.isEmpty && newIsGenerated.qualifier.isEmpty)
    assert(newIsGenerated.dataType == BooleanType && !newIsGenerated.nullable)
    assert(generate.generatorOutput == Seq(newTs, newV, newBinStart, newIsGenerated))
    assert(generator.partitionColumns == Seq(p))
    assert(generator.timeColumn == ts && generator.valueColumns == Seq(v))
    assert(generator.elementSchema.map(_.dataType) ==
      Seq(TimestampNTZType, IntegerType, TimestampNTZType, BooleanType))
    // The clause can spell a column in another letter case. The output keeps the input names.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val output = analyze(gapFill(Seq("P"), "TS", Seq("V" -> GapFillMethod.ForwardFill))).output
      assert(output.map(_.name) == input.output.map(_.name))
    }
    // An input that lists a column twice gives one value column, and both copies read it.
    val twice = analyze(unresolved(t.select($"p", $"ts", $"v", $"v")).copy(
      partitionColumns = Seq($"p")))
    val Project(_, twiceGenerate @ Generate(twiceGenerator: GapFillGenerator, _, _, _, _, _)) =
      (twice: @unchecked)
    val Seq(_, _, inV, inV2) = twiceGenerate.child.output
    assert(inV2 == inV && twiceGenerator.valueColumns == Seq(inV))
    val Seq(_, twiceTs, twiceV, twiceV2) = twice.output
    assert(twiceV2 == twiceV && twiceV.exprId != inV.exprId)
    assert(twiceGenerate.generatorOutput == Seq(twiceTs, twiceV))
  }

  test("BIN_START and IS_GENERATED are added only when asked for") {
    assert(analyze(gapFill()).output.map(_.name) == input.output.map(_.name))
    val named = gapFill().copy(
      binStart = Some(GapFillOutputColumn(Some("b"))),
      isGenerated = Some(GapFillOutputColumn(None)))
    assert(analyze(named).output.map(_.name).takeRight(2) == Seq("b", "is_generated"))
  }

  test("the constants are folded and each CONSTANT is cast to its column type and folded") {
    val generator = generatorOf(gapFill(fill = Seq(
      "v" -> GapFillMethod.Linear,
      "w" -> GapFillMethod.Constant(Literal(1)),
      "d" -> GapFillMethod.ForwardFill)).copy(
      originExpr = Some(ntz("2026-01-01T00:05:00")),
      rangeStartExpr = Some(ntz("2026-01-01T00:00:00")),
      rangeEndExpr = Some(ntz("2026-01-02T00:00:00")),
      maxGapExpr = Some(minutes(40))))
    assert(generator.binWidth == minutes(15))
    assert(generator.alignTo == ntz("2026-01-01T00:05:00"))
    assert(generator.rangeStart.contains(ntz("2026-01-01T00:00:00")))
    assert(generator.rangeEnd.contains(ntz("2026-01-02T00:00:00")))
    assert(generator.maxGap.contains(minutes(40)))
    assert(generator.valueColumns.map(_.sql) == Seq("v", "w", "s", "d", "m", "day", "ltz"))
    assert(generator.fillMethods == Seq(GapFillMethod.Linear, GapFillMethod.Constant(Literal("1")),
      GapFillMethod.Null, GapFillMethod.ForwardFill, GapFillMethod.Null, GapFillMethod.Null,
      GapFillMethod.Null))
  }

  test("the default ALIGN TO is the 1970-01-01 bin start of the time column type") {
    assert(generatorOf(gapFill()).alignTo == Literal(0L, TimestampNTZType))
    assert(generatorOf(gapFill(timeColumn = "day", binWidth = Literal(Duration.ofDays(1))))
      .alignTo == Literal(0, DateType))
    Seq("UTC", "America/Los_Angeles").foreach { zone =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
        assert(generatorOf(gapFill(timeColumn = "ltz")).alignTo ==
          Literal(DateTimeUtils.daysToMicros(0, DateTimeUtils.getZoneId(zone)), TimestampType))
      }
    }
    val days = generatorOf(gapFill(timeColumn = "day", binWidth = Literal(Duration.ofDays(1)))
      .copy(rangeStartExpr = Some(Literal(LocalDate.of(1970, 1, 3))),
        rangeEndExpr = Some(Literal(LocalDate.of(1970, 1, 5)))))
    assert(days.rangeStart.contains(Literal(2, DateType)))
    assert(days.rangeEnd.contains(Literal(4, DateType)))
  }

  test("the element schema follows the types of the generator children") {
    val generator = generatorOf(gapFill(fill = Seq("w" -> GapFillMethod.Constant(Literal("x")))))
    // A rule can change the type of a child, and the element schema must follow it.
    val rewritten = generator.withNewChildren(generator.children.map {
      case e: Expression if e.sql == "v" => Cast(e, LongType)
      case e => e
    }).asInstanceOf[GapFillGenerator]
    assert(generator.elementSchema(1).dataType == IntegerType)
    assert(rewritten.elementSchema(1).dataType == LongType)
    assert(rewritten.fillMethods == generator.fillMethods)
    assert(rewritten.children.length == generator.children.length)
    // Only the GAP FILL exec runs it.
    assert(intercept[SparkException](generator.eval()).getCondition == "INTERNAL_ERROR")
  }

  test("a rewrite of the CONSTANT children puts each value on its own column") {
    val generator = generatorOf(gapFill(fill = Seq(
      "v" -> GapFillMethod.Constant(Literal(1)), "w" -> GapFillMethod.Constant(Literal("x")))))
    val rewritten = generator.withNewChildren(generator.children.map {
      case Literal(1, IntegerType) => Literal(2)
      case Literal(_, StringType) => Literal("y")
      case e => e
    }).asInstanceOf[GapFillGenerator]
    assert(rewritten.fillMethods.take(2) ==
      Seq(GapFillMethod.Constant(Literal(2)), GapFillMethod.Constant(Literal("y"))))
  }

  test("GAP_FILL_REQUIRES_TOP_LEVEL_COLUMN for a nested column") {
    expectError(gapFill(partitionColumns = Seq("s.a")),
      "GAP_FILL_REQUIRES_TOP_LEVEL_COLUMN", Map("columnName" -> "\"s.a\""))
    expectError(gapFill(fill = Seq("s.a" -> GapFillMethod.ForwardFill)),
      "GAP_FILL_REQUIRES_TOP_LEVEL_COLUMN", Map("columnName" -> "\"s.a\""))
  }

  test("GAP_FILL_TIME_TYPE_MISMATCH") {
    expectError(gapFill(timeColumn = "v"), "GAP_FILL_TIME_TYPE_MISMATCH",
      Map("columnName" -> "`v`", "columnType" -> "\"INT\""))
  }

  test("GAP_FILL_INVALID_PARTITION_COLUMN, INVALID_FILL_COLUMN and DUPLICATE_FILL_COLUMN") {
    expectError(gapFill(partitionColumns = Seq("p", "ts")),
      "GAP_FILL_INVALID_PARTITION_COLUMN", Map("columnName" -> "`ts`"))
    Seq("ts", "p").foreach { column =>
      expectError(gapFill(fill = Seq(column -> GapFillMethod.Null)),
        "GAP_FILL_INVALID_FILL_COLUMN", Map("columnName" -> s"`$column`"))
    }
    expectError(gapFill(fill = Seq("v" -> GapFillMethod.Linear, "v" -> GapFillMethod.Null)),
      "GAP_FILL_DUPLICATE_FILL_COLUMN", Map("columnName" -> "`v`"))
  }

  test("DATATYPE_MISMATCH.NON_FOLDABLE_INPUT") {
    expectError(gapFill(binWidth = $"v"), "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT",
      Map("sqlExpr" -> "\"v\"", "inputName" -> "`BIN WIDTH`", "inputType" -> "\"INT\"",
        "inputExpr" -> "\"v\""))
    expectError(gapFill(fill = Seq("w" -> GapFillMethod.Constant($"p"))),
      "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT",
      Map("sqlExpr" -> "\"p\"", "inputName" -> "`CONSTANT`", "inputType" -> "\"STRING\"",
        "inputExpr" -> "\"p\""))
  }

  test("GAP_FILL_INVALID_INTERVAL_TYPE and GAP_FILL_ARGUMENT_TYPE_MISMATCH") {
    expectError(gapFill(binWidth = Literal(1)), "GAP_FILL_INVALID_INTERVAL_TYPE",
      Map("clause" -> "BIN WIDTH", "inputType" -> "\"INT\""))
    expectError(gapFill().copy(maxGapExpr = Some(Literal.create(1, YearMonthIntervalType()))),
      "GAP_FILL_INVALID_INTERVAL_TYPE",
      Map("clause" -> "MAX GAP", "inputType" -> "\"INTERVAL YEAR TO MONTH\""))
    expectError(gapFill().copy(originExpr = Some(Literal.create(0L, TimestampType))),
      "GAP_FILL_ARGUMENT_TYPE_MISMATCH",
      Map("clause" -> "ALIGN TO", "argType" -> "\"TIMESTAMP\"", "timeType" -> "\"TIMESTAMP_NTZ\""))
    expectError(
      gapFill().copy(rangeStartExpr = Some(ntz("2026-01-01T00:00:00")),
        rangeEndExpr = Some(Literal("2026-01-02"))),
      "GAP_FILL_ARGUMENT_TYPE_MISMATCH",
      Map("clause" -> "range_end", "argType" -> "\"STRING\"", "timeType" -> "\"TIMESTAMP_NTZ\""))
  }

  test("DATATYPE_MISMATCH.UNEXPECTED_NULL, also for a NULL without a type") {
    expectError(gapFill(binWidth = Literal(null)), "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      Map("exprName" -> "`BIN WIDTH`", "sqlExpr" -> "\"NULL\""))
    expectError(gapFill().copy(originExpr = Some(Literal.create(null, TimestampNTZType))),
      "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      Map("exprName" -> "`ALIGN TO`", "sqlExpr" -> "\"NULL\""))
    // CONSTANT NULL is allowed.
    analyze(gapFill(fill = Seq("v" -> GapFillMethod.Constant(Literal(null)))))
  }

  test("DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE") {
    val range = "(INTERVAL '0 00:00:00' DAY TO SECOND, INTERVAL '3652058 00:00:00' DAY TO SECOND]"
    expectError(gapFill(binWidth = minutes(0)), "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
      Map("exprName" -> "`BIN WIDTH`", "sqlExpr" -> "\"INTERVAL '0 00:00:00' DAY TO SECOND\"",
        "valueRange" -> range, "currentValue" -> "INTERVAL '0 00:00:00' DAY TO SECOND"))
    expectError(gapFill(binWidth = Literal(Duration.ofDays(3652059))),
      "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
      Map("exprName" -> "`BIN WIDTH`", "sqlExpr" -> "\"INTERVAL '3652059 00:00:00' DAY TO SECOND\"",
        "valueRange" -> range, "currentValue" -> "INTERVAL '3652059 00:00:00' DAY TO SECOND"))
    analyze(gapFill(binWidth = Literal(Duration.ofDays(3652058))))
    expectError(gapFill().copy(maxGapExpr = Some(minutes(-1))),
      "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
      Map("exprName" -> "`MAX GAP`", "sqlExpr" -> "\"INTERVAL '-0 00:01:00' DAY TO SECOND\"",
        "valueRange" -> ("(INTERVAL '0 00:00:00' DAY TO SECOND, " +
          "INTERVAL '106751991 04:00:54.775807' DAY TO SECOND]"),
        "currentValue" -> "INTERVAL '-0 00:01:00' DAY TO SECOND"))
  }

  test("a clause value with a current time function is evaluated and checked after analysis") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC", SQLConf.ANSI_ENABLED.key -> "true") {
      val now = LocalTimestamp()
      val at = "2026-01-01T00:00:00Z"
      val generator = generatorOf(gapFill(
        fill = Seq("ltz" -> GapFillMethod.Constant(CurrentTimestamp())),
        binWidth = now - ntz("2025-12-31T23:45:00")).copy(
        originExpr = Some(now),
        rangeStartExpr = Some(ntz("2025-12-31T00:00:00")),
        rangeEndExpr = Some(now),
        maxGapExpr = Some(now - ntz("2025-12-31T23:00:00"))))
      // Analysis evaluates only the value without a current time function.
      val GapFillMethod.Constant(constant) = generator.fillMethods.last: @unchecked
      assert(Seq(generator.binWidth, generator.alignTo, generator.rangeEnd.get,
        generator.maxGap.get, constant).forall(!_.isInstanceOf[Literal]))
      assert(generator.rangeStart.contains(ntz("2025-12-31T00:00:00")))
      val evaluated = evalAt(generator, at)
      assert(evaluated.binWidth == minutes(15))
      assert(evaluated.alignTo == ntz("2026-01-01T00:00:00"))
      assert(evaluated.rangeStart.contains(ntz("2025-12-31T00:00:00")))
      assert(evaluated.rangeEnd.contains(ntz("2026-01-01T00:00:00")))
      assert(evaluated.maxGap.contains(minutes(60)))
      assert(evaluated.fillMethods.last == GapFillMethod.Constant(Literal(Instant.parse(at))))

      // The type of such a value is still checked during analysis.
      expectError(gapFill().copy(originExpr = Some(CurrentDate())),
        "GAP_FILL_ARGUMENT_TYPE_MISMATCH",
        Map("clause" -> "ALIGN TO", "argType" -> "\"DATE\"", "timeType" -> "\"TIMESTAMP_NTZ\""))
      // Its value errors come after the analysis errors.
      expectError(gapFill(fill = Seq("w" -> GapFillMethod.Linear), binWidth = now - now),
        "GAP_FILL_LINEAR_TYPE_MISMATCH", Map("columnName" -> "`w`", "columnType" -> "\"STRING\""))

      // The errors show the value with the current time in it.
      def evalError(plan: LogicalPlan): SparkThrowable =
        intercept[Exception](evalAt(generatorOf(plan), at)).asInstanceOf[SparkThrowable]
      val nowSql = "TIMESTAMP_NTZ '2026-01-01 00:00:00'"
      val zero = "INTERVAL '0 00:00:00' DAY TO SECOND"
      // BIN WIDTH comes before the range.
      checkError(
        exception = evalError(gapFill(binWidth = now - now).copy(
          rangeStartExpr = Some(now), rangeEndExpr = Some(ntz("2025-12-31T00:00:00")))),
        condition = "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
        parameters = Map("exprName" -> "`BIN WIDTH`", "sqlExpr" -> s"\"($nowSql - $nowSql)\"",
          "valueRange" -> s"($zero, INTERVAL '3652058 00:00:00' DAY TO SECOND]",
          "currentValue" -> zero))
      checkError(
        exception = evalError(gapFill(timeColumn = "day",
          binWidth = now - ntz("2025-12-31T23:00:00"))),
        condition = "GAP_FILL_INVALID_DATE_BIN_WIDTH",
        parameters = Map("binWidth" -> "INTERVAL '0 01:00:00' DAY TO SECOND"))
      checkError(
        exception = evalError(gapFill().copy(maxGapExpr = Some(now - now))),
        condition = "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
        parameters = Map("exprName" -> "`MAX GAP`", "sqlExpr" -> s"\"($nowSql - $nowSql)\"",
          "valueRange" -> s"($zero, INTERVAL '106751991 04:00:54.775807' DAY TO SECOND]",
          "currentValue" -> zero))
      val nullAt = If(IsNull(now), now, Literal(null, TimestampNTZType))
      Seq(
        "ALIGN TO" -> gapFill().copy(originExpr = Some(nullAt)),
        "range_start" -> gapFill().copy(rangeStartExpr = Some(nullAt), rangeEndExpr = Some(now)),
        "range_end" -> gapFill().copy(rangeStartExpr = Some(now), rangeEndExpr = Some(nullAt))
      ).foreach { case (clause, plan) =>
        checkError(
          exception = evalError(plan),
          condition = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
          parameters = Map("exprName" -> s"`$clause`",
            "sqlExpr" -> s"\"(IF(($nowSql IS NULL), $nowSql, NULL))\""))
      }
      checkError(
        exception = evalError(gapFill().copy(
          rangeStartExpr = Some(now), rangeEndExpr = Some(ntz("2025-12-31T00:00:00")))),
        condition = "GAP_FILL_INVALID_RANGE",
        parameters =
          Map("rangeStart" -> nowSql, "rangeEnd" -> "TIMESTAMP_NTZ '2025-12-31 00:00:00'"))
      checkError(
        exception = evalError(gapFill(fill = Seq("d" ->
          GapFillMethod.Constant(Literal(1) / (DayOfMonth(CurrentDate()) * 0))))),
        condition = "DIVIDE_BY_ZERO",
        parameters = Map("config" -> "\"spark.sql.ansi.enabled\""),
        queryContext = Array(ExpectedContext("", -1, -1)))
    }
  }

  test("GAP_FILL_INVALID_DATE_BIN_WIDTH") {
    expectError(gapFill(timeColumn = "day", binWidth = Literal(Duration.ofHours(36))),
      "GAP_FILL_INVALID_DATE_BIN_WIDTH",
      Map("binWidth" -> "INTERVAL '1 12:00:00' DAY TO SECOND"))
    analyze(gapFill(timeColumn = "day", binWidth = Literal(Duration.ofHours(48))))
  }

  test("GAP_FILL_CONSTANT_TYPE_MISMATCH") {
    expectError(gapFill(fill = Seq("v" -> GapFillMethod.Constant(Literal(true)))),
      "GAP_FILL_CONSTANT_TYPE_MISMATCH",
      Map("valueType" -> "\"BOOLEAN\"", "columnType" -> "\"INT\"", "columnName" -> "`v`"))
    // The implicit cast follows the ANSI mode: only the ANSI rules cast a STRING to BOOLEAN.
    val stringOnBoolean = unresolved(LocalRelation($"ts".timestampNTZ, $"b".boolean)).copy(
      fillColumns = Seq(GapFillColumn($"b", GapFillMethod.Constant(Literal("true")))))
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      assert(generatorOf(stringOnBoolean).fillMethods ==
        Seq(GapFillMethod.Constant(Literal(true))))
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      expectError(stringOnBoolean, "GAP_FILL_CONSTANT_TYPE_MISMATCH",
        Map("valueType" -> "\"STRING\"", "columnType" -> "\"BOOLEAN\"", "columnName" -> "`b`"))
    }
    // A day-time interval of another type is not truncated to the column type, also in an ARRAY.
    val day = DayTimeIntervalType(DayTimeIntervalType.DAY)
    val hour = DayTimeIntervalType(DayTimeIntervalType.HOUR)
    val intervals = LocalRelation($"ts".timestampNTZ, AttributeReference("d", day)(),
      AttributeReference("a", ArrayType(day, containsNull = false))())
    Seq(
      ("d", Literal.create(Duration.ofHours(36), hour), "\"INTERVAL HOUR\"", "\"INTERVAL DAY\""),
      ("a", Literal.create(Seq(Duration.ofHours(36)), ArrayType(hour, containsNull = false)),
        "\"ARRAY<INTERVAL HOUR>\"", "\"ARRAY<INTERVAL DAY>\"")
    ).foreach { case (column, value, valueType, columnType) =>
      val plan = unresolved(intervals).copy(fillColumns =
        Seq(GapFillColumn(UnresolvedAttribute(column), GapFillMethod.Constant(value))))
      Seq("true", "false").foreach { ansi =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          expectError(plan, "GAP_FILL_CONSTANT_TYPE_MISMATCH", Map("valueType" -> valueType,
            "columnType" -> columnType, "columnName" -> s"`$column`"))
        }
      }
    }
  }

  test("a CONSTANT is cast to a string column with another collation, also in an ARRAY") {
    val collated = StringType("UTF8_LCASE")
    val collatedArray = ArrayType(collated, containsNull = false)
    val input = LocalRelation($"ts".timestampNTZ, AttributeReference("c", collated)(),
      AttributeReference("a", collatedArray)())
    val plan = unresolved(input).copy(fillColumns = Seq(
      GapFillColumn($"c", GapFillMethod.Constant(Literal("x"))),
      GapFillColumn($"a", GapFillMethod.Constant(
        Literal.create(Seq("x"), ArrayType(StringType, containsNull = false))))))
    Seq("true", "false").foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        assert(generatorOf(plan).fillMethods == Seq(
          GapFillMethod.Constant(Literal.create("x", collated)),
          GapFillMethod.Constant(Literal.create(Seq("x"), collatedArray))))
      }
    }
  }

  test("a CONSTANT that can hold a NULL inside lets its column hold one") {
    val ints = ArrayType(IntegerType, containsNull = false)
    val intMap = MapType(StringType, IntegerType, valueContainsNull = false)
    val struct = new StructType().add("x", IntegerType, nullable = false)
    val input = LocalRelation($"ts".timestampNTZ, AttributeReference("a", ints)(),
      AttributeReference("m", intMap)(), AttributeReference("s", struct)(),
      AttributeReference("b", ints)(), AttributeReference("t", struct)())
    val values = Seq(
      Literal.create(Seq(null), ints.asNullable),
      Literal.create(Map("k" -> null), intMap.asNullable),
      Literal.create(Row(null), struct.asNullable),
      // A value type that allows no NULL inside keeps the column type, also when a field name has
      // another letter case.
      Literal.create(Seq(1), ints),
      Literal.create(Row(1), new StructType().add("X", IntegerType, nullable = false)))
    val fill = Seq("a", "m", "s", "b", "t").zip(values).map { case (column, value) =>
      GapFillColumn(UnresolvedAttribute(column), GapFillMethod.Constant(value))
    }
    val plan = unresolved(input).copy(fillColumns = fill)
    // The same values with a current time function, which the optimizer evaluates.
    val later = plan.copy(fillColumns = fill.zip(values).map { case (column, value) =>
      column.copy(method = GapFillMethod.Constant(If(IsNull(CurrentDate()), value, value)))
    })
    val types = Seq(ints.asNullable, intMap.asNullable, struct.asNullable, ints, struct)
    Seq("true", "false").foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        val constants = values.zip(types).map { case (value, dataType) =>
          GapFillMethod.Constant(Literal(value.value, dataType))
        }
        Seq(plan, later).foreach { gapFill =>
          val analyzed = analyze(gapFill)
          val generator = generatorOf(analyzed)
          assert(generator.elementSchema.map(_.dataType) == TimestampNTZType +: types)
          assert(analyzed.output.map(_.dataType) == TimestampNTZType +: types)
          assert(evalAt(generator, "2026-01-01T00:00:00Z").fillMethods == constants)
        }
        assert(generatorOf(later).fillMethods.forall {
          case GapFillMethod.Constant(value) => !value.isInstanceOf[Literal]
          case _ => false
        })
      }
    }
  }

  test("a CONSTANT is cast without the ANSI checks when ANSI mode is off") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      // An overflowing BIGINT wraps around, and a string that is not a number becomes NULL.
      Seq(3000000000L -> -1294967296, "abc" -> null).foreach { case (value, expected) =>
        assert(generatorOf(gapFill(fill = Seq("v" -> GapFillMethod.Constant(Literal(value)))))
          .fillMethods.head == GapFillMethod.Constant(Literal(expected, IntegerType)))
      }
    }
  }

  test("a CONSTANT is cast to TIMESTAMP in the session time zone") {
    val plan = gapFill(fill = Seq("ltz" -> GapFillMethod.Constant(Literal("2026-01-01 00:00:00"))))
    val utc = DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse("2026-01-01T00:00:00"))
    Seq("UTC" -> 0L, "America/Los_Angeles" -> 8 * MICROS_PER_HOUR).foreach { case (zone, offset) =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
        assert(generatorOf(plan).fillMethods.last ==
          GapFillMethod.Constant(Literal(utc + offset, TimestampType)))
      }
    }
  }

  test("GAP_FILL_INVALID_RANGE") {
    expectError(
      gapFill().copy(rangeStartExpr = Some(ntz("2026-01-02T00:00:00")),
        rangeEndExpr = Some(ntz("2026-01-01T00:00:00"))),
      "GAP_FILL_INVALID_RANGE",
      Map("rangeStart" -> "TIMESTAMP_NTZ '2026-01-02 00:00:00'",
        "rangeEnd" -> "TIMESTAMP_NTZ '2026-01-01 00:00:00'"))
    analyze(gapFill().copy(rangeStartExpr = Some(ntz("2026-01-01T00:00:00")),
      rangeEndExpr = Some(ntz("2026-01-01T00:00:00"))))
  }

  test("GAP_FILL_LINEAR_TYPE_MISMATCH") {
    expectError(gapFill(fill = Seq("w" -> GapFillMethod.Linear)),
      "GAP_FILL_LINEAR_TYPE_MISMATCH", Map("columnName" -> "`w`", "columnType" -> "\"STRING\""))
  }

  test("a TIMESTAMP time column keeps the session time zone, also for day bins") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
      Seq(Duration.ofHours(23), Duration.ofDays(1), Duration.ofHours(36)).foreach { width =>
        val generator = generatorOf(gapFill(timeColumn = "ltz", binWidth = Literal(width)))
        assert(generator.timeZoneId.contains("Asia/Tokyo"))
      }
      assert(generatorOf(gapFill(binWidth = Literal(Duration.ofDays(1)))).timeZoneId.isEmpty)
    }
  }

  test("UNSUPPORTED_FEATURE.GAP_FILL_PARTITION_KEY_TYPE, also inside a STRUCT or ARRAY") {
    // The SQL type of ExamplePointUDT is ARRAY<DOUBLE>.
    Seq(
      DoubleType, FloatType, StringType("UTF8_LCASE"), MapType(IntegerType, IntegerType),
      VariantType, CalendarIntervalType, ArrayType(FloatType), GeometryType(4326),
      GeographyType(4326), StructType.fromDDL("a INT, b ARRAY<DOUBLE>"),
      new ExamplePointUDT, FileType.external, ArrayType(FileType.external)).foreach { keyType =>
      val plan = unresolved(LocalRelation(AttributeReference("k", keyType)(), $"ts".timestampNTZ))
        .copy(partitionColumns = Seq($"k"))
      expectError(plan, "UNSUPPORTED_FEATURE.GAP_FILL_PARTITION_KEY_TYPE",
        Map("columnName" -> "`k`", "columnType" -> toSQLType(keyType)))
    }
    // The SQL type of GroupableUDT is INT.
    Seq(StringType, IntegerType, DecimalType(10, 2), StructType.fromDDL("a INT, b STRING"),
      ArrayType(IntegerType), new GroupableUDT).foreach { keyType =>
      analyze(unresolved(LocalRelation(AttributeReference("k", keyType)(), $"ts".timestampNTZ))
        .copy(partitionColumns = Seq($"k")))
    }
    // The message says what to do instead, and doing it works. A GEOMETRY has no cast to STRING,
    // but st_asbinary converts it to BINARY.
    val doubleKey = LocalRelation($"k".double, $"ts".timestampNTZ)
    val e = intercept[AnalysisException](
      analyze(unresolved(doubleKey).copy(partitionColumns = Seq($"k"))))
    assert(e.getMessage == "[UNSUPPORTED_FEATURE.GAP_FILL_PARTITION_KEY_TYPE] The feature is not " +
      "supported: GAP FILL with PARTITION BY column `k` of type \"DOUBLE\". Convert the column " +
      "to a supported type, such as STRING or BINARY, in a select before GAP FILL. SQLSTATE: 0A000")
    val geometry = LocalRelation(AttributeReference("g", GeometryType(4326))(), $"ts".timestampNTZ)
    Seq(
      doubleKey.select(Cast($"k", StringType).as("k"), $"ts"),
      geometry.select(new ST_AsBinary($"g").as("k"), $"ts")).foreach { input =>
      analyze(unresolved(input).copy(partitionColumns = Seq($"k")))
    }
  }

  test("UNSUPPORTED_FEATURE.GAP_FILL_TIMESTAMP_NANOS for every nanosecond timestamp type") {
    Seq(TimestampNTZNanosType(9), TimestampNTZNanosType(7), TimestampLTZNanosType(9))
      .foreach { timeType =>
        val plan = unresolved(LocalRelation(AttributeReference("ts", timeType)()))
        expectError(plan, "UNSUPPORTED_FEATURE.GAP_FILL_TIMESTAMP_NANOS",
          Map("columnName" -> "`ts`", "columnType" -> toSQLType(timeType)))
      }
    // The message says what to do instead, and doing it works.
    val e = intercept[AnalysisException](
      analyze(unresolved(LocalRelation(AttributeReference("ts", TimestampNTZNanosType(9))()))))
    assert(e.getMessage == "[UNSUPPORTED_FEATURE.GAP_FILL_TIMESTAMP_NANOS] The feature is not " +
      "supported: GAP FILL with time column `ts` of type \"TIMESTAMP_NTZ(9)\". Cast the time " +
      "column to TIMESTAMP or TIMESTAMP_NTZ before GAP FILL. SQLSTATE: 0A000")
    Seq(TimestampNTZNanosType(9) -> TimestampNTZType, TimestampLTZNanosType(9) -> TimestampType)
      .foreach { case (nanosType, microsType) =>
        val nanos = LocalRelation(AttributeReference("t", nanosType)())
        analyze(unresolved(nanos.select(Cast($"t", microsType).as("ts"))))
      }
  }

  test("FFILL and BFILL take a type that ORDER BY cannot sort") {
    val plan = unresolved(LocalRelation(
      $"ts".timestampNTZ, AttributeReference("m", MapType(StringType, IntegerType))(),
      $"x".array(VariantType), AttributeReference("pt", new ExamplePointUDT)()))
    Seq(GapFillMethod.ForwardFill, GapFillMethod.BackwardFill).foreach { method =>
      analyze(plan.copy(fillColumns = Seq($"m", $"x", $"pt").map(GapFillColumn(_, method))))
    }
  }

  // The errors below come in a fixed order, one test per boundary.

  test("error order: an unresolved column before a column that is not top-level") {
    expectError(
      gapFill(partitionColumns = Seq("s.a"), fill = Seq("nope" -> GapFillMethod.Null)),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`nope`",
        "proposal" -> "`p`, `d`, `m`, `s`, `ts`"))
    expectError(
      gapFill(partitionColumns = Seq("s.a"), fill = Seq("current_date" -> GapFillMethod.Null)),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`current_date`",
        "proposal" -> "`day`, `d`, `ltz`, `ts`, `m`"))
  }

  test("error order: a nested FILL column before the time column type") {
    expectError(gapFill(timeColumn = "v", fill = Seq("s.a" -> GapFillMethod.Null)),
      "GAP_FILL_REQUIRES_TOP_LEVEL_COLUMN", Map("columnName" -> "\"s.a\""))
  }

  test("error order: the time column type before the PARTITION BY rule") {
    expectError(gapFill(partitionColumns = Seq("v"), timeColumn = "v"),
      "GAP_FILL_TIME_TYPE_MISMATCH", Map("columnName" -> "`v`", "columnType" -> "\"INT\""))
  }

  test("error order: PARTITION BY, then FILL, then duplicate FILL columns") {
    expectError(gapFill(partitionColumns = Seq("ts"), fill = Seq("ts" -> GapFillMethod.Null)),
      "GAP_FILL_INVALID_PARTITION_COLUMN", Map("columnName" -> "`ts`"))
    expectError(
      gapFill(fill = Seq("v" -> GapFillMethod.Null, "v" -> GapFillMethod.Null,
        "ts" -> GapFillMethod.Null)),
      "GAP_FILL_INVALID_FILL_COLUMN", Map("columnName" -> "`ts`"))
  }

  test("error order: the FILL column rules before the constants") {
    expectError(
      gapFill(fill = Seq("v" -> GapFillMethod.Null, "v" -> GapFillMethod.Null), binWidth = $"v"),
      "GAP_FILL_DUPLICATE_FILL_COLUMN", Map("columnName" -> "`v`"))
  }

  test("error order: each constant in turn, all checks of one before the next") {
    // BIN WIDTH before ALIGN TO.
    expectError(
      gapFill(binWidth = minutes(0)).copy(originExpr = Some($"ts")),
      "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
      Map("exprName" -> "`BIN WIDTH`", "sqlExpr" -> "\"INTERVAL '0 00:00:00' DAY TO SECOND\"",
        "valueRange" ->
          "(INTERVAL '0 00:00:00' DAY TO SECOND, INTERVAL '3652058 00:00:00' DAY TO SECOND]",
        "currentValue" -> "INTERVAL '0 00:00:00' DAY TO SECOND"))
    // The DATE check of BIN WIDTH before ALIGN TO.
    expectError(
      gapFill(timeColumn = "day", binWidth = Literal(Duration.ofHours(36)))
        .copy(originExpr = Some($"ts")),
      "GAP_FILL_INVALID_DATE_BIN_WIDTH",
      Map("binWidth" -> "INTERVAL '1 12:00:00' DAY TO SECOND"))
    // ALIGN TO before range_start.
    expectError(
      gapFill().copy(
        originExpr = Some(Literal("x")),
        rangeStartExpr = Some(Literal(1)),
        rangeEndExpr = Some(ntz("2026-01-02T00:00:00"))),
      "GAP_FILL_ARGUMENT_TYPE_MISMATCH",
      Map("clause" -> "ALIGN TO", "argType" -> "\"STRING\"", "timeType" -> "\"TIMESTAMP_NTZ\""))
    // range_start before range_end, range_end before MAX GAP, MAX GAP before each CONSTANT.
    expectError(
      gapFill(fill = Seq("v" -> GapFillMethod.Constant(Literal(true)))).copy(
        rangeStartExpr = Some(Literal("x")),
        rangeEndExpr = Some($"ts"),
        maxGapExpr = Some(Literal(1))),
      "GAP_FILL_ARGUMENT_TYPE_MISMATCH",
      Map("clause" -> "range_start", "argType" -> "\"STRING\"", "timeType" -> "\"TIMESTAMP_NTZ\""))
    expectError(
      gapFill().copy(
        rangeStartExpr = Some(ntz("2026-01-01T00:00:00")),
        rangeEndExpr = Some(Literal("x")),
        maxGapExpr = Some(Literal(1))),
      "GAP_FILL_ARGUMENT_TYPE_MISMATCH",
      Map("clause" -> "range_end", "argType" -> "\"STRING\"", "timeType" -> "\"TIMESTAMP_NTZ\""))
    expectError(
      gapFill(fill = Seq("v" -> GapFillMethod.Constant(Literal(true))))
        .copy(maxGapExpr = Some(Literal(1))),
      "GAP_FILL_INVALID_INTERVAL_TYPE", Map("clause" -> "MAX GAP", "inputType" -> "\"INT\""))
  }

  test("error order: the constants before the range") {
    expectError(
      gapFill(fill = Seq("v" -> GapFillMethod.Constant(Literal(true)))).copy(
        rangeStartExpr = Some(ntz("2026-01-02T00:00:00")),
        rangeEndExpr = Some(ntz("2026-01-01T00:00:00"))),
      "GAP_FILL_CONSTANT_TYPE_MISMATCH",
      Map("valueType" -> "\"BOOLEAN\"", "columnType" -> "\"INT\"", "columnName" -> "`v`"))
  }

  test("error order: the range before LINEAR") {
    expectError(
      gapFill(fill = Seq("w" -> GapFillMethod.Linear)).copy(
        rangeStartExpr = Some(ntz("2026-01-02T00:00:00")),
        rangeEndExpr = Some(ntz("2026-01-01T00:00:00"))),
      "GAP_FILL_INVALID_RANGE",
      Map("rangeStart" -> "TIMESTAMP_NTZ '2026-01-02 00:00:00'",
        "rangeEnd" -> "TIMESTAMP_NTZ '2026-01-01 00:00:00'"))
  }

  test("error order: LINEAR before the unsupported cases") {
    expectError(gapFill(partitionColumns = Seq("d"), fill = Seq("w" -> GapFillMethod.Linear)),
      "GAP_FILL_LINEAR_TYPE_MISMATCH", Map("columnName" -> "`w`", "columnType" -> "\"STRING\""))
  }

  test("error order: the unsupported cases, one after the other") {
    val nanos = LocalRelation($"d".double, AttributeReference("ts", TimestampNTZNanosType(9))())
    expectError(
      unresolved(nanos).copy(partitionColumns = Seq($"d")),
      "UNSUPPORTED_FEATURE.GAP_FILL_PARTITION_KEY_TYPE",
      Map("columnName" -> "`d`", "columnType" -> "\"DOUBLE\""))
  }

  test("GAP_FILL_CORRELATED_INPUT for a subquery of the input that refers past the input") {
    val generate = analyze(gapFill()).collectFirst { case g: Generate => g }.get
    def withSubquery(subquery: Exists): Generate =
      generate.copy(child = generate.child.where(subquery))
    val c = $"c".int
    // With nested correlation, a subquery lists a column that comes from neither its plan nor
    // the plan that holds it as an OuterScopeReference. No rule makes one yet, so the plan is
    // built by hand.
    checkError(
      exception = intercept[AnalysisException] {
        getAnalyzer.checkAnalysis(withSubquery(
          Exists(LocalRelation(c), outerAttrs = Seq(OuterScopeReference($"o".int)))))
      },
      condition = "GAP_FILL_CORRELATED_INPUT",
      parameters = Map.empty)
    // A subquery that refers to a column of the input is fine.
    val v = generate.child.output.find(_.name == "v").get
    getAnalyzer.checkAnalysis(withSubquery(
      Exists(LocalRelation(c).where(c === OuterReference(v)), outerAttrs = Seq(v))))
  }

  test("an input column that is not a partition column must not pass through") {
    val generate = analyze(gapFill()).collectFirst { case g: Generate => g }.get
    val passingEverything = generate.copy(unrequiredChildIndex = Nil)
    val e = intercept[SparkException] {
      getAnalyzer.checkAnalysis(Project(passingEverything.output, passingEverything))
    }
    assert(e.getCondition == "INTERNAL_ERROR")
    assert(e.getMessage.contains("GAP FILL passes the input column"))
  }
}
