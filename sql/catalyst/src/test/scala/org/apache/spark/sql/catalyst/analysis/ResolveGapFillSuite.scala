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

import java.time.{Duration, LocalDate, LocalDateTime}

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExamplePointUDT
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, GapFillGenerator, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{GapFillColumn, GapFillMethod, GapFillOutputColumn, Generate, LocalRelation, LogicalPlan, Project, UnresolvedGapFill}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.{MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE}
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
    val Seq(newV, _, newTs, binStart, isGenerated) = analyzed.output
    Seq(v -> newV, ts -> newTs).foreach { case (in, out) =>
      assert(out.exprId != in.exprId)
      assert(out.dataType == in.dataType && out.nullable && out.qualifier == Seq("t"))
    }
    assert(binStart.dataType == TimestampNTZType && binStart.nullable)
    assert(binStart.qualifier.isEmpty && isGenerated.qualifier.isEmpty)
    assert(isGenerated.dataType == BooleanType && !isGenerated.nullable)
    assert(generate.generatorOutput == Seq(newTs, newV, binStart, isGenerated))
    assert(generator.partitionColumns == Seq(p))
    assert(generator.timeColumn == ts && generator.valueColumns == Seq(v))
    assert(generator.elementSchema.map(_.dataType) ==
      Seq(TimestampNTZType, IntegerType, TimestampNTZType, BooleanType))
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
    val start = DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse("2026-01-01T00:00:00"))
    assert(generator.binWidthMicros == 15 * MICROS_PER_MINUTE)
    assert(generator.alignTo == start + 5 * MICROS_PER_MINUTE)
    assert(generator.rangeStart.contains(start))
    assert(generator.rangeEnd.contains(start + MICROS_PER_DAY))
    assert(generator.maxGapMicros.contains(40 * MICROS_PER_MINUTE))
    assert(generator.valueColumns.map(_.sql) == Seq("v", "w", "s", "d", "m", "day", "ltz"))
    assert(generator.fillMethods == Seq(GapFillMethod.Linear, GapFillMethod.Constant(Literal("1")),
      GapFillMethod.Null, GapFillMethod.ForwardFill, GapFillMethod.Null, GapFillMethod.Null,
      GapFillMethod.Null))
  }

  test("the default ALIGN TO is the 1970-01-01 bin start of the time column type") {
    assert(generatorOf(gapFill()).alignTo == 0L)
    assert(generatorOf(gapFill(timeColumn = "day", binWidth = Literal(Duration.ofDays(1))))
      .alignTo == 0L)
    Seq("UTC", "America/Los_Angeles").foreach { zone =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
        assert(generatorOf(gapFill(timeColumn = "ltz")).alignTo ==
          DateTimeUtils.daysToMicros(0, DateTimeUtils.getZoneId(zone)))
      }
    }
    val days = generatorOf(gapFill(timeColumn = "day", binWidth = Literal(Duration.ofDays(1)))
      .copy(rangeStartExpr = Some(Literal(LocalDate.of(1970, 1, 3))),
        rangeEndExpr = Some(Literal(LocalDate.of(1970, 1, 5)))))
    assert(days.rangeStart.contains(2L) && days.rangeEnd.contains(4L))
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
  }

  test("a CONSTANT is cast to a string column with another collation") {
    val collated = StringType("UTF8_LCASE")
    val input = LocalRelation($"ts".timestampNTZ, AttributeReference("c", collated)())
    val plan = unresolved(input)
      .copy(fillColumns = Seq(GapFillColumn($"c", GapFillMethod.Constant(Literal("x")))))
    Seq("true", "false").foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        assert(generatorOf(plan).fillMethods ==
          Seq(GapFillMethod.Constant(Literal.create("x", collated))))
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
      new ExamplePointUDT).foreach { keyType =>
      val plan = unresolved(LocalRelation(AttributeReference("k", keyType)(), $"ts".timestampNTZ))
        .copy(partitionColumns = Seq($"k"))
      expectError(plan, "UNSUPPORTED_FEATURE.GAP_FILL_PARTITION_KEY_TYPE",
        Map("columnName" -> "`k`", "columnType" -> toSQLType(keyType)))
    }
    Seq(StringType, IntegerType, DecimalType(10, 2), StructType.fromDDL("a INT, b STRING"))
      .foreach { keyType =>
        analyze(unresolved(LocalRelation(AttributeReference("k", keyType)(), $"ts".timestampNTZ))
          .copy(partitionColumns = Seq($"k")))
      }
  }

  test("UNSUPPORTED_FEATURE.GAP_FILL_TIMESTAMP_NANOS for every nanosecond timestamp type") {
    Seq(TimestampNTZNanosType(9), TimestampNTZNanosType(7), TimestampLTZNanosType(9))
      .foreach { timeType =>
        val plan = unresolved(LocalRelation(AttributeReference("ts", timeType)()))
        expectError(plan, "UNSUPPORTED_FEATURE.GAP_FILL_TIMESTAMP_NANOS",
          Map("columnName" -> "`ts`", "columnType" -> toSQLType(timeType)))
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
