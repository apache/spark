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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{BinBy, BinByOutputAliases, Join, JoinHint, LocalRelation, LogicalPlan, SubqueryAlias, UnresolvedBinBy}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class ResolveBinBySuite extends AnalysisTest {

  private val tsStart = $"ts_start".timestamp
  private val tsEnd = $"ts_end".timestamp
  private val tsStartNtz = $"ts_start".timestampNTZ
  private val tsEndNtz = $"ts_end".timestampNTZ
  private val value = $"value".long
  private val label = $"label".string

  private val ltzChild: LogicalPlan = LocalRelation(tsStart, tsEnd, value, label)
  private val ntzChild: LogicalPlan = LocalRelation(tsStartNtz, tsEndNtz, value)

  private val fiveMinutes: Expression = Literal(5L * 60L * 1000000L, DayTimeIntervalType())
  private val ltzOrigin: Expression = Literal(0L, TimestampType)
  private val ntzOrigin: Expression = Literal(0L, TimestampNTZType)

  private def unresolved(
      child: LogicalPlan = ltzChild,
      binWidth: Expression = fiveMinutes,
      rangeStart: Expression = tsStart,
      rangeEnd: Expression = tsEnd,
      originExpr: Option[Expression] = Some(ltzOrigin),
      distribute: Seq[Expression] = Seq(value),
      aliases: BinByOutputAliases = BinByOutputAliases.empty): UnresolvedBinBy =
    UnresolvedBinBy(binWidth, rangeStart, rangeEnd, originExpr, distribute, aliases, child)

  private def expectError(u: UnresolvedBinBy, condition: String): Unit = {
    val ex = intercept[SparkThrowable](ResolveBinBy.apply(u))
    assert(ex.getCondition == condition,
      s"expected condition '$condition' but got '${ex.getCondition}'")
  }

  test("user-supplied foldable ALIGN TO of matching type is preserved") {
    val originExpr: Expression = Literal(123456L, TimestampType)
    val resolved = ResolveBinBy.apply(unresolved(originExpr = Some(originExpr)))
    assert(resolved.asInstanceOf[BinBy].originExpr == originExpr)
  }

  test("omitted ALIGN TO fills the default origin per type and session zone") {
    // LTZ, UTC: epoch.
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val utc = ResolveBinBy.apply(unresolved(originExpr = None)).asInstanceOf[BinBy]
      assert(utc.originExpr == Literal(0L, TimestampType))
    }

    // LTZ, non-UTC: shifted by the zone offset. LA is UTC-8 on 1970-01-01, so local
    // midnight is UTC 08:00 = 28800000000 micros.
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val la = ResolveBinBy.apply(unresolved(originExpr = None)).asInstanceOf[BinBy]
      assert(la.originExpr == Literal(8L * 3600L * 1000000L, TimestampType))
    }

    // NTZ: epoch, zone-independent.
    val ntz = ResolveBinBy.apply(
      unresolved(child = ntzChild, rangeStart = tsStartNtz, rangeEnd = tsEndNtz,
        originExpr = None)).asInstanceOf[BinBy]
    assert(ntz.originExpr == Literal(0L, TimestampNTZType))
  }

  test("rejects invalid ALIGN TO") {
    // Type must match the range columns, both directions.
    expectError(unresolved(originExpr = Some(ntzOrigin)), "BIN_BY_ALIGN_TO_TYPE_MISMATCH")
    expectError(
      unresolved(child = ntzChild, rangeStart = tsStartNtz, rangeEnd = tsEndNtz,
        originExpr = Some(ltzOrigin)),
      "BIN_BY_ALIGN_TO_TYPE_MISMATCH")
    // Must be foldable.
    val originAttr = AttributeReference("o", TimestampType, nullable = true)()
    val childWithOrigin = LocalRelation(tsStart, tsEnd, value, originAttr)
    expectError(
      unresolved(child = childWithOrigin, originExpr = Some(originAttr)),
      "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT")
  }

  test("rejects invalid BIN WIDTH") {
    def invalid(w: Expression): Unit =
      expectError(unresolved(binWidth = w), "BIN_BY_INVALID_BIN_WIDTH")
    invalid(Literal(1, YearMonthIntervalType()))   // year-month
    invalid(Literal(0L, DayTimeIntervalType()))    // zero
    invalid(Literal(-1L, DayTimeIntervalType()))   // negative
    invalid(Literal(null, DayTimeIntervalType()))  // null
    // A foldable CAST that throws on eval (ANSI) surfaces cleanly, not as a raw exception.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      invalid(Cast(Literal.create("not an interval", StringType), DayTimeIntervalType()))
    }
    // Non-foldable is rejected before evaluation.
    val widthAttr = AttributeReference("w", DayTimeIntervalType(), nullable = true)()
    val childWithWidth = LocalRelation(tsStart, tsEnd, value, widthAttr)
    expectError(
      unresolved(child = childWithWidth, binWidth = widthAttr),
      "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT")
  }

  test("captures the session zone for LTZ inputs and none for NTZ") {
    // LTZ: columns resolve and a zone is captured.
    val ltz = ResolveBinBy.apply(unresolved()).asInstanceOf[BinBy]
    assert(ltz.rangeStart == tsStart)
    assert(ltz.rangeEnd == tsEnd)
    assert(ltz.distributeColumns == Seq(value))
    assert(ltz.timeZoneId.isDefined)

    // The configured zone is the one captured.
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val la = ResolveBinBy.apply(unresolved()).asInstanceOf[BinBy]
      assert(la.timeZoneId.contains("America/Los_Angeles"))
    }

    // NTZ: no zone captured.
    val ntz = ResolveBinBy.apply(
      unresolved(child = ntzChild, rangeStart = tsStartNtz, rangeEnd = tsEndNtz,
        originExpr = Some(ntzOrigin))).asInstanceOf[BinBy]
    assert(ntz.timeZoneId.isEmpty)
  }

  test("resolved BinBy output schema appends three columns, honoring renames") {
    // Default names and types.
    val default = ResolveBinBy.apply(unresolved()).asInstanceOf[BinBy]
    assert(default.output.length == ltzChild.output.length + 3)
    val appended = default.output.takeRight(3)
    assert(appended.map(_.name) == Seq("bin_start", "bin_end", "bin_distribute_ratio"))
    assert(appended.map(_.dataType) == Seq(TimestampType, TimestampType, DoubleType))

    // All three renamed.
    val full = ResolveBinBy.apply(
      unresolved(aliases = BinByOutputAliases(Some("ws"), Some("we"), Some("frac"))))
      .asInstanceOf[BinBy]
    assert(full.output.takeRight(3).map(_.name) == Seq("ws", "we", "frac"))

    // Partial rename; the other two keep defaults.
    val partial = ResolveBinBy.apply(
      unresolved(aliases = BinByOutputAliases(binStart = Some("ws")))).asInstanceOf[BinBy]
    assert(partial.output.takeRight(3).map(_.name) == Seq("ws", "bin_end", "bin_distribute_ratio"))
  }

  test("resolves UnresolvedAttribute references against child output") {
    val tsStartU = UnresolvedAttribute("ts_start")
    val tsEndU = UnresolvedAttribute("ts_end")
    val valueU = UnresolvedAttribute("value")
    val result = ResolveBinBy.apply(
      unresolved(rangeStart = tsStartU, rangeEnd = tsEndU, distribute = Seq(valueU)))
    val bi = result.asInstanceOf[BinBy]
    assert(bi.rangeStart.exprId == tsStart.exprId)
    assert(bi.rangeEnd.exprId == tsEnd.exprId)
    assert(bi.distributeColumns.map(_.exprId) == Seq(value.exprId))
  }

  test("multipart identifiers disambiguate same-name columns across a JOIN") {
    val t1Start = AttributeReference("ts_start", TimestampType, nullable = true)()
    val t1End = AttributeReference("ts_end", TimestampType, nullable = true)()
    val t2Start = AttributeReference("ts_start", TimestampType, nullable = true)()
    val t2End = AttributeReference("ts_end", TimestampType, nullable = true)()
    val t2Value = AttributeReference("value", LongType, nullable = true)()
    val t1 = SubqueryAlias("t1", LocalRelation(t1Start, t1End))
    val t2 = SubqueryAlias("t2", LocalRelation(t2Start, t2End, t2Value))
    val join = Join(t1, t2, Inner, None, JoinHint.NONE)

    // t1. picks t1's columns.
    val u1 = UnresolvedBinBy(
      binWidthExpr = fiveMinutes,
      rangeStartCol = UnresolvedAttribute(Seq("t1", "ts_start")),
      rangeEndCol = UnresolvedAttribute(Seq("t1", "ts_end")),
      originExpr = Some(ltzOrigin),
      distributeColumns = Seq(UnresolvedAttribute(Seq("t2", "value"))),
      outputAliases = BinByOutputAliases.empty,
      child = join)
    val resolved1 = ResolveBinBy.apply(u1).asInstanceOf[BinBy]
    assert(resolved1.rangeStart.exprId == t1Start.exprId)
    assert(resolved1.rangeEnd.exprId == t1End.exprId)

    // t2. picks t2's columns.
    val u2 = UnresolvedBinBy(
      binWidthExpr = fiveMinutes,
      rangeStartCol = UnresolvedAttribute(Seq("t2", "ts_start")),
      rangeEndCol = UnresolvedAttribute(Seq("t2", "ts_end")),
      originExpr = Some(ltzOrigin),
      distributeColumns = Seq(UnresolvedAttribute(Seq("t2", "value"))),
      outputAliases = BinByOutputAliases.empty,
      child = join)
    val resolved2 = ResolveBinBy.apply(u2).asInstanceOf[BinBy]
    assert(resolved2.rangeStart.exprId == t2Start.exprId)
    assert(resolved2.rangeEnd.exprId == t2End.exprId)
  }

  test("rejects unresolvable column references with BIN_BY_COLUMN_NOT_FOUND") {
    expectError(
      unresolved(rangeStart = UnresolvedAttribute("nonexistent")), "BIN_BY_COLUMN_NOT_FOUND")
    expectError(
      unresolved(distribute = Seq(UnresolvedAttribute("nonexistent"))), "BIN_BY_COLUMN_NOT_FOUND")
  }

  test("rejects nested/computed column references with BIN_BY_REQUIRES_TOP_LEVEL_COLUMN") {
    // A struct-field access resolves to a non-Attribute (Alias(GetStructField)). The column
    // exists, so this is distinct from BIN_BY_COLUMN_NOT_FOUND.
    val structField = AttributeReference(
      "outer",
      StructType(Seq(
        StructField("ts_start", TimestampType, nullable = true),
        StructField("ts_end", TimestampType, nullable = true))),
      nullable = true)()
    val numeric = AttributeReference("value", LongType, nullable = true)()
    expectError(
      UnresolvedBinBy(
        binWidthExpr = fiveMinutes,
        rangeStartCol = UnresolvedAttribute(Seq("outer", "ts_start")),
        rangeEndCol = UnresolvedAttribute(Seq("outer", "ts_end")),
        originExpr = Some(ltzOrigin),
        distributeColumns = Seq(numeric),
        outputAliases = BinByOutputAliases.empty,
        child = LocalRelation(structField, numeric)),
      "BIN_BY_REQUIRES_TOP_LEVEL_COLUMN")
  }

  test("rejects non-timestamp or mismatched RANGE columns") {
    // Range columns must be TIMESTAMP/TIMESTAMP_NTZ...
    expectError(unresolved(rangeStart = value), "BIN_BY_RANGE_TYPE_MISMATCH")
    // ...and both must share the same type.
    expectError(unresolved(rangeEnd = tsEndNtz), "BIN_BY_RANGE_TYPE_MISMATCH")
  }

  test("rejects invalid DISTRIBUTE UNIFORM columns") {
    // Empty (defensive: the parser also rejects this), non-numeric, and duplicate columns.
    expectError(unresolved(distribute = Seq.empty), "BIN_BY_MISSING_DISTRIBUTE")
    expectError(unresolved(distribute = Seq(label)), "BIN_BY_DISTRIBUTE_TYPE_MISMATCH")
    expectError(unresolved(distribute = Seq(value, value)), "BIN_BY_DUPLICATE_DISTRIBUTE_COLUMN")
  }

  test("BinBy survives full Analyzer + CheckAnalysis (regression for producedAttributes)") {
    // Without BinBy.producedAttributes, CheckAnalysis would flag MISSING_ATTRIBUTES.
    assertAnalysisSuccess(unresolved())
    assertAnalysisSuccess(
      unresolved(aliases = BinByOutputAliases(Some("ws"), Some("we"), Some("frac"))))
  }

  test("self-join over a shared BinBy subtree is deduplicated") {
    // Reference the same resolved BinBy subtree on both sides of a join: the appended
    // bin_start/bin_end/bin_distribute_ratio attributes start with identical exprIds.
    val binBy = ResolveBinBy.apply(unresolved()).asInstanceOf[BinBy]
    val selfJoin = Join(binBy, binBy, Inner, None, JoinHint.NONE)

    // DeduplicateRelations must renew the right side's appended attributes; without the
    // BinBy cases in both dedup phases the conflicting exprIds make analysis fail.
    val analyzed = getAnalyzer.executeAndCheck(selfJoin, new QueryPlanningTracker)

    val binBys = analyzed.collect { case b: BinBy => b }
    assert(binBys.size == 2, s"expected two BinBy nodes, got ${binBys.size}")
    val appendedExprIds = binBys.flatMap(_.appendedAttributes.map(_.exprId))
    assert(appendedExprIds.distinct.size == appendedExprIds.size,
      "appended BinBy attributes must have distinct exprIds across the two join sides")
  }
}
