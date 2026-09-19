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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{BinBy, BinByOutputAliases, Join, JoinHint, LocalRelation, LogicalPlan, SubqueryAlias, UnresolvedBinBy}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class ResolveBinBySuite extends AnalysisTest {

  // Enable the operator for all tests here (the gate test opts out via `super.test`).
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
                             (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  private val tsStart = $"ts_start".timestamp
  private val tsEnd = $"ts_end".timestamp
  private val tsStartNtz = $"ts_start".timestampNTZ
  private val tsEndNtz = $"ts_end".timestampNTZ
  private val value = $"value".double
  private val label = $"label".string
  private val label2 = $"label2".string

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

  test("user-supplied foldable ALIGN TO of matching type is folded to micros") {
    val origin = Some(Literal(123456L, TimestampType))
    val resolved = ResolveBinBy.apply(unresolved(originExpr = origin))
    assert(resolved.asInstanceOf[BinBy].originMicros == 123456L)
  }

  test("omitted ALIGN TO fills the default origin per type and session zone") {
    // LTZ, UTC: epoch.
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val utc = ResolveBinBy.apply(unresolved(originExpr = None)).asInstanceOf[BinBy]
      assert(utc.originMicros == 0L)
    }

    // LTZ, non-UTC: shifted by the zone offset. LA is UTC-8 on 1970-01-01, so local
    // midnight is UTC 08:00 = 28800000000 micros.
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val la = ResolveBinBy.apply(unresolved(originExpr = None)).asInstanceOf[BinBy]
      assert(la.originMicros == 8L * 3600L * 1000000L)
    }

    // NTZ: epoch, zone-independent.
    val ntz = ResolveBinBy.apply(
      unresolved(child = ntzChild, rangeStart = tsStartNtz, rangeEnd = tsEndNtz,
        originExpr = None)).asInstanceOf[BinBy]
    assert(ntz.originMicros == 0L)
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
    // A foldable but null origin is rejected as a null argument.
    expectError(
      unresolved(originExpr = Some(Literal(null, TimestampType))), "BIN_BY_NULL_ARGUMENT")
    // A foldable CAST that throws on eval (ANSI) surfaces cleanly, not as a raw exception.
    // TimestampType cast needs a resolved time zone, mirroring the post-ResolveTimeZone state.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      expectError(
        unresolved(originExpr =
          Some(Cast(Literal.create("not a ts", StringType), TimestampType, Some("UTC")))),
        "BIN_BY_INVALID_ALIGN_TO")
    }
  }

  test("rejects invalid BIN WIDTH") {
    expectError(
      unresolved(binWidth = Literal(1, YearMonthIntervalType())), "BIN_BY_INVALID_BIN_WIDTH_TYPE")
    expectError(
      unresolved(binWidth = Literal(0L, DayTimeIntervalType())), "BIN_BY_NON_POSITIVE_BIN_WIDTH")
    expectError(
      unresolved(binWidth = Literal(-1L, DayTimeIntervalType())), "BIN_BY_NON_POSITIVE_BIN_WIDTH")
    // Null width is rejected as a null argument, distinct from a non-positive width.
    expectError(
      unresolved(binWidth = Literal(null, DayTimeIntervalType())), "BIN_BY_NULL_ARGUMENT")
    // A foldable CAST that throws on eval (ANSI) surfaces cleanly, not as a raw exception.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      expectError(
        unresolved(binWidth = Cast(Literal.create("not an interval", StringType),
          DayTimeIntervalType())),
        "BIN_BY_INVALID_BIN_WIDTH")
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

  test("resolved BinBy emits the DISTRIBUTE column as a produced attribute replacing the input") {
    // `value` sits mid-schema (not last) and carries a qualifier + metadata, so this covers
    // in-place replacement, produced identity, and the qualifier/metadata drop in one go.
    val md = new MetadataBuilder().putString("comment", "a measure").build()
    val valueMd = AttributeReference("value", DoubleType, nullable = true, md)()
    val child = SubqueryAlias("m", LocalRelation(tsStart, tsEnd, valueMd, label))
    val bi = ResolveBinBy.apply(
      unresolved(child = child, distribute = Seq(UnresolvedAttribute(Seq("m", "value")))))
      .asInstanceOf[BinBy]

    // The input is read (held in distributeColumns) but not forwarded by identity.
    assert(bi.distributeColumns.head.qualifier == Seq("m"))
    assert(bi.distributeColumns.head.metadata == md)
    assert(!bi.output.exists(_.exprId == valueMd.exprId))

    // It is replaced at its own position by a fresh-id, same-name produced attribute.
    val outValue = bi.output(child.output.indexWhere(_.exprId == valueMd.exprId))
    assert(outValue.name == "value" && outValue.exprId != valueMd.exprId)
    assert(bi.scaledDistributeColumns.map(_.exprId) == Seq(outValue.exprId))
    assert(bi.producedAttributes.contains(outValue))

    // The produced (computed) value drops the input's qualifier and metadata.
    assert(outValue.qualifier.isEmpty && outValue.metadata == Metadata.empty)

    // Forwarded (non-distribute) columns keep their identity.
    assert(bi.output.exists(_.exprId == label.exprId))
    assert(bi.output.exists(_.exprId == tsStart.exprId))
  }

  test("resolved BinBy emits each of multiple DISTRIBUTE columns as a produced attribute " +
    "replacing the input") {
    // `v1`, `v2`, `v3` sit at non-adjacent schema positions with forwarded columns between
    // them, so this covers per-slot in-place replacement with distinct fresh ids.
    val v1 = AttributeReference("v1", DoubleType, nullable = true)()
    val v2 = AttributeReference("v2", DoubleType, nullable = true)()
    val v3 = AttributeReference("v3", DoubleType, nullable = true)()
    val child = LocalRelation(tsStart, tsEnd, v1, label, v2, label2, v3)
    val distribute = Seq(v1, v2, v3)
    val bi = ResolveBinBy.apply(
      unresolved(child = child, distribute = distribute)).asInstanceOf[BinBy]

    // The inputs are read (held in distributeColumns) but none is forwarded by identity.
    assert(bi.distributeColumns.map(_.exprId) == distribute.map(_.exprId))
    assert(distribute.forall(v => !bi.output.exists(_.exprId == v.exprId)))

    // Each is replaced at its own position by a fresh-id, same-name produced attribute.
    val outputs = distribute.map(v => bi.output(child.output.indexWhere(_.exprId == v.exprId)))
    outputs.zip(distribute).foreach { case (out, in) =>
      assert(out.name == in.name && out.exprId != in.exprId)
    }
    assert(outputs.map(_.exprId).distinct.length == distribute.length)
    assert(bi.scaledDistributeColumns.map(_.exprId) == outputs.map(_.exprId))
    assert(outputs.forall(bi.producedAttributes.contains))

    // The child portion of `output` keeps the child's column order and names.
    assert(bi.output.take(child.output.length).map(_.name) == child.output.map(_.name))
    assert(bi.output.exists(_.exprId == label.exprId))
    assert(bi.output.exists(_.exprId == label2.exprId))
    assert(bi.output.exists(_.exprId == tsStart.exprId))
  }

  test("multipart identifiers disambiguate same-name columns across a JOIN") {
    val t1Start = AttributeReference("ts_start", TimestampType, nullable = true)()
    val t1End = AttributeReference("ts_end", TimestampType, nullable = true)()
    val t2Start = AttributeReference("ts_start", TimestampType, nullable = true)()
    val t2End = AttributeReference("ts_end", TimestampType, nullable = true)()
    val t2Value = AttributeReference("value", DoubleType, nullable = true)()
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

  test("rejects unresolvable column references with UNRESOLVED_COLUMN") {
    expectError(
      unresolved(rangeStart = UnresolvedAttribute("nonexistent")),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION")
    expectError(
      unresolved(distribute = Seq(UnresolvedAttribute("nonexistent"))),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION")
  }

  test("UNRESOLVED_COLUMN suggestions are ordered by similarity to the missing name") {
    // `valeu` is one edit away from `value`, so `value` is suggested first and the remaining
    // columns follow by edit distance.
    val ex = intercept[SparkThrowable](
      ResolveBinBy.apply(unresolved(rangeStart = UnresolvedAttribute("valeu"))))
    assert(ex.getCondition == "UNRESOLVED_COLUMN.WITH_SUGGESTION")
    assert(ex.getMessageParameters.get("objectName") == "`valeu`")
    assert(ex.getMessageParameters.get("proposal") == "`value`, `label`, `ts_end`, `ts_start`")
  }

  test("rejects nested column refs through the full analyzer (RULE_ORDERING_DEPENDENCIES)") {
    val structCol = AttributeReference(
      "outer",
      StructType(Seq(
        StructField("ts_start", TimestampType, nullable = true),
        StructField("ts_end", TimestampType, nullable = true))),
      nullable = true)()
    val numeric = AttributeReference("value", DoubleType, nullable = true)()
    val plan = UnresolvedBinBy(
      binWidthExpr = fiveMinutes,
      rangeStartCol = UnresolvedAttribute(Seq("outer", "ts_start")),
      rangeEndCol = UnresolvedAttribute(Seq("outer", "ts_end")),
      originExpr = Some(ltzOrigin),
      distributeColumns = Seq(numeric),
      outputAliases = BinByOutputAliases.empty,
      child = LocalRelation(structCol, numeric))
    assertAnalysisErrorCondition(
      plan,
      "BIN_BY_REQUIRES_TOP_LEVEL_COLUMN",
      Map("columnName" -> "\"outer.ts_start\""))
  }

  test("rejects non-timestamp or mismatched RANGE columns") {
    // Range columns must be TIMESTAMP/TIMESTAMP_NTZ...
    expectError(unresolved(rangeStart = value), "BIN_BY_RANGE_TYPE_MISMATCH")
    // ...and both must share the same type.
    expectError(unresolved(rangeEnd = tsEndNtz), "BIN_BY_RANGE_TYPE_MISMATCH")
  }

  test("rejects invalid DISTRIBUTE UNIFORM columns") {
    // Empty (defensive: the parser also rejects this) and duplicate columns.
    expectError(unresolved(distribute = Seq.empty), "BIN_BY_MISSING_DISTRIBUTE")
    expectError(unresolved(distribute = Seq(value, value)), "BIN_BY_DUPLICATE_DISTRIBUTE_COLUMN")
  }

  test("DISTRIBUTE UNIFORM accepts only FLOAT and DOUBLE columns") {
    val floatCol = $"f".float
    val intCol = $"i".int
    val decimalCol = AttributeReference("dec", DecimalType(10, 2), nullable = true)()
    val intervalCol = AttributeReference("dur", DayTimeIntervalType(), nullable = true)()
    val child = LocalRelation(tsStart, tsEnd, floatCol, intCol, decimalCol, intervalCol, label)

    // FLOAT resolves; DOUBLE is exercised by the default `value` fixture throughout.
    ResolveBinBy.apply(unresolved(child = child, distribute = Seq(floatCol)))

    // Integral, DECIMAL, DT INTERVAL, and other non-float/double types are rejected;
    // users CAST to DOUBLE in an upstream projection.
    Seq(intCol, decimalCol, intervalCol, label).foreach { c =>
      expectError(
        unresolved(child = child, distribute = Seq(c)), "BIN_BY_INVALID_DISTRIBUTE_COLUMN_TYPE")
    }
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
    val producedExprIds = binBys.flatMap(b =>
      (b.scaledDistributeColumns ++ b.appendedAttributes).map(_.exprId))
    assert(producedExprIds.distinct.size == producedExprIds.size,
      "produced BinBy attributes must have distinct exprIds across the two join sides")
  }

  // `super.test` escapes the suite-wide flag-on wrapper; pin the flag off explicitly.
  super.test("BIN BY is rejected when the operator is disabled") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "false") {
      expectError(unresolved(), "UNSUPPORTED_FEATURE.BIN_BY")
    }
  }
}
