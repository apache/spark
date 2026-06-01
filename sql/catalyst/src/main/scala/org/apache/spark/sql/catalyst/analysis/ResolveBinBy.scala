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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, EmptyRow, ExprId, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{BinBy, LogicalPlan, UnresolvedBinBy}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_BIN_BY
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AnyTimestampType, DayTimeIntervalType, NumericType, TimestampNTZType, TimestampType}

/**
 * Resolves [[UnresolvedBinBy]] into [[BinBy]]: looks up column references against the child's
 * output, validates types and foldability, and captures the session local time zone for the
 * physical execution to use.
 */
object ResolveBinBy extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(UNRESOLVED_BIN_BY), ruleId) {
    case b: UnresolvedBinBy if !readyToResolve(b) => b
    case b: UnresolvedBinBy => resolve(b)
  }

  // `binWidthExpr` must be fully resolved before validation can proceed. The optional
  // `originExpr` must also be resolved when present; an absent `ALIGN TO` clause defaults to
  // `1970-01-01 00:00:00` and bypasses user-facing foldability and type checks. The
  // range/distribute column references can stay as `UnresolvedAttribute`; `resolveColumn` below
  // converts a missing name to `BIN_BY_COLUMN_NOT_FOUND` rather than letting the default
  // resolution pass leave them unresolved indefinitely.
  private def readyToResolve(b: UnresolvedBinBy): Boolean = {
    b.childrenResolved && b.binWidthExpr.resolved && b.originExpr.forall(_.resolved)
  }

  private def resolve(b: UnresolvedBinBy): LogicalPlan = {
    val resolver = SQLConf.get.resolver
    val child = b.child

    val rangeStart = resolveColumn(b.rangeStartCol, child, resolver)
    val rangeEnd = resolveColumn(b.rangeEndCol, child, resolver)
    val distributeAttrs = b.distributeColumns.map(c => resolveColumn(c, child, resolver))

    val rangeType = rangeStart.dataType
    if (!AnyTimestampType.acceptsType(rangeType)) {
      throw QueryCompilationErrors.binByRangeTypeMismatchError(rangeStart.name, rangeType)
    }
    if (rangeEnd.dataType != rangeType) {
      throw QueryCompilationErrors.binByRangeTypeMismatchError(rangeEnd.name, rangeEnd.dataType)
    }

    if (!b.binWidthExpr.foldable) {
      throw QueryCompilationErrors.binByNonFoldableInputError("BIN WIDTH", b.binWidthExpr)
    }
    // Evaluating a foldable expression can still throw (e.g., a CAST that fails under ANSI
    // mode, integer overflow inside a constant fold). Surface any such failure as a clean
    // analysis-time BIN_BY_INVALID_BIN_WIDTH rather than letting the raw exception escape.
    val binWidthValid = b.binWidthExpr.dataType match {
      case _: DayTimeIntervalType =>
        try {
          val v = b.binWidthExpr.eval(EmptyRow)
          v != null && v.asInstanceOf[Long] > 0L
        } catch {
          case NonFatal(_) => false
        }
      case _ => false
    }
    if (!binWidthValid) {
      throw QueryCompilationErrors.binByInvalidBinWidthError(b.binWidthExpr)
    }

    val sessionZone = SQLConf.get.sessionLocalTimeZone
    val isLTZ = rangeType.isInstanceOf[TimestampType]

    // `ALIGN TO` is optional. When omitted, default the resolved plan's origin to
    // `1970-01-01 00:00:00` in the session zone for `TIMESTAMP` (LTZ) and epoch for
    // `TIMESTAMP_NTZ`.
    val resolvedOrigin: Expression = b.originExpr match {
      case Some(o) =>
        if (!o.foldable) {
          throw QueryCompilationErrors.binByNonFoldableInputError("ALIGN TO", o)
        }
        if (o.dataType != rangeType) {
          throw QueryCompilationErrors.binByAlignToTypeMismatchError(o.dataType, rangeType)
        }
        o
      case None if isLTZ =>
        Literal(DateTimeUtils.daysToMicros(0, DateTimeUtils.getZoneId(sessionZone)), TimestampType)
      case None =>
        Literal(0L, TimestampNTZType)
    }

    if (distributeAttrs.isEmpty) {
      throw QueryCompilationErrors.binByMissingDistributeError()
    }
    distributeAttrs.foreach { attr =>
      attr.dataType match {
        case _: NumericType | _: DayTimeIntervalType => // ok
        case other =>
          throw QueryCompilationErrors.binByDistributeTypeMismatchError(attr.name, other)
      }
    }
    val seen = mutable.HashSet.empty[ExprId]
    distributeAttrs.foreach { attr =>
      if (!seen.add(attr.exprId)) {
        throw QueryCompilationErrors.binByDuplicateDistributeColumnError(attr.name)
      }
    }

    val appendedAttributes = BinBy.appendedAttributesWithAliases(rangeType, b.outputAliases)

    BinBy(
      binWidthExpr = b.binWidthExpr,
      rangeStart = rangeStart,
      rangeEnd = rangeEnd,
      originExpr = resolvedOrigin,
      distributeColumns = distributeAttrs,
      appendedAttributes = appendedAttributes,
      child = child,
      timeZoneId = if (isLTZ) Some(sessionZone) else None)
  }

  private def resolveColumn(
      expr: Expression,
      child: LogicalPlan,
      resolver: Resolver): Attribute = expr match {
    case u: UnresolvedAttribute =>
      child.resolve(u.nameParts, resolver) match {
        case Some(a: Attribute) => a
        case Some(_) =>
          // Resolved to a NamedExpression that is not a top-level Attribute (e.g.,
          // `RANGE struct_col.field TO ...` resolves to an Alias wrapping GetStructField).
          throw QueryCompilationErrors.binByColumnNotFoundError(u.name)
        case None => throw QueryCompilationErrors.binByColumnNotFoundError(u.name)
      }
    case a: Attribute => a
    case other =>
      // This branch is unreachable from user SQL: AstBuilder always builds UnresolvedAttribute,
      // and resolved Attributes are handled above.
      throw SparkException.internalError(
        s"Unexpected expression in BIN BY column position: $other")
  }
}
