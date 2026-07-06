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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, EmptyRow, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.BinByOutputAliases
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{
  AnyTimestampType,
  DataType,
  DayTimeIntervalType,
  DoubleType,
  FloatType,
  TimestampType
}

/**
 * Folded, validated BIN BY parameters produced by
 * [[BinByResolution.validateAndComputeParameters]].
 *
 * @param binWidthMicros bin width in microseconds, folded from the BIN WIDTH interval
 * @param originMicros ALIGN TO origin in microseconds; epoch (session-zone epoch for LTZ) when the
 *   ALIGN TO clause is omitted
 * @param rangeType data type of the RANGE columns (`TIMESTAMP` or `TIMESTAMP_NTZ`)
 * @param timeZoneId session time zone, set iff `rangeType` is `TIMESTAMP` (LTZ)
 */
case class ResolvedBinByParameters(
    binWidthMicros: Long,
    originMicros: Long,
    rangeType: DataType,
    timeZoneId: Option[String])

/**
 * Shared BIN BY validation and folding for the [[ResolveBinBy]] rule and the single-pass
 * `BinByResolver`: validates types/foldability and folds bin width and origin to microseconds.
 */
object BinByResolution {

  /**
   * Validates the BIN BY range, bin width, align-to, and distribute inputs and folds the bin width
   * and origin to microseconds, returning the [[ResolvedBinByParameters]].
   */
  def validateAndComputeParameters(
      rangeStart: Attribute,
      rangeEnd: Attribute,
      distributeAttributes: Seq[Attribute],
      binWidthExpr: Expression,
      originExpr: Option[Expression]): ResolvedBinByParameters = {
    val rangeType = rangeStart.dataType

    if (!AnyTimestampType.acceptsType(rangeType)) {
      throw QueryCompilationErrors.binByRangeTypeMismatchError(rangeStart.name, rangeType)
    }
    if (rangeEnd.dataType != rangeType) {
      throw QueryCompilationErrors.binByRangeTypeMismatchError(rangeEnd.name, rangeEnd.dataType)
    }

    if (!binWidthExpr.foldable) {
      throw QueryCompilationErrors.binByNonFoldableInputError("BIN WIDTH", binWidthExpr)
    }

    val binWidthMicros: Long = binWidthExpr.dataType match {
      case _: DayTimeIntervalType =>
        val v = try {
          binWidthExpr.eval(EmptyRow)
        } catch {
          case NonFatal(_) =>
            throw QueryCompilationErrors.binByInvalidBinWidthError(binWidthExpr)
        }
        if (v == null) {
          throw QueryCompilationErrors.binByNullArgumentError("BIN WIDTH")
        }
        if (v.asInstanceOf[Long] <= 0L) {
          throw QueryCompilationErrors.binByNonPositiveBinWidthError(binWidthExpr)
        }
        v.asInstanceOf[Long]
      case _ =>
        throw QueryCompilationErrors.binByInvalidBinWidthTypeError(binWidthExpr)
    }

    val sessionZone = SQLConf.get.sessionLocalTimeZone
    val isLTZ = rangeType.isInstanceOf[TimestampType]

    // `ALIGN TO` is optional. When omitted, default the resolved plan's origin to
    // `1970-01-01 00:00:00` in the session zone for `TIMESTAMP` (LTZ) and epoch for
    // `TIMESTAMP_NTZ`.
    val originMicros: Long = originExpr match {
      case Some(o) =>
        if (!o.foldable) {
          throw QueryCompilationErrors.binByNonFoldableInputError("ALIGN TO", o)
        }
        if (o.dataType != rangeType) {
          throw QueryCompilationErrors.binByAlignToTypeMismatchError(o.dataType, rangeType)
        }
        val v = try {
          o.eval(EmptyRow)
        } catch {
          case NonFatal(_) =>
            throw QueryCompilationErrors.binByInvalidAlignToError(o)
        }

        if (v == null) {
          throw QueryCompilationErrors.binByNullArgumentError("ALIGN TO")
        }

        v.asInstanceOf[Long]
      case None if isLTZ =>
        DateTimeUtils.daysToMicros(0, DateTimeUtils.getZoneId(sessionZone))
      case None =>
        0L
    }

    if (distributeAttributes.isEmpty) {
      throw QueryCompilationErrors.binByMissingDistributeError()
    }

    distributeAttributes.foreach { attr =>
      attr.dataType match {
        case _: FloatType | _: DoubleType =>
        case other =>
          throw QueryCompilationErrors.binByInvalidDistributeColumnTypeError(attr.name, other)
      }
    }
    val seen = mutable.HashSet.empty[ExprId]
    distributeAttributes.foreach { attr =>
      if (!seen.add(attr.exprId)) {
        throw QueryCompilationErrors.binByDuplicateDistributeColumnError(attr.name)
      }
    }

    ResolvedBinByParameters(
      binWidthMicros = binWidthMicros,
      originMicros = originMicros,
      rangeType = rangeType,
      timeZoneId = if (isLTZ) Some(sessionZone) else None
    )
  }

  /**
   * Builds the three appended output attributes (`bin_start`, `bin_end`, `bin_distribute_ratio`),
   * applying `aliases`; `rangeType` is the type of `bin_start` / `bin_end`.
   */
  def appendedAttributesWithAliases(
      rangeType: DataType,
      aliases: BinByOutputAliases): Seq[Attribute] = Seq(
    AttributeReference(aliases.effectiveBinStart, rangeType, nullable = true)(),
    AttributeReference(aliases.effectiveBinEnd, rangeType, nullable = true)(),
    AttributeReference(aliases.effectiveBinRatio, DoubleType, nullable = true)())

  /**
   * Mints a produced output attribute for each DISTRIBUTE input column: same name, type, and
   * nullability, but a fresh `ExprId` so the rescaled value is a distinct attribute from the input.
   */
  def scaledDistributeAttributes(distributeColumns: Seq[Attribute]): Seq[Attribute] =
    distributeColumns.map(a => AttributeReference(a.name, a.dataType, a.nullable)())
}
