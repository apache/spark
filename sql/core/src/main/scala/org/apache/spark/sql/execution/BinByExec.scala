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

package org.apache.spark.sql.execution

import java.time.ZoneOffset

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSet, BindReferences, BoundReference, Cast, Expression, GenericInternalRow, JoinedRow, Literal, Multiply, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.DoubleType

/**
 * Physical node for the `BIN BY` relation operator. For each input row it emits one output row per
 * bin overlapping `[rangeStart, rangeEnd)`.
 *
 * Per output sub-row: the DISTRIBUTE UNIFORM columns are scaled by the overlap fraction (the bin's
 * half-open intersection with `[rangeStart, rangeEnd)`); the other forwarded columns, including the
 * range columns, are replicated unchanged; `bin_start`, `bin_end`, `bin_distribute_ratio` are
 * appended. Bin boundaries reuse [[DateTimeUtils.timeBucketDTInterval]] /
 * [[DateTimeUtils.timestampAddDayTime]], so they match `time_bucket`: sub-day widths use UTC
 * microsecond arithmetic, multi-day widths use civil-time arithmetic in the session zone (UTC for
 * TIMESTAMP_NTZ).
 *
 * DISTRIBUTE UNIFORM columns are FLOAT or DOUBLE only (enforced by `ResolveBinBy`); scaling is IEEE
 * multiplication by the ratio, no rounding. Per-row edge cases: a NULL in either range column emits
 * one row with all computed columns NULL (scaled DISTRIBUTE values and all three appended columns);
 * a zero-length range emits one row with ratio 1.0; an inverted range raises
 * `BIN_BY_INVALID_RANGE`.
 *
 * Output shape mirrors the logical `BinBy`: the child columns with each DISTRIBUTE slot swapped to
 * its scaled produced attribute, then the three appended columns.
 */
case class BinByExec(
    binWidthMicros: Long,
    originMicros: Long,
    rangeStart: Attribute,
    rangeEnd: Attribute,
    distributeColumns: Seq[Attribute],
    scaledDistributeColumns: Seq[Attribute],
    appendedAttributes: Seq[Attribute],
    timeZoneId: Option[String],
    child: SparkPlan)
  extends UnaryExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private val distributeReplacements: AttributeMap[Attribute] =
    AttributeMap(distributeColumns.zip(scaledDistributeColumns))

  // Appended columns are [bin_start, bin_end, bin_distribute_ratio]; the ratio is the last one.
  private val numAppended: Int = appendedAttributes.length
  assert(numAppended == 3,
    s"BinBy appends exactly 3 columns (bin_start, bin_end, bin_distribute_ratio), got $numAppended")

  override def output: Seq[Attribute] =
    child.output.map(a => distributeReplacements.getOrElse(a, a)) ++ appendedAttributes

  override def producedAttributes: AttributeSet =
    AttributeSet(scaledDistributeColumns ++ appendedAttributes)

  override protected def withNewChildInternal(newChild: SparkPlan): BinByExec =
    copy(child = newChild)

  protected override def doExecute(): RDD[InternalRow] = {
    val width = binWidthMicros
    val origin = originMicros
    val zone = timeZoneId.map(DateTimeUtils.getZoneId).getOrElse(ZoneOffset.UTC)
    val numOutputRows = longMetric("numOutputRows")

    val rsIdx = bindOrdinal(rangeStart)
    val reIdx = bindOrdinal(rangeEnd)
    val childLen = child.output.length

    // Main projection: runs over JoinedRow(childRow, appendedRow) where appendedRow holds
    // [bin_start, bin_end, bin_distribute_ratio]. DISTRIBUTE columns are scaled;
    // all other forwarded columns are passed through by reference.
    val projExprs = buildOutputExpressions(childLen)

    // Null-range projection: runs directly over the child row (no appended row needed).
    // Non-DISTRIBUTE forwarded columns pass through; DISTRIBUTE columns and the three appended
    // columns are literal NULL.
    val nullProjExprs = buildNullRangeExpressions()

    child.execute().mapPartitionsInternal { rows =>
      val proj = UnsafeProjection.create(projExprs)
      val nullProj = UnsafeProjection.create(nullProjExprs)
      val appended = new GenericInternalRow(numAppended)
      val joined = new JoinedRow
      val fmt = TimestampFormatter.getFractionFormatter(zone)

      rows.flatMap { row =>
        joined.withLeft(row)
        val rowIter: Iterator[InternalRow] =
          if (row.isNullAt(rsIdx) || row.isNullAt(reIdx)) {
            Iterator.single(nullProj(row))
          } else {
            val rs = row.getLong(rsIdx)
            val re = row.getLong(reIdx)
            if (rs > re) {
              throw QueryExecutionErrors.binByInvalidRangeError(fmt.format(rs), fmt.format(re))
            } else if (rs == re) {
              val binStart = DateTimeUtils.timeBucketDTInterval(width, rs, origin, zone)
              val binEnd = DateTimeUtils.timestampAddDayTime(binStart, width, zone)
              appended.update(0, binStart)
              appended.update(1, binEnd)
              appended.update(2, 1.0d)
              Iterator.single(proj(joined.withRight(appended)))
            } else {
              val total = Math.subtractExact(re, rs)
              new Iterator[InternalRow] {
                private var curStart =
                  DateTimeUtils.timeBucketDTInterval(width, rs, origin, zone)

                override def hasNext: Boolean = curStart < re

                override def next(): InternalRow = {
                  val curEnd = DateTimeUtils.timestampAddDayTime(curStart, width, zone)
                  val overlap = math.min(re, curEnd) - math.max(rs, curStart)
                  appended.update(0, curStart)
                  appended.update(1, curEnd)
                  appended.update(2, overlap.toDouble / total.toDouble)
                  curStart = curEnd
                  proj(joined.withRight(appended))
                }
              }
            }
          }
        rowIter.map { r =>
          numOutputRows += 1
          r
        }
      }
    }
  }

  private def bindOrdinal(a: Attribute): Int =
    (BindReferences.bindReference(a, child.output): Expression) match {
      case b: BoundReference => b.ordinal
      case _ =>
        throw SparkException.internalError(
          s"BinByExec attribute ${a.name}#${a.exprId.id} is not bound in child output " +
            s"${child.output.mkString("[", ", ", "]")}")
    }

  /**
   * Builds the scaled projection over `JoinedRow(childRow, appendedRow)`: a `BoundReference` for
   * each forwarded child column, `Cast(Multiply(Cast(col, Double), ratioRef))` for the
   * DISTRIBUTE columns, and `BoundReference`s into the appended row for the appended cols.
   */
  private def buildOutputExpressions(childLen: Int): Seq[Expression] = {
    val distSet = distributeColumns.map(bindOrdinal).toSet
    // bin_distribute_ratio is the last appended column.
    val ratioRef = BoundReference(childLen + numAppended - 1, DoubleType, nullable = false)

    val forwarded = child.output.map { attr =>
      val ordinal = bindOrdinal(attr)
      val ref = BoundReference(ordinal, attr.dataType, attr.nullable)
      if (distSet.contains(ordinal)) {
        // Multiply in double then narrow back to the column's type. A FLOAT column would
        // otherwise form a mismatched Multiply(FloatType, DoubleType): these expressions are
        // bound at execution time, so the analyzer's operand coercion never runs. For DOUBLE the
        // inner cast is a no-op; for FLOAT this is exactly `(value * ratio).toFloat`.
        Cast(Multiply(Cast(ref, DoubleType), ratioRef), attr.dataType)
      } else {
        ref
      }
    }

    val appendedRefs = appendedAttributes.zipWithIndex.map { case (attr, i) =>
      BoundReference(childLen + i, attr.dataType, attr.nullable)
    }

    forwarded ++ appendedRefs
  }

  /**
   * Builds the null-range projection over the child row only: non-DISTRIBUTE forwarded columns
   * pass through unchanged; DISTRIBUTE columns and the three appended columns are NULL (no valid
   * bin exists, so no scaled value or bin boundary can be computed).
   */
  private def buildNullRangeExpressions(): Seq[Expression] = {
    val distSet = distributeColumns.map(bindOrdinal).toSet
    val forwarded = child.output.map { attr =>
      val ordinal = bindOrdinal(attr)
      if (distSet.contains(ordinal)) Literal.create(null, attr.dataType)
      else BoundReference(ordinal, attr.dataType, attr.nullable)
    }
    val nullAppended = appendedAttributes.map { attr =>
      Literal.create(null, attr.dataType)
    }
    forwarded ++ nullAppended
  }
}
