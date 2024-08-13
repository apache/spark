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

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.Column
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.{ColumnNode, SortOrder, Window => EvalWindow, WindowFrame, WindowSpec => InternalWindowSpec}

/**
 * A window specification that defines the partitioning, ordering, and frame boundaries.
 *
 * Use the static methods in [[Window]] to create a [[WindowSpec]].
 *
 * @since 1.4.0
 */
@Stable
class WindowSpec private[sql](
    partitionSpec: Seq[ColumnNode],
    orderSpec: Seq[SortOrder],
    frame: Option[WindowFrame]) {

  /**
   * Defines the partitioning columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec = {
    partitionBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Defines the partitioning columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec = {
    new WindowSpec(cols.map(_.node), orderSpec, frame)
  }

  /**
   * Defines the ordering columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec = {
    orderBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Defines the ordering columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec = {
    new WindowSpec(partitionSpec, cols.map(_.sortOrder), frame)
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative positions from the current row. For example, "0" means
   * "current row", while "-1" means the row before the current row, and "5" means the fifth row
   * after the current row.
   *
   * We recommend users use `Window.unboundedPreceding`, `Window.unboundedFollowing`,
   * and `Window.currentRow` to specify special boundary values, rather than using integral
   * values directly.
   *
   * A row based boundary is based on the position of the row within the partition.
   * An offset indicates the number of rows above or below the current row, the frame for the
   * current row starts or ends. For instance, given a row based sliding frame with a lower bound
   * offset of -1 and a upper bound offset of +2. The frame for row with index 5 would range from
   * index 4 to index 7.
   *
   * {{{
   *   import org.apache.spark.sql.expressions.Window
   *   val df = Seq((1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b"))
   *     .toDF("id", "category")
   *   val byCategoryOrderedById =
   *     Window.partitionBy($"category").orderBy($"id").rowsBetween(Window.currentRow, 1)
   *   df.withColumn("sum", sum($"id") over byCategoryOrderedById).show()
   *
   *   +---+--------+---+
   *   | id|category|sum|
   *   +---+--------+---+
   *   |  1|       b|  3|
   *   |  2|       b|  5|
   *   |  3|       b|  3|
   *   |  1|       a|  2|
   *   |  1|       a|  3|
   *   |  2|       a|  2|
   *   +---+--------+---+
   * }}}
   *
   * @param start boundary start, inclusive. The frame is unbounded if this is
   *              the minimum long value (`Window.unboundedPreceding`).
   * @param end boundary end, inclusive. The frame is unbounded if this is the
   *            maximum long value (`Window.unboundedFollowing`).
   * @since 1.4.0
   */
  // Note: when updating the doc for this method, also update Window.rowsBetween.
  def rowsBetween(start: Long, end: Long): WindowSpec = {
    val boundaryStart = start match {
      case 0 => WindowFrame.CurrentRow
      case Long.MinValue => WindowFrame.UnboundedPreceding
      case x if Int.MinValue <= x && x <= Int.MaxValue => WindowFrame.value(x.toInt)
      case x => throw QueryCompilationErrors.invalidBoundaryStartError(x)
    }

    val boundaryEnd = end match {
      case 0 => WindowFrame.CurrentRow
      case Long.MaxValue => WindowFrame.UnboundedFollowing
      case x if Int.MinValue <= x && x <= Int.MaxValue => WindowFrame.value(x.toInt)
      case x => throw QueryCompilationErrors.invalidBoundaryEndError(x)
    }

    withFrame(WindowFrame.Row, boundaryStart, boundaryEnd)
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative from the current row. For example, "0" means
   * "current row", while "-1" means one off before the current row, and "5" means the five off
   * after the current row.
   *
   * We recommend users use `Window.unboundedPreceding`, `Window.unboundedFollowing`,
   * and `Window.currentRow` to specify special boundary values, rather than using long values
   * directly.
   *
   * A range-based boundary is based on the actual value of the ORDER BY
   * expression(s). An offset is used to alter the value of the ORDER BY expression, for
   * instance if the current order by expression has a value of 10 and the lower bound offset
   * is -3, the resulting lower bound for the current row will be 10 - 3 = 7. This however puts a
   * number of constraints on the ORDER BY expressions: there can be only one expression and this
   * expression must have a numerical data type. An exception can be made when the offset is
   * unbounded, because no value modification is needed, in this case multiple and non-numeric
   * ORDER BY expression are allowed.
   *
   * {{{
   *   import org.apache.spark.sql.expressions.Window
   *   val df = Seq((1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b"))
   *     .toDF("id", "category")
   *   val byCategoryOrderedById =
   *     Window.partitionBy($"category").orderBy($"id").rangeBetween(Window.currentRow, 1)
   *   df.withColumn("sum", sum($"id") over byCategoryOrderedById).show()
   *
   *   +---+--------+---+
   *   | id|category|sum|
   *   +---+--------+---+
   *   |  1|       b|  3|
   *   |  2|       b|  5|
   *   |  3|       b|  3|
   *   |  1|       a|  4|
   *   |  1|       a|  4|
   *   |  2|       a|  2|
   *   +---+--------+---+
   * }}}
   *
   * @param start boundary start, inclusive. The frame is unbounded if this is
   *              the minimum long value (`Window.unboundedPreceding`).
   * @param end boundary end, inclusive. The frame is unbounded if this is the
   *            maximum long value (`Window.unboundedFollowing`).
   * @since 1.4.0
   */
  // Note: when updating the doc for this method, also update Window.rangeBetween.
  def rangeBetween(start: Long, end: Long): WindowSpec = {
    val boundaryStart = start match {
      case 0 => WindowFrame.CurrentRow
      case Long.MinValue => WindowFrame.UnboundedPreceding
      case x => WindowFrame.value(x)
    }

    val boundaryEnd = end match {
      case 0 => WindowFrame.CurrentRow
      case Long.MaxValue => WindowFrame.UnboundedFollowing
      case x => WindowFrame.value(x)
    }
    withFrame(WindowFrame.Range, boundaryStart, boundaryEnd)
  }

  private[sql] def withFrame(
      frameType: WindowFrame.FrameType,
      lower: WindowFrame.FrameBoundary,
      uppper: WindowFrame.FrameBoundary): WindowSpec = {
    val frame = WindowFrame(frameType, lower, uppper)
    new WindowSpec(partitionSpec, orderSpec, Some(frame))
  }

  /**
   * Converts this [[WindowSpec]] into a [[Column]] with an aggregate expression.
   */
  private[sql] def withAggregate(aggregate: Column): Column = {
    val spec = InternalWindowSpec(partitionSpec, sortColumns = orderSpec, frame = frame)
    Column(EvalWindow(aggregate.node, spec))
  }
}
