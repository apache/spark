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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{catalyst, Column}
import org.apache.spark.sql.catalyst.expressions._

/**
 * :: Experimental ::
 * A window specification that defines the partitioning, ordering, and frame boundaries.
 *
 * Use the static methods in [[Window]] to create a [[WindowSpec]].
 *
 * @since 1.4.0
 */
@Experimental
class WindowSpec private[sql](
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frame: catalyst.expressions.WindowFrame,
    exclude: ExcludeClause =  ExcludeClause.defaultExclude ) {

  /**
   * Defines the partitioning columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @_root_.scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec = {
    partitionBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Defines the partitioning columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @_root_.scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec = {
    new WindowSpec(cols.map(_.expr), orderSpec, frame)
  }

  /**
   * Defines the ordering columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @_root_.scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec = {
    orderBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Defines the ordering columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @_root_.scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec = {
    val sortOrder: Seq[SortOrder] = cols.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    new WindowSpec(partitionSpec, sortOrder, frame)
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative positions from the current row. For example, "0" means
   * "current row", while "-1" means the row before the current row, and "5" means the fifth row
   * after the current row.
   *
   * @param start boundary start, inclusive.
   *              The frame is unbounded if this is the minimum long value.
   * @param end boundary end, inclusive.
   *            The frame is unbounded if this is the maximum long value.
   * @since 1.4.0
   */
  def rowsBetween(start: Long, end: Long): WindowSpec = {
    between(RowFrame, start, end)
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative from the current row. For example, "0" means "current row",
   * while "-1" means one off before the current row, and "5" means the five off after the
   * current row.
   *
   * @param start boundary start, inclusive.
   *              The frame is unbounded if this is the minimum long value.
   * @param end boundary end, inclusive.
   *            The frame is unbounded if this is the maximum long value.
   * @since 1.4.0
   */
  def rangeBetween(start: Long, end: Long): WindowSpec = {
    between(RangeFrame, start, end)
  }

  private def between(typ: FrameType, start: Long, end: Long): WindowSpec = {
    val boundaryStart = start match {
      case 0 => CurrentRow
      case Long.MinValue => UnboundedPreceding
      case x if x < 0 => ValuePreceding(-start.toInt)
      case x if x > 0 => ValueFollowing(start.toInt)
    }

    val boundaryEnd = end match {
      case 0 => CurrentRow
      case Long.MaxValue => UnboundedFollowing
      case x if x < 0 => ValuePreceding(-end.toInt)
      case x if x > 0 => ValueFollowing(end.toInt)
    }

    new WindowSpec(
      partitionSpec,
      orderSpec,
      SpecifiedWindowFrame(typ, boundaryStart, boundaryEnd))
  }

  /**
   * Converts this [[WindowSpec]] into a [[Column]] with an aggregate expression.
   */
  private[sql] def withAggregate(aggregate: Column): Column = {
    val spec = WindowSpecDefinition(partitionSpec, orderSpec, frame, exclude)
    new Column(WindowExpression(aggregate.expr, spec))
  }
}
