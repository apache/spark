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

package org.apache.spark.sql

import scala.language.implicitConversions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.expressions._

/**
 * :: Experimental ::
 * A set of methods for window function definition for aggregate expressions.
 * For example:
 * {{{
 *   df.select(
 *     avg("value")
 *       .over
 *       .partitionBy("k1")
 *       .orderBy("k2", "k3")
 *       .row
 *       .following(1)
 *       .toColumn.as("avg_value"),
 *     max("value")
 *       .over
 *       .partitionBy("k2")
 *       .orderBy("k3")
 *       .between
 *       .preceding(4)
 *       .following(3)
 *       .toColumn.as("max_value"))
 * }}}
 *
 *
 */
@Experimental
class WindowFunctionDefinition protected[sql](
    column: Column,
    partitionSpec: Seq[Expression] = Nil,
    orderSpec: Seq[SortOrder] = Nil,
    frame: WindowFrame = UnspecifiedFrame) {

  /**
   * Returns a new [[WindowFunctionDefinition]] partitioned by the specified column.
   * {{{
   *   // The following 2 are equivalent
   *   df.over.partitionBy("k1", "k2", ...)
   *   df.over.partitionBy($"K1", $"k2", ...)
   * }}}
   * @group window_funcs
   */
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowFunctionDefinition = {
    partitionBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Returns a new [[WindowFunctionDefinition]] partitioned by the specified column. For example:
   * {{{
   *   df.over.partitionBy($"col1", $"col2")
   * }}}
   * @group window_funcs
   */
  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowFunctionDefinition = {
    new WindowFunctionDefinition(column, cols.map(_.expr), orderSpec, frame)
  }

  /**
   * Returns a new [[WindowFunctionDefinition]] sorted by the specified column within
   * the partition.
   * {{{
   *   // The following 2 are equivalent
   *   df.over.partitionBy("k1").orderBy("k2", "k3")
   *   df.over.partitionBy("k1").orderBy($"k2", $"k3")
   * }}}
   * @group window_funcs
   */
  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowFunctionDefinition = {
    orderBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Returns a new [[WindowFunctionDefinition]] sorted by the specified column within
   * the partition. For example
   * {{{
   *   df.over.partitionBy("k1").orderBy($"k2", $"k3")
   * }}}
   * @group window_funcs
   */
  def orderBy(cols: Column*): WindowFunctionDefinition = {
    val sortOrder: Seq[SortOrder] = cols.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    new WindowFunctionDefinition(column, partitionSpec, sortOrder, frame)
  }

  /**
   * Returns a new ranged [[WindowFunctionDefinition]]. For example:
   * {{{
   *   df.over.partitionBy("k1").orderBy($"k2", $"k3").between
   * }}}
   * @group window_funcs
   */
  def between: WindowFunctionDefinition = {
    new WindowFunctionDefinition(column, partitionSpec, orderSpec,
      SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, UnboundedFollowing))
  }

  /**
   * Returns a new [[WindowFunctionDefinition]], with fixed number of records
   * from/to CURRENT ROW. For example:
   * {{{
   *   df.over.partitionBy("k1").orderBy($"k2", $"k3").row
   * }}}
   * @group window_funcs
   */
  def rows: WindowFunctionDefinition = {
    new WindowFunctionDefinition(column, partitionSpec, orderSpec,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))
  }

  /**
   * Returns a new [[WindowFunctionDefinition]], with range of preceding position specified.
   * For a Ranged [[WindowFunctionDefinition]], the range is [CURRENT_ROW - n, unspecified]
   * For a Fixed Row [[WindowFunctionDefinition]], the range as [CURRENT_ROW - n, CURRENT_ROW].
   * For example:
   * {{{
   *   // The range is [CURRENT_ROW - 1, CURRENT_ROW]
   *   df.over.partitionBy("k1").orderBy($"k2", $"k3").row.preceding(1)
   *   // The range [CURRENT_ROW - 1, previous upper bound]
   *   df.over.partitionBy("k1").orderBy($"k2", $"k3").between.preceding(1)
   * }}}
   * If n equals 0, it will be considered as CURRENT_ROW
   * @group window_funcs
   */
  def preceding(n: Int): WindowFunctionDefinition = {
    val newFrame = frame match {
      case f @ SpecifiedWindowFrame(RowFrame, _, _) if n == 0 => // TODO should we need this?
        f.copy(frameStart = CurrentRow, frameEnd = CurrentRow)
      case f @ SpecifiedWindowFrame(RowFrame, _, _) =>
        f.copy(frameStart = ValuePreceding(n), frameEnd = CurrentRow)
      case f @ SpecifiedWindowFrame(RangeFrame, _, _) if n == 0 => f.copy(frameStart = CurrentRow)
      case f @ SpecifiedWindowFrame(RangeFrame, _, _) => f.copy(frameStart = ValuePreceding(n))
      case f => throw new UnsupportedOperationException(s"preceding on $f")
    }
    new WindowFunctionDefinition(column, partitionSpec, orderSpec, newFrame)
  }

  /**
   * Returns a new [[WindowFunctionDefinition]], with range of following position specified.
   * For a Ranged [[WindowFunctionDefinition]], the range is [unspecified, CURRENT_ROW + n]
   * For a Fixed Row [[WindowFunctionDefinition]], the range as [CURRENT_ROW, CURRENT_ROW + n].
   * For example:
   * {{{
   *   // The range is [CURRENT_ROW, CURRENT_ROW + 1]
   *   df.over.partitionBy("k1").orderBy($"k2", $"k3").row.following(1)
   *   // The range [previous lower bound, CURRENT_ROW + 1]
   *   df.over.partitionBy("k1").orderBy($"k2", $"k3").between.following(1)
   * }}}
   * If n equals 0, it will be considered as CURRENT_ROW
   * @group window_funcs
   */
  def following(n: Int): WindowFunctionDefinition = {
    val newFrame = frame match {
      case f @ SpecifiedWindowFrame(RowFrame, _, _) if n == 0 => // TODO should we need this?
        f.copy(frameStart = CurrentRow, frameEnd = CurrentRow)
      case f @ SpecifiedWindowFrame(RowFrame, _, _) =>
        f.copy(frameStart = CurrentRow, frameEnd = ValueFollowing(n))
      case f @ SpecifiedWindowFrame(RangeFrame, _, _) if n == 0 => f.copy(frameEnd = CurrentRow)
      case f @ SpecifiedWindowFrame(RangeFrame, _, _) => f.copy(frameEnd = ValuePreceding(n))
      case f => throw new UnsupportedOperationException(s"following on $f")
    }
    new WindowFunctionDefinition(column, partitionSpec, orderSpec, newFrame)
  }

  /**
   * Convert the window definition into a new Column.
   * Currently, only aggregate expressions are supported for window function. For Example:
   * {{{
   *   df.select(
   *     avg("value")
   *       .over
   *       .partitionBy("k1")
   *       .orderBy($"k2", $"k3")
   *       .row
   *       .following(1)
   *       .toColumn.as("avg_value"),
   *     max("value")
   *       .over
   *       .partitionBy("k2")
   *       .orderBy("k3")
   *       .between
   *       .preceding(4)
   *       .following(3)
   *       .toColumn.as("max_value"))
   * }}}
   * @group window_funcs
   */
  def toColumn: Column = {
    val windowExpr = column.expr match {
      case Average(child) => WindowExpression(
        UnresolvedWindowFunction("avg", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Sum(child) => WindowExpression(
        UnresolvedWindowFunction("sum", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Count(child) => WindowExpression(
        UnresolvedWindowFunction("count", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case First(child) => WindowExpression(
        // TODO this is a hack for Hive UDAF first_value
        UnresolvedWindowFunction("first_value", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Last(child) => WindowExpression(
        // TODO this is a hack for Hive UDAF last_value
        UnresolvedWindowFunction("last_value", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Min(child) => WindowExpression(
        UnresolvedWindowFunction("min", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Max(child) => WindowExpression(
        UnresolvedWindowFunction("max", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case wf: WindowFunction => WindowExpression(
        wf,
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case aggr: AggregateExpression =>
        throw new UnsupportedOperationException(
          """Only support Aggregate Functions:
            | avg, sum, count, first, last, min, max for now""".stripMargin)
      case x =>
        throw new UnsupportedOperationException(s"We don't support $x in window operation.")
    }
    new Column(windowExpr)
  }
}
