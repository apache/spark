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


sealed private[sql] class Frame(private[sql] var boundary: FrameBoundary = null)

/**
 * :: Experimental ::
 * An utility to specify the Window Frame Range.
 */
object Frame {
  val currentRow: Frame = new Frame(CurrentRow)
  val unbounded: Frame = new Frame()
  def preceding(n: Int): Frame = if (n == 0) {
    new Frame(CurrentRow)
  } else {
    new Frame(ValuePreceding(n))
  }

  def following(n: Int): Frame = if (n == 0) {
    new Frame(CurrentRow)
  } else {
    new Frame(ValueFollowing(n))
  }
}

/**
 * :: Experimental ::
 * A Window object with everything unset. But can build new Window object
 * based on it.
 */
@Experimental
object Window extends Window()

/**
 * :: Experimental ::
 * A set of methods for window function definition for aggregate expressions.
 * For example:
 * {{{
 *   // predefine a window
 *   val w = Window.partitionBy("name").orderBy("id")
 *            .rowsBetween(Frame.unbounded, Frame.currentRow)
 *   df.select(
 *     avg("age").over(Window.partitionBy("..", "..").orderBy("..", "..")
 *       .rowsBetween(Frame.unbounded, Frame.currentRow))
 *   )
 *
 *   df.select(
 *     avg("age").over(Window.partitionBy("..", "..").orderBy("..", "..")
 *      .rowsBetween(Frame.preceding(50), Frame.following(10)))
 *   )
 *
 * }}}
 *
 */
@Experimental
class Window {
  private var column: Column = _
  private var partitionSpec: Seq[Expression] = Nil
  private var orderSpec: Seq[SortOrder] = Nil
  private var frame: WindowFrame = UnspecifiedFrame

  private def this(
      column: Column = null,
      partitionSpec: Seq[Expression] = Nil,
      orderSpec: Seq[SortOrder] = Nil,
      frame: WindowFrame = UnspecifiedFrame) {
    this()
    this.column = column
    this.partitionSpec = partitionSpec
    this.orderSpec = orderSpec
    this.frame     = frame
  }

  private[sql] def newColumn(c: Column): Window = {
    new Window(c, partitionSpec, orderSpec, frame)
  }

  /**
   * Returns a new [[Window]] partitioned by the specified column.
   * {{{
   *   // The following 2 are equivalent
   *   df.over(Window.partitionBy("k1", "k2", ...))
   *   df.over(Window.partitionBy($"K1", $"k2", ...))
   * }}}
   * @group window_funcs
   */
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): Window = {
    partitionBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Returns a new [[Window]] partitioned by the specified column. For example:
   * {{{
   *   df.over(Window.partitionBy($"col1", $"col2"))
   * }}}
   * @group window_funcs
   */
  @scala.annotation.varargs
  def partitionBy(cols: Column*): Window = {
    new Window(column, cols.map(_.expr), orderSpec, frame)
  }

  /**
   * Returns a new [[Window]] sorted by the specified column within
   * the partition.
   * {{{
   *   // The following 2 are equivalent
   *   df.over(Window.partitionBy("k1").orderBy("k2", "k3"))
   *   df.over(Window.partitionBy("k1").orderBy($"k2", $"k3"))
   * }}}
   * @group window_funcs
   */
  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): Window = {
    orderBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Returns a new [[Window]] sorted by the specified column within
   * the partition. For example
   * {{{
   *   df.over(Window.partitionBy("k1").orderBy($"k2", $"k3"))
   * }}}
   * @group window_funcs
   */
  @scala.annotation.varargs
  def orderBy(cols: Column*): Window = {
    val sortOrder: Seq[SortOrder] = cols.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    new Window(column, partitionSpec, sortOrder, frame)
  }

  def rowsBetween(start: Frame, end: Frame): Window = {
    assert(start.boundary != UnboundedFollowing, "Start can not be UnboundedFollowing")
    assert(end.boundary != UnboundedPreceding, "End can not be UnboundedPreceding")

    val s = if (start.boundary == null) UnboundedPreceding else start.boundary
    val e = if (end.boundary == null) UnboundedFollowing else end.boundary

    new Window(column, partitionSpec, orderSpec, SpecifiedWindowFrame(RowFrame, s, e))
  }

  def rangeBetween(start: Frame, end: Frame): Window = {
    assert(start.boundary != UnboundedFollowing, "Start can not be UnboundedFollowing")
    assert(end.boundary != UnboundedPreceding, "End can not be UnboundedPreceding")

    val s = if (start.boundary == null) UnboundedPreceding else start.boundary
    val e = if (end.boundary == null) UnboundedFollowing else end.boundary

    new Window(column, partitionSpec, orderSpec, SpecifiedWindowFrame(RangeFrame, s, e))
  }

  /**
   * Convert the window definition into a Column object.
   * @group window_funcs
   */
  private[sql] def toColumn: Column = {
    if (column == null) {
      throw new AnalysisException("Window didn't bind with expression")
    }
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
      case x =>
        throw new UnsupportedOperationException(s"We don't support $x in window operation.")
    }
    new Column(windowExpr)
  }
}
