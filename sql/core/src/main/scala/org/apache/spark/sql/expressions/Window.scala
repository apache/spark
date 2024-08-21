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

/**
 * Utility functions for defining window in DataFrames.
 *
 * {{{
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *   Window.partitionBy("country").orderBy("date")
 *     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
 *
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
 *   Window.partitionBy("country").orderBy("date").rowsBetween(-3, 3)
 * }}}
 *
 * @note When ordering is not defined, an unbounded window frame (rowFrame, unboundedPreceding,
 *       unboundedFollowing) is used by default. When ordering is defined, a growing window frame
 *       (rangeFrame, unboundedPreceding, currentRow) is used by default.
 *
 * @since 1.4.0
 */
@Stable
object Window {

  /**
   * Creates a [[WindowSpec]] with the partitioning defined.
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec = {
    spec.partitionBy(colName, colNames : _*)
  }

  /**
   * Creates a [[WindowSpec]] with the partitioning defined.
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec = {
    spec.partitionBy(cols : _*)
  }

  /**
   * Creates a [[WindowSpec]] with the ordering defined.
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec = {
    spec.orderBy(colName, colNames : _*)
  }

  /**
   * Creates a [[WindowSpec]] with the ordering defined.
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec = {
    spec.orderBy(cols : _*)
  }

  /**
   * Value representing the first row in the partition, equivalent to "UNBOUNDED PRECEDING" in SQL.
   * This can be used to specify the frame boundaries:
   *
   * {{{
   *   Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
   * }}}
   *
   * @since 2.1.0
   */
  def unboundedPreceding: Long = Long.MinValue

  /**
   * Value representing the last row in the partition, equivalent to "UNBOUNDED FOLLOWING" in SQL.
   * This can be used to specify the frame boundaries:
   *
   * {{{
   *   Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
   * }}}
   *
   * @since 2.1.0
   */
  def unboundedFollowing: Long = Long.MaxValue

  /**
   * Value representing the current row. This can be used to specify the frame boundaries:
   *
   * {{{
   *   Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
   * }}}
   *
   * @since 2.1.0
   */
  def currentRow: Long = 0

  /**
   * Creates a [[WindowSpec]] with the frame boundaries defined,
   * from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are positions relative to the current row. For example, "0" means
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
   * @since 2.1.0
   */
  // Note: when updating the doc for this method, also update WindowSpec.rowsBetween.
  def rowsBetween(start: Long, end: Long): WindowSpec = {
    spec.rowsBetween(start, end)
  }

  /**
   * Creates a [[WindowSpec]] with the frame boundaries defined,
   * from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative to the current row. For example, "0" means "current row",
   * while "-1" means one off before the current row, and "5" means the five off after the
   * current row.
   *
   * We recommend users use `Window.unboundedPreceding`, `Window.unboundedFollowing`,
   * and `Window.currentRow` to specify special boundary values, rather than using long values
   * directly.
   *
   * A range-based boundary is based on the actual value of the ORDER BY
   * expression(s). An offset is used to alter the value of the ORDER BY expression,
   * for instance if the current ORDER BY expression has a value of 10 and the lower bound offset
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
   * @since 2.1.0
   */
  // Note: when updating the doc for this method, also update WindowSpec.rangeBetween.
  def rangeBetween(start: Long, end: Long): WindowSpec = {
    spec.rangeBetween(start, end)
  }

  private[sql] def spec: WindowSpec = {
    new WindowSpec(Seq.empty, Seq.empty, None)
  }

}

/**
 * Utility functions for defining window in DataFrames.
 *
 * {{{
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *   Window.partitionBy("country").orderBy("date")
 *     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
 *
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
 *   Window.partitionBy("country").orderBy("date").rowsBetween(-3, 3)
 * }}}
 *
 * @since 1.4.0
 */
@Stable
class Window private()  // So we can see Window in JavaDoc.
