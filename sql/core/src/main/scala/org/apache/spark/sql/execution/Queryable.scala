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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

import scala.util.control.NonFatal

/** A trait that holds shared code between DataFrames and Datasets. */
private[sql] trait Queryable {
  def schema: StructType
  def queryExecution: QueryExecution
  def sqlContext: SQLContext

  override def toString: String = {
    try {
      schema.map(f => s"${f.name}: ${f.dataType.simpleString}").mkString("[", ", ", "]")
    } catch {
      case NonFatal(e) =>
        s"Invalid tree; ${e.getMessage}:\n$queryExecution"
    }
  }

  /**
   * Prints the schema to the console in a nice tree format.
   * @group basic
   * @since 1.3.0
   */
  // scalastyle:off println
  def printSchema(): Unit = println(schema.treeString)
  // scalastyle:on println

  /**
   * Prints the plans (logical and physical) to the console for debugging purposes.
   * @since 1.3.0
   */
  def explain(extended: Boolean): Unit = {
    val explain = ExplainCommand(queryExecution.logical, extended = extended)
    sqlContext.executePlan(explain).executedPlan.executeCollect().foreach {
      // scalastyle:off println
      r => println(r.getString(0))
      // scalastyle:on println
    }
  }

  /**
   * Only prints the physical plan to the console for debugging purposes.
   * @since 1.3.0
   */
  def explain(): Unit = explain(extended = false)

  /**
   * Displays the [[Queryable]] in a tabular form. Strings more than 20 characters
   * will be truncated, and all cells will be aligned right. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   * @param numRows Number of rows to show
   *
   * @group action
   * @since 1.3.0
   */
  def show(numRows: Int): Unit = show(numRows, truncate = true)

  /**
   * Displays the top 20 rows of [[Queryable]] in a tabular form. Strings more than 20 characters
   * will be truncated, and all cells will be aligned right.
   *
   * @group action
   * @since 1.3.0
   */
  def show(): Unit = show(20)

  /**
   * Displays the top 20 rows of [[Queryable]] in a tabular form.
   *
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *              be truncated and all cells will be aligned right
   *
   * @group action
   * @since 1.5.0
   */
  def show(truncate: Boolean): Unit = show(20, truncate)

  /**
   * Displays the [[Queryable]] in a tabular form. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   * @param numRows Number of rows to show
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *              be truncated and all cells will be aligned right
   *
   * @group action
   * @since 1.5.0
   */
  // scalastyle:off println
  def show(numRows: Int, truncate: Boolean): Unit = println(showString(numRows, truncate))
  // scalastyle:on println

  /**
   * Compose the string representing rows for output
   * @param _numRows The max limit of rows to show
   * @param truncate Whether truncate long strings and align cells right
   */
  private[sql] def showString(_numRows: Int, truncate: Boolean = true): String

  /**
   * Format the string representing rows for output
   * @param rows The rows to show
   * @param numRows Number of rows to show
   * @param hasMoreData Whether some rows are not shown due to the limit
   * @param truncate Whether truncate long strings and align cells right
   *
   */
  private[sql] def formatString (rows: Seq[Seq[String]],
                                 numRows: Int,
                                 hasMoreData : Boolean,
                                 truncate: Boolean = true): String = {
    val sb = new StringBuilder
    val numCols = schema.fieldNames.length

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    rows.head.zipWithIndex.map { case (cell, i) =>
      if (truncate) {
        StringUtils.leftPad(cell, colWidths(i))
      } else {
        StringUtils.rightPad(cell, colWidths(i))
      }
    }.addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex.map { case (cell, i) =>
        if (truncate) {
          StringUtils.leftPad(cell.toString, colWidths(i))
        } else {
          StringUtils.rightPad(cell.toString, colWidths(i))
        }
      }.addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    // For Data that has more than "numRows" records
    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }
}
