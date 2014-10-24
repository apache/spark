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

package org.apache.spark.ui

import java.util.Date

import scala.collection.mutable
import scala.xml.{Node, Text}

import org.apache.spark.util.Utils


/**
 * Describes how to render a column of values in a web UI table.
 *
 * @param name the name / title of this column
 * @param fieldExtractor function for extracting this field's value from the table's row data type
 * @tparam T the table's row data type
 * @tparam V this column's value type
 */
case class UITableColumn[T, V](
  name: String,
  fieldExtractor: T => V) {

  private var sortable: Boolean = true
  private var sortKey: Option[V => String] = None
  private var formatter: V => String = x => x.toString
  private var cellContentsRenderer: V => Seq[Node] = (data: V) => Text(formatter(data))

  /**
   * Optional method for sorting this table by a key other than the cell's text contents.
   */
  def sortBy(keyFunc: V => String): UITableColumn[T, V] = {
    sortKey = Some(keyFunc)
    this
  }

  /**
   * Override the default cell formatting of the extracted value.  By default, values are rendered
   * by calling toString().
   */
  def formatWith(formatFunc: V => String): UITableColumn[T, V] = {
    formatter = formatFunc
    this
  }

  /**
   * Make this column unsortable.  This is useful for columns that display UI elements, such
   * as buttons to link to logs
   */
  def isUnsortable(): UITableColumn[T, V] = {
    sortable = false
    this
  }

  /**
   * Customize the markup used to render this table cell.  The markup should only describe how to
   * render the contents of the TD tag, not the TD tag itself.  This overrides `formatWith`.
   */
  def withMarkup(markupFunc: V => Seq[Node]): UITableColumn[T, V] = {
    cellContentsRenderer = markupFunc
    this
  }

  /** Render the TD tag for this row */
  def _renderCell(row: T): Seq[Node] =  {
    val data = fieldExtractor(row)
    val cellContents = cellContentsRenderer(data)
    val cls = if (sortable) None else Some(Text("sorttable_nosort"))
    <td sorttable_customkey={sortKey.map(k => Text(k(data)))} class={cls}>
      {cellContents}
    </td>
  }
}

/**
 * Describes how to render a table to display rows of type `T`.
 * @param cols a sequence of UITableColumns that describe how each column should be rendered
 * @param fixedWidth if true, all columns of this table will be displayed with the same width
 * @tparam T the row data type
 */
private[spark] class UITable[T] (cols: Seq[UITableColumn[T, _]], fixedWidth: Boolean) {

  private val tableClass = if (fixedWidth) {
    UIUtils.TABLE_CLASS + " table-fixed"
  } else {
    UIUtils.TABLE_CLASS
  }

  private val colWidthAttr = if (fixedWidth) Some(Text((100.toDouble / cols.size) + "%")) else None

  private val headerRow: Seq[Node] = {
    val headers = cols.map(_.name)
    // if none of the headers have "\n" in them
    if (headers.forall(!_.contains("\n"))) {
      // represent header as simple text
      headers.map(h => <th width={colWidthAttr}>{h}</th>)
    } else {
      // represent header text as list while respecting "\n"
      headers.map { case h =>
        <th width={colWidthAttr}>
          <ul class="unstyled">
            { h.split("\n").map { case t => <li> {t} </li> } }
          </ul>
        </th>
      }
    }
  }

  private def renderRow(row: T): Seq[Node] = {
    val tds = cols.map(_._renderCell(row))
    <tr>{ tds }</tr>
  }

  /** Render the table with the given data */
  def render(data: Iterable[T]): Seq[Node] = {
    val rows = data.map(renderRow)
    <table class={tableClass}>
      <thead>{headerRow}</thead>
      <tbody>
        {rows}
      </tbody>
    </table>
  }
}

/**
 * Builder for constructing web UI tables.  This builder offers several advantages over constructing
 * tables by hand using raw XML:
 *
 *  - All of the table's data and formatting logic can live in one place; the table headers and
 *    rows aren't described in separate code.  This prevents several common errors, like changing
 *    the ordering of two column headers but forgetting to re-order the corresponding TD tags.
 *
 *  - No repetition of code for type-specific display rules: common column types like "memory",
 *    "duration", and "time" have convenience methods that implement the right formatting logic.
 *
 *  - Details of our specific markup are generally abstracted away.  For example, the markup for
 *    setting a custom sort key on a column now lives in one place, rather than being repeated
 *    in each table.
 *
 * The recommended way of using this class:
 *
 *  - Create a new builder that is parametrized by the type (`T`) of data that you want to render.
 *    In many cases, there may be some record type like `WorkerInfo` that holds all of the
 *    information needed to render a particular row.  If the data for each table row comes from
 *    several objects, you can combine those objects into a tuple or case-class.
 *
 *  - Use the `col` methods to add columns to this builder.  The final argument of each `col` method
 *    is a function that extracts the column's field from a row object of type `T`.  Columns are
 *    displayed in the order that they are added to the builder.  For most columns, you can write
 *    code like
 *
 *      builder.col("Id") { _.id }
 *      builder.sizeCol("Memory" { _.memory }
 *
 *    Columns have additional options, such as controlling their sort keys; see the individual
 *    methods' documentation for more details.
 *
 *  - Call `build()` to construct an immutable object which can be used to render tables.
 *
 * There are many other features, including support for arbitrary markup in custom column types;
 * see the actual uses in the web UI code for more details.
 *
 * @param fixedWidth if true, all columns will be rendered with the same width
 * @tparam T the type of the data items that will be used to render individual rows
 */
private[spark] class UITableBuilder[T](fixedWidth: Boolean = false) {
  private val cols = mutable.Buffer[UITableColumn[T, _]]()

  /**
   * General builder method for table columns.  By default, this extracts a field
   * and displays it as as a string.  You can call additional methods on the result
   * of this method to customize this column's display.
   */
  def col[V](name: String)(fieldExtractor: T => V): UITableColumn[T, V] = {
    val newCol = new UITableColumn[T, V](name, fieldExtractor)
    cols.append(newCol)
    newCol
  }

  /**
   * Display a column of sizes, in megabytes, as human-readable strings, such as "4.0 MB".
   */
  def sizeCol(name: String)(fieldExtractor: T => Long) {
    col[Long](name)(fieldExtractor) sortBy (x => x.toString) formatWith Utils.megabytesToString
  }

  /**
   * Display a column of dates as yyyy/MM/dd HH:mm:ss format.
   */
  def dateCol(name: String)(fieldExtractor: T => Date) {
    col[Date](name)(fieldExtractor) formatWith UIUtils.formatDate
  }

  /**
   * Display a column of dates as yyyy/MM/dd HH:mm:ss format.
   */
  def epochDateCol(name: String)(fieldExtractor: T => Long) {
    col[Long](name)(fieldExtractor) formatWith UIUtils.formatDate
  }

  /**
   * Display a column of durations, in milliseconds, as human-readable strings, such as "12 s".
   */
  def durationCol(name: String)(fieldExtractor: T => Long) {
    col[Long](name)(fieldExtractor) sortBy (_.toString) formatWith UIUtils.formatDuration
  }

  def build(): UITable[T] = {
    val immutableCols: Seq[UITableColumn[T, _]] = cols.toSeq
    new UITable[T](immutableCols, fixedWidth)
  }
}
