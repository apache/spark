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
 * @param formatter function that formats values for display in the table
 * @param sortable if false, this column will not be sortable
 * @param sortKey optional function for sorting by a key other than `formatter(value)`
 * @param fieldExtractor function for extracting this field's value from the table's row data type
 * @tparam T the table's row data type
 * @tparam V this column's value type
 */
private case class UITableColumn[T, V](
  name: String,
  formatter: V => String,
  sortable: Boolean,
  sortKey: Option[V => String],
  fieldExtractor: T => V) {

  /** Render the TD tag for this row */
  def renderCell(row: T): Seq[Node] =  {
    val data = fieldExtractor(row)
    val cellContents = renderCellContents(data)
    <td sorttable_customkey={sortKey.map(k => Text(k(data)))}>
      {cellContents}
    </td>
  }

  /** Render the contents of the TD tag for this row.  The contents may be a string or HTML */
  def renderCellContents(data: V): Seq[Node] = {
    Text(formatter(data))
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
    val tds = cols.map(_.renderCell(row))
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
 *      builder.memCol("Memory" { _.memory }
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
   * Display a column with custom HTML markup.  The markup should only describe how to
   * render the contents of the TD tag, not the TD tag itself.
   */
  def customCol[V](
      name: String,
      sortable: Boolean = true,
      sortKey: Option[T => String] = None)(renderer: T => Seq[Node]): UITableBuilder[T] = {
    val customColumn = new UITableColumn[T, T](name, null, sortable, sortKey, identity) {
      override def renderCellContents(row: T) = renderer(row)
    }
    cols.append(customColumn)
    this
  }

  def col[V](
      name: String,
      formatter: V => String,
      sortable: Boolean = true,
      sortKey: Option[V => String] = None)(fieldExtractor: T => V): UITableBuilder[T] = {
    cols.append(UITableColumn(name, formatter, sortable, sortKey, fieldExtractor))
    this
  }

  def col(
      name: String,
      sortable: Boolean = true,
      sortKey: Option[String => String] = None)(fieldExtractor: T => String): UITableBuilder[T] = {
    col[String](name, {x: String => x}, sortable, sortKey)(fieldExtractor)
  }

  def intCol(
      name: String,
      formatter: Int => String = { x: Int => x.toString },
      sortable: Boolean = true)(fieldExtractor: T => Int): UITableBuilder[T] = {
    col[Int](name, formatter, sortable = sortable)(fieldExtractor)
  }

  /**
   * Display a column of sizes, in megabytes, as human-readable strings, such as "4.0 MB".
   */
  def sizeCol(name: String)(fieldExtractor: T => Long): UITableBuilder[T] = {
    col[Long](
      name,
      formatter = Utils.megabytesToString,
      sortKey = Some(x => x.toString))(fieldExtractor)
  }

  /**
   * Display a column of dates as yyyy/MM/dd HH:mm:ss format.
   */
  def dateCol(name: String)(fieldExtractor: T => Date): UITableBuilder[T] = {
    col[Date](name, formatter = UIUtils.formatDate)(fieldExtractor)
  }

  /**
   * Display a column of dates as yyyy/MM/dd HH:mm:ss format.
   */
  def epochDateCol(name: String)(fieldExtractor: T => Long): UITableBuilder[T] = {
    col[Long](name, formatter = UIUtils.formatDate)(fieldExtractor)
  }

  /**
   * Display a column of durations, in milliseconds, as human-readable strings, such as "12 s".
   */
  def durationCol(name: String)(fieldExtractor: T => Long): UITableBuilder[T] = {
    col[Long](name, formatter = UIUtils.formatDuration, sortKey = Some(_.toString))(fieldExtractor)
  }

  /**
   * Display a column of optional durations, in milliseconds, as human-readable strings,
   * such as "12 s".  If the duration is None, then '-' will be displayed.
   */
  def optDurationCol(name: String)(fieldExtractor: T => Option[Long]): UITableBuilder[T] = {
    col[Option[Long]](
      name,
      formatter = { _.map(UIUtils.formatDuration).getOrElse("-")},
      sortKey = Some(_.getOrElse("-").toString)
    )(fieldExtractor)
  }

  def build(): UITable[T] = {
    val immutableCols: Seq[UITableColumn[T, _]] = cols.toSeq
    new UITable[T](immutableCols, fixedWidth)
  }
}
