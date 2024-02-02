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

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters._
import scala.xml.{Node, Unparsed}

import com.google.common.base.Splitter
import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.util.Utils

/**
 * A data source that provides data for a page.
 *
 * @param pageSize the number of rows in a page
 */
private[spark] abstract class PagedDataSource[T](val pageSize: Int) {

  /**
   * Return the size of all data.
   */
  protected def dataSize: Int

  /**
   * Slice a range of data.
   */
  protected def sliceData(from: Int, to: Int): collection.Seq[T]

  /**
   * Slice the data for this page
   */
  def pageData(page: Int): PageData[T] = {
    // Display all the data in one page, if the pageSize is less than or equal to zero.
    val pageTableSize = if (pageSize <= 0) {
      dataSize
    } else {
      pageSize
    }
    val totalPages = (dataSize + pageTableSize - 1) / pageTableSize

    val pageToShow = if (page <= 0) {
      1
    } else if (page > totalPages) {
      totalPages
    } else {
      page
    }

    val (from, to) = ((pageToShow - 1) * pageSize, dataSize.min(pageToShow * pageTableSize))

    PageData(totalPages, sliceData(from, to))
  }

}

/**
 * The data returned by `PagedDataSource.pageData`, including the page number, the number of total
 * pages and the data in this page.
 */
private[ui] case class PageData[T](totalPage: Int, data: collection.Seq[T])

/**
 * A paged table that will generate a HTML table for a specified page and also the page navigation.
 */
private[spark] trait PagedTable[T] {

  def tableId: String

  def tableCssClass: String

  def pageSizeFormField: String

  def pageNumberFormField: String

  def dataSource: PagedDataSource[T]

  def headers: Seq[Node]

  def row(t: T): Seq[Node]

  def table(page: Int): Seq[Node] = {
    val _dataSource = dataSource
    try {
      val PageData(totalPages, data) = _dataSource.pageData(page)

      val pageToShow = if (page <= 0) {
        1
      } else if (page > totalPages) {
        totalPages
      } else {
        page
      }
      // Display all the data in one page, if the pageSize is less than or equal to zero.
      val pageSize = if (_dataSource.pageSize <= 0) {
        data.size
      } else {
        _dataSource.pageSize
      }

      val pageNaviTop = pageNavigation(pageToShow, pageSize, totalPages, tableId + "-top")
      val pageNaviBottom = pageNavigation(pageToShow, pageSize, totalPages, tableId + "-bottom")

      <div>
        {pageNaviTop}
        <table class={tableCssClass} id={tableId}>
          {headers}
          <tbody>
            {data.map(row)}
          </tbody>
        </table>
        {pageNaviBottom}
      </div>
    } catch {
      case e: IndexOutOfBoundsException =>
        val PageData(totalPages, _) = _dataSource.pageData(1)
        <div>
          {pageNavigation(1, _dataSource.pageSize, totalPages)}
          <div class="alert alert-error">
            <p>Error while rendering table:</p>
            <pre>
              {Utils.exceptionString(e)}
            </pre>
          </div>
        </div>
    }
  }

  /**
   * Return a page navigation.
   *
   * It will create a page navigation including a group of page numbers and a form
   * to submit the page number.
   *
   * Here are some examples of the page navigation:
   * {{{
   * << < 11 12 13* 14 15 16 17 18 19 20 > >>
   *
   * This is the first group, so "<<" is hidden.
   * < 1 2* 3 4 5 6 7 8 9 10 > >>
   *
   * This is the first group and the first page, so "<<" and "<" are hidden.
   * 1* 2 3 4 5 6 7 8 9 10 > >>
   *
   * Assume totalPages is 19. This is the last group, so ">>" is hidden.
   * << < 11 12 13* 14 15 16 17 18 19 >
   *
   * Assume totalPages is 19. This is the last group and the last page, so ">>" and ">" are hidden.
   * << < 11 12 13 14 15 16 17 18 19*
   *
   * * means the current page number
   * << means jumping to the first page of the previous group.
   * < means jumping to the previous page.
   * >> means jumping to the first page of the next group.
   * > means jumping to the next page.
   * }}}
   */
  private[ui] def pageNavigation(
      page: Int,
      pageSize: Int,
      totalPages: Int,
      navigationId: String = tableId): Seq[Node] = {
    // A group includes all page numbers will be shown in the page navigation.
    // The size of group is 10 means there are 10 page numbers will be shown.
    // The first group is 1 to 10, the second is 2 to 20, and so on
    val groupSize = 10
    val firstGroup = 0
    val lastGroup = (totalPages - 1) / groupSize
    val currentGroup = (page - 1) / groupSize
    val startPage = currentGroup * groupSize + 1
    val endPage = totalPages.min(startPage + groupSize - 1)
    val pageTags = (startPage to endPage).map { p =>
      if (p == page) {
        // The current page should be disabled so that it cannot be clicked.
        <li class="page-item disabled"><a href="#" class="page-link">{p}</a></li>
      } else {
        <li class="page-item"><a href={Unparsed(pageLink(p))} class="page-link">{p}</a></li>
      }
    }

    val hiddenFormFields = {
      if (goButtonFormPath.contains('?')) {
        val queryString = goButtonFormPath.split("\\?", 2)(1)
        val search = queryString.split("#")(0)
        Splitter
          .on('&')
          .trimResults()
          .omitEmptyStrings()
          .withKeyValueSeparator("=")
          .split(search)
          .asScala
          .filter { case (k, _) => k != pageSizeFormField}
          .filter { case (k, _) => k != pageNumberFormField}
          .map { case (k, v) => (k, URLDecoder.decode(v, UTF_8.name())) }
          .map { case (k, v) =>
            <input type="hidden" name={k} value={v} />
          }
      } else {
        Seq.empty
      }
    }

    <div>
      <div>
        <form id={s"form-$navigationId-page"}
              method="get"
              action={Unparsed(goButtonFormPath)}
              class="form-inline float-right justify-content-end"
              style="margin-bottom: 0px;">
          {hiddenFormFields}
          <label>{totalPages} Pages. Jump to</label>
          <input type="text"
                 name={pageNumberFormField}
                 id={s"form-$navigationId-page-no"}
                 value={page.toString}
                 class="col-1 form-control" />

          <label>. Show </label>
          <input type="text"
                 id={s"form-$navigationId-page-size"}
                 name={pageSizeFormField}
                 value={pageSize.toString}
                 class="col-1 form-control" />
          <label>items in a page.</label>

          <button type="submit" class="btn btn-spark">Go</button>
        </form>
      </div>
      <div>
        <span style="float: left; padding-top: 4px; padding-right: 4px;">Page: </span>
        <ul class="pagination">
          {if (currentGroup > firstGroup) {
          <li class="page-item">
            <a href={Unparsed(pageLink(startPage - groupSize))} class="page-link"
               aria-label="Previous Group">
              <span aria-hidden="true">
                &lt;&lt;
              </span>
            </a>
          </li>
          }}
          {if (page > 1) {
          <li class="page-item">
          <a href={Unparsed(pageLink(page - 1))} class="page-link" aria-label="Previous">
            <span aria-hidden="true">
              &lt;
            </span>
          </a>
          </li>
          }}
          {pageTags}
          {if (page < totalPages) {
          <li class="page-item">
            <a href={Unparsed(pageLink(page + 1))} class="page-link" aria-label="Next">
              <span aria-hidden="true">&gt;</span>
            </a>
          </li>
          }}
          {if (currentGroup < lastGroup) {
          <li class="page-item">
            <a href={Unparsed(pageLink(startPage + groupSize))} class="page-link"
               aria-label="Next Group">
              <span aria-hidden="true">
                &gt;&gt;
              </span>
            </a>
          </li>
        }}
        </ul>
      </div>
    </div>
  }

  /**
   * Return a link to jump to a page.
   */
  def pageLink(page: Int): String

  /**
   * Returns the submission path for the "go to page #" form.
   */
  def goButtonFormPath: String

  /**
   * Returns parameters of other tables in the page.
   */
  def getParameterOtherTable(request: HttpServletRequest, tableTag: String): String = {
    request.getParameterMap.asScala
      .filterNot(_._1.startsWith(tableTag))
      .map(parameter => parameter._1 + "=" + parameter._2(0))
      .mkString("&")
  }

  /**
   * Returns parameter of this table.
   */
  def getTableParameters(
      request: HttpServletRequest,
      tableTag: String,
      defaultSortColumn: String): (String, Boolean, Int) = {
    val parameterSortColumn = request.getParameter(s"$tableTag.sort")
    val parameterSortDesc = request.getParameter(s"$tableTag.desc")
    val parameterPageSize = request.getParameter(s"$tableTag.pageSize")
    val sortColumn = Option(parameterSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse(defaultSortColumn)
    val desc = Option(parameterSortDesc).map(_.toBoolean).getOrElse(
      sortColumn == defaultSortColumn
    )
    val pageSize = Option(parameterPageSize).map(_.toInt).getOrElse(100)

    (sortColumn, desc, pageSize)
  }

  /**
   * Check if given sort column is valid or not. If invalid then an exception is thrown.
   */
  def isSortColumnValid(
      headerInfo: Seq[(String, Boolean, Option[String])],
      sortColumn: String): Unit = {
    if (!headerInfo.filter(_._2).map(_._1).contains(sortColumn)) {
      throw new IllegalArgumentException(s"Unknown column: $sortColumn")
    }
  }

  def headerRow(
      headerInfo: Seq[(String, Boolean, Option[String])],
      desc: Boolean,
      pageSize: Int,
      sortColumn: String,
      parameterPath: String,
      tableTag: String,
      headerId: String): Seq[Node] = {
    val row: Seq[Node] = {
      headerInfo.map { case (header, sortable, tooltip) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$tableTag.desc=${!desc}" +
              s"&$tableTag.pageSize=$pageSize" +
              s"#$headerId")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          <th>
            <a href={headerLink}>
              <span data-toggle="tooltip" data-placement="top" title={tooltip.getOrElse("")}>
                {header}&nbsp;{Unparsed(arrow)}
              </span>
            </a>
          </th>
        } else {
          if (sortable) {
            val headerLink = Unparsed(
              parameterPath +
                s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
                s"&$tableTag.pageSize=$pageSize" +
                s"#$headerId")

            <th>
              <a href={headerLink}>
                <span data-toggle="tooltip" data-placement="top" title={tooltip.getOrElse("")}>
                  {header}
                </span>
              </a>
            </th>
          } else {
            <th>
              <span data-toggle="tooltip" data-placement="top" title={tooltip.getOrElse("")}>
                {header}
              </span>
            </th>
          }
        }
      }
    }
    <thead>
      <tr>{row}</tr>
    </thead>
  }
}
