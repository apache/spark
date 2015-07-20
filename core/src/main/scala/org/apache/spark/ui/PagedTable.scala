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

import scala.xml.{Node, Unparsed}

/**
 * A data source that provides data for a page.
 *
 * @param pageSize the number of rows in a page
 */
private[ui] abstract class PagedDataSource[T](val pageSize: Int) {

  if (pageSize <= 0) {
    throw new IllegalArgumentException("Page size must be positive")
  }

  /**
   * Return the size of all data.
   */
  protected def dataSize: Int

  /**
   * Slice a range of data.
   */
  protected def sliceData(from: Int, to: Int): Seq[T]

  /**
   * Slice the data for this page
   */
  def pageData(page: Int): PageData[T] = {
    val totalPages = (dataSize + pageSize - 1) / pageSize
    if (page <= 0 || page > totalPages) {
      throw new IndexOutOfBoundsException(
        s"Page $page is out of range. Please select a page number between 1 and $totalPages.")
    }
    val from = (page - 1) * pageSize
    val to = dataSize.min(page * pageSize)
    PageData(totalPages, sliceData(from, to))
  }

}

/**
 * The data returned by `PagedDataSource.pageData`, including the page number, the number of total
 * pages and the data in this page.
 */
private[ui] case class PageData[T](totalPage: Int, data: Seq[T])

/**
 * A paged table that will generate a HTML table for a specified page and also the page navigation.
 */
private[ui] trait PagedTable[T] {

  def tableId: String

  def tableCssClass: String

  def dataSource: PagedDataSource[T]

  def headers: Seq[Node]

  def row(t: T): Seq[Node]

  def table(page: Int): Seq[Node] = {
    val _dataSource = dataSource
    try {
      val PageData(totalPages, data) = _dataSource.pageData(page)
      <div>
        {pageNavigation(page, _dataSource.pageSize, totalPages)}
        <table class={tableCssClass} id={tableId}>
          {headers}
          <tbody>
            {data.map(row)}
          </tbody>
        </table>
      </div>
    } catch {
      case e: IndexOutOfBoundsException =>
        val PageData(totalPages, _) = _dataSource.pageData(1)
        <div>
          {pageNavigation(1, _dataSource.pageSize, totalPages)}
          <div class="alert alert-error">{e.getMessage}</div>
        </div>
    }
  }

  /**
   * Return a page navigation.
   * <ul>
   *   <li>If the totalPages is 1, the page navigation will be empty</li>
   *   <li>
   *     If the totalPages is more than 1, it will create a page navigation including a group of
   *     page numbers and a form to submit the page number.
   *   </li>
   * </ul>
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
  private[ui] def pageNavigation(page: Int, pageSize: Int, totalPages: Int): Seq[Node] = {
    if (totalPages == 1) {
      Nil
    } else {
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
          <li class="disabled"><a href="#">{p}</a></li>
        } else {
          <li><a href={pageLink(p)}>{p}</a></li>
        }
      }
      val (goButtonJsFuncName, goButtonJsFunc) = goButtonJavascriptFunction
      // When clicking the "Go" button, it will call this javascript method and then call
      // "goButtonJsFuncName"
      val formJs =
        s"""$$(function(){
          |  $$( "#form-task-page" ).submit(function(event) {
          |    var page = $$("#form-task-page-no").val()
          |    var pageSize = $$("#form-task-page-size").val()
          |    pageSize = pageSize ? pageSize: 100;
          |    if (page != "") {
          |      ${goButtonJsFuncName}(page, pageSize);
          |    }
          |    event.preventDefault();
          |  });
          |});
        """.stripMargin

      <div>
        <div>
          <form id="form-task-page" class="form-inline pull-right" style="margin-bottom: 0px;">
            <label>{totalPages} Pages. Jump to</label>
            <input type="text" id="form-task-page-no" value={page.toString} class="span1" />
            <label>. Show </label>
            <input type="text" id="form-task-page-size" value={pageSize.toString} class="span1" />
            <label>tasks in a page.</label>
            <button type="submit" class="btn">Go</button>
          </form>
        </div>
        <div class="pagination" style="margin-bottom: 0px;">
          <span style="float: left; padding-top: 4px; padding-right: 4px;">Page: </span>
          <ul>
            {if (currentGroup > firstGroup) {
            <li>
              <a href={pageLink(startPage - groupSize)} aria-label="Previous Group">
                <span aria-hidden="true">
                  &lt;&lt;
                </span>
              </a>
            </li>
            }}
            {if (page > 1) {
            <li>
            <a href={pageLink(page - 1)} aria-label="Previous">
              <span aria-hidden="true">
                &lt;
              </span>
            </a>
            </li>
            }}
            {pageTags}
            {if (page < totalPages) {
            <li>
              <a href={pageLink(page + 1)} aria-label="Next">
                <span aria-hidden="true">&gt;</span>
              </a>
            </li>
            }}
            {if (currentGroup < lastGroup) {
            <li>
              <a href={pageLink(startPage + groupSize)} aria-label="Next Group">
                <span aria-hidden="true">
                  &gt;&gt;
                </span>
              </a>
            </li>
          }}
          </ul>
        </div>
        <script>
          {Unparsed(goButtonJsFunc)}

          {Unparsed(formJs)}
        </script>
      </div>
    }
  }

  /**
   * Return a link to jump to a page.
   */
  def pageLink(page: Int): String

  /**
   * Only the implementation knows how to create the url with a page number and the page size, so we
   * leave this one to the implementation. The implementation should create a JavaScript method that
   * accepts a page number along with the page size and jumps to the page. The return value is this
   * method name and its JavaScript codes.
   */
  def goButtonJavascriptFunction: (String, String)
}
