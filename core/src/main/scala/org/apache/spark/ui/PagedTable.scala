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

private[ui] abstract class PagedDataSource[T](page: Int, pageSize: Int) {

  protected val data: Seq[T]

  def pageData: PageData[T] = {
    val dataSize = data.size
    val totalPages = (dataSize + pageSize - 1) / pageSize
    require(page > 0, "page must be positive")
    require(page <= totalPages, s"page must not exceed $totalPages")
    val from = (page - 1) * pageSize
    val to = dataSize.min(page * pageSize)
    PageData(page, totalPages, data.slice(from, to))
  }

}

private[ui] case class PageData[T](page: Int, totalPage: Int, data: Seq[T])

private[ui] trait PagedTable[T] {

  def tableId: String

  def tableCssClass: String

  def dataSource: PagedDataSource[T]

  def headers: Seq[Node]

  def row(t: T): Seq[Node]

  def table: Seq[Node] = {
    val PageData(page, totalPages, data) = dataSource.pageData
    <div>
      {pageNavigation(page, totalPages)}
      <table class={tableCssClass} id={tableId}>
        {headers}
        <tbody>
          {data.map(row)}
        </tbody>
      </table>
    </div>
  }

  private[ui] def pageNavigation(page: Int, totalPages: Int): Seq[Node] = {
    if (totalPages == 1) {
      Nil
    } else {
      val groupSize = 10
      val firstGroup = 0
      val lastGroup = (totalPages - 1) / groupSize
      val currentGroup = (page - 1) / groupSize
      val startPage = currentGroup * groupSize + 1
      val endPage = totalPages.min(startPage + groupSize - 1)
      val pageTags = (startPage to endPage).map { p =>
        if (p == page) {
          <li class="disabled"><a href="#">{p}</a></li>
        } else {
          <li class="active"><a href={pageLink(p)}>{p}</a></li>
        }
      }
      val (goButtonJsFuncName, goButtonJsFunc) = goButtonJavascriptFunction
      val formJs =
        s"""$$(function(){
          |  $$( "#form-task-page" ).submit(function(event) {
          |    var page = $$("#form-task-page-no").val()
          |    if (page != "") {
          |      ${goButtonJsFuncName}(page);
          |    }
          |    event.preventDefault();
          |  });
          |});
        """.stripMargin

      <div>
          <div>
            <form id="form-task-page" class="form-horizontal  pull-right">
              <div class="control-group">
                <label class="control-label">{totalPages} Pages. Jump to</label>
                <div class="controls">
                  <input type="text" id="form-task-page-no" value={page.toString} class="span1" />
                  <button type="submit" class="btn">Go</button>
                </div>
              </div>
            </form>
          </div>
          <div class="pagination">
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

  def pageLink(page: Int): String

  def goButtonJavascriptFunction: (String, String)
}
