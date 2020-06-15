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

import scala.xml.Node

import org.apache.spark.SparkFunSuite

class PagedDataSourceSuite extends SparkFunSuite {

  test("basic") {
    val dataSource1 = new SeqPagedDataSource[Int](1 to 5, pageSize = 2)
    assert(dataSource1.pageData(1) === PageData(3, (1 to 2)))

    val dataSource2 = new SeqPagedDataSource[Int](1 to 5, pageSize = 2)
    assert(dataSource2.pageData(2) === PageData(3, (3 to 4)))

    val dataSource3 = new SeqPagedDataSource[Int](1 to 5, pageSize = 2)
    assert(dataSource3.pageData(3) === PageData(3, Seq(5)))
    // If the page number is more than maximum page, fall back to the last page
    val dataSource4 = new SeqPagedDataSource[Int](1 to 5, pageSize = 2)
    assert(dataSource4.pageData(4) === PageData(3, Seq(5)))
    // If the page number is less than or equal to zero, fall back to the first page
    val dataSource5 = new SeqPagedDataSource[Int](1 to 5, pageSize = 2)
    assert(dataSource5.pageData(0) === PageData(3, 1 to 2))
  }
}

class PagedTableSuite extends SparkFunSuite {
  test("pageNavigation") {
    // Create a fake PagedTable to test pageNavigation
    val pagedTable = new PagedTable[Int] {
      override def tableId: String = ""

      override def tableCssClass: String = ""

      override def dataSource: PagedDataSource[Int] = null

      override def pageLink(page: Int): String = page.toString

      override def headers: Seq[Node] = Nil

      override def row(t: Int): Seq[Node] = Nil

      override def pageSizeFormField: String = "pageSize"

      override def pageNumberFormField: String = "page"

      override def goButtonFormPath: String = ""
    }

    assert((pagedTable.pageNavigation(1, 10, 1).head \\ "li").map(_.text.trim) === Seq("1"))
    assert(
      (pagedTable.pageNavigation(1, 10, 2).head \\ "li").map(_.text.trim) === Seq("1", "2", ">"))
    assert(
      (pagedTable.pageNavigation(2, 10, 2).head \\ "li").map(_.text.trim) === Seq("<", "1", "2"))

    assert((pagedTable.pageNavigation(1, 10, 100).head \\ "li").map(_.text.trim) ===
      (1 to 10).map(_.toString) ++ Seq(">", ">>"))
    assert((pagedTable.pageNavigation(2, 10, 100).head \\ "li").map(_.text.trim) ===
      Seq("<") ++ (1 to 10).map(_.toString) ++ Seq(">", ">>"))

    assert((pagedTable.pageNavigation(100, 10, 100).head \\ "li").map(_.text.trim) ===
      Seq("<<", "<") ++ (91 to 100).map(_.toString))
    assert((pagedTable.pageNavigation(99, 10, 100).head \\ "li").map(_.text.trim) ===
      Seq("<<", "<") ++ (91 to 100).map(_.toString) ++ Seq(">"))

    assert((pagedTable.pageNavigation(11, 10, 100).head \\ "li").map(_.text.trim) ===
      Seq("<<", "<") ++ (11 to 20).map(_.toString) ++ Seq(">", ">>"))
    assert((pagedTable.pageNavigation(93, 10, 97).head \\ "li").map(_.text.trim) ===
      Seq("<<", "<") ++ (91 to 97).map(_.toString) ++ Seq(">"))
  }

  test("pageNavigation with different id") {
    val pagedTable = new PagedTable[Int] {
      override def tableId: String = "testTable"

      override def tableCssClass: String = ""

      override def dataSource: PagedDataSource[Int] = null

      override def pageLink(page: Int): String = ""

      override def headers: Seq[Node] = Nil

      override def row(t: Int): Seq[Node] = Nil

      override def pageSizeFormField: String = ""

      override def pageNumberFormField: String = ""

      override def goButtonFormPath: String = ""
    }

    val defaultIdNavigation = pagedTable.pageNavigation(1, 10, 2).head \\ "form"
    assert(defaultIdNavigation \@ "id" === "form-testTable-page")

    val customIdNavigation = pagedTable.pageNavigation(1, 10, 2, "customIdTable").head \\ "form"
    assert(customIdNavigation \@ "id" === "form-customIdTable-page")
    assert(defaultIdNavigation !== customIdNavigation)
  }
}

private[spark] class SeqPagedDataSource[T](seq: Seq[T], pageSize: Int)
  extends PagedDataSource[T](pageSize) {

  override protected def dataSize: Int = seq.size

  override protected def sliceData(from: Int, to: Int): Seq[T] = seq.slice(from, to)
}
