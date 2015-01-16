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

import org.apache.spark.sql.catalyst.analysis.{EmptyCatalog, OverrideCatalog, SimpleCatalog}
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class ShowTablesSuite extends FunSuite with Matchers with BeforeAndAfter {

  val simpleCatalog = new SimpleCatalog(true)

  val expectedTablesOne = List("org.apache.spark.sql.ListTablesSuite.foo",
    "org.apache.spark.sql.ListTablesSuite.bar",
    "org.apache.spark.sql.ListTablesSuite.baz")

  val expectedTablesTwo = List("org.apache.spark.sql.ListTablesSuite.Larry",
    "org.apache.spark.sql.ListTablesSuite.Moe",
    "org.apache.spark.sql.ListTablesSuite.Curly")

  val overrideCatalog = new SimpleCatalog(true) with OverrideCatalog

  val overrideCaseInsensitiveCatalog = new SimpleCatalog(false) with OverrideCatalog


  before {
    expectedTablesOne.map(t => simpleCatalog.registerTable(Seq(t), null))
    expectedTablesOne.map(t => overrideCatalog.registerTable(Seq("A", t), null))
    expectedTablesTwo.map(t => overrideCatalog.registerTable(Seq("B", t), null))
    expectedTablesOne.map(t => overrideCaseInsensitiveCatalog.registerTable(Seq("x", t), null))
    expectedTablesTwo.map(t => overrideCaseInsensitiveCatalog.registerTable(Seq("y", t), null))

    expectedTablesOne.map(t =>
      TestSQLContext.registerRDDAsTable(new SchemaRDD(TestSQLContext,null),t))
  }

  after {
    simpleCatalog.unregisterAllTables()
    overrideCatalog.unregisterAllTables()
  }

  test("SPARK-3299 showTables (foo, bar, baz) from SimpleCatalog") {
    val returnedTables = simpleCatalog.showTables(None)
    returnedTables should contain theSameElementsAs expectedTablesOne
  }

  test("SPARK-3299 showTables works correctly when no databases defined for tables"){
      val newTables = List("tableX", "tableY", "tableZ")
      newTables.map(t => overrideCatalog.registerTable(Seq(t), null))

      val returnedTables = overrideCatalog.showTables(Some("A"))
      returnedTables should contain theSameElementsAs expectedTablesOne
  }

  test("SPARK-3299 showTables correct with database names from OverrideCatalog") {
    var returnedTables = overrideCatalog.showTables(Some("A"))
    returnedTables should contain theSameElementsAs expectedTablesOne

    returnedTables = overrideCatalog.showTables(Some("B"))
    returnedTables should contain theSameElementsAs expectedTablesTwo
  }

  test("SPARK-3299 showTables with database names and case insensitivity from OverrideCatalog") {
    var returnedTables = overrideCaseInsensitiveCatalog.showTables(Some("X"))
    returnedTables should contain theSameElementsAs expectedTablesOne.map(_.toLowerCase)

    returnedTables = overrideCaseInsensitiveCatalog.showTables(Some("Y"))
    returnedTables should contain theSameElementsAs expectedTablesTwo.map(_.toLowerCase)
  }

  test("SPARK-3299 showTables contains all tables no database specified from OverrideCatalog") {
    val returnedTables = overrideCatalog.showTables(None)
    returnedTables should contain theSameElementsAs expectedTablesOne ++ expectedTablesTwo
  }

  test("SPARK-3329 showTables contains all tables for SQLContext") {
    val returnedTables = TestSQLContext.showTables()
    returnedTables should contain allOf("org.apache.spark.sql.ListTablesSuite.foo",
      "org.apache.spark.sql.ListTablesSuite.bar",
      "org.apache.spark.sql.ListTablesSuite.baz")
  }

  test("SPARK-3299 EmptyCatalog throws UnsupportedOperationException when showTables is called") {
    intercept[UnsupportedOperationException] {
      EmptyCatalog.showTables(None)
    }
  }
}
