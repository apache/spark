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

package org.apache.spark.sql.connect

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.DSv2RepeatedTableAccessTests
import org.apache.spark.sql.connector.catalog.{CachingInMemoryTableCatalog, Column, Identifier, InMemoryTableCatalog, TableCatalog, TableChange, TableInfo}
import org.apache.spark.sql.types.IntegerType

/**
 * Connect-mode runner for [[DSv2RepeatedTableAccessTests]]. All shared test logic lives in the
 * trait; this class provides the Connect-specific session, catalog access, and result comparison.
 *
 * The "DataFrame reuse" tests at the bottom are Connect-specific: reusing the same DataFrame
 * object across external mutations still sees fresh data, because Connect re-sends the plan
 * to the server for fresh analysis on every action. In classic Spark, the resolved plan is
 * captured at DataFrame creation time, so reusing a DF after schema changes would fail.
 */
class DataSourceV2RepeatedSQLConnectSuite
    extends SparkConnectServerTest
    with DSv2RepeatedTableAccessTests {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")
    .set("spark.sql.catalog.cachingcat", classOf[CachingInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.cachingcat.copyOnLoad", "true")

  override protected def testPrefix: String = "[connect] "

  override protected def withTestSession(fn: SparkSession => Unit): Unit =
    withSession(fn)

  override protected def checkRows(df: => DataFrame, expected: Seq[Row]): Unit =
    QueryTest.sameRows(expected, df.collect().toSeq).foreach(msg => fail(msg))

  override protected def getTableCatalog[C <: TableCatalog: ClassTag](
      session: SparkSession,
      catalogName: String): C = {
    val serverSession = getServerSession(session)
    val catalog = serverSession.sessionState.catalogManager.catalog(catalogName)
    val ct = implicitly[ClassTag[C]]
    require(
      ct.runtimeClass.isInstance(catalog),
      s"Expected ${ct.runtimeClass.getName} but got ${catalog.getClass.getName}")
    catalog.asInstanceOf[C]
  }

  override protected def withTestTableAndViews(
      session: SparkSession,
      table: String,
      views: Seq[String] = Seq.empty)(fn: => Unit): Unit = {
    try { fn }
    finally {
      views.foreach(v => session.sql(s"DROP VIEW IF EXISTS $v").collect())
      session.sql(s"DROP TABLE IF EXISTS $table").collect()
    }
  }

  // Connect-specific DataFrame reuse tests: reusing the same DataFrame object across
  // external mutations still sees fresh data, because Connect re-sends the plan to the
  // server for fresh analysis on every action.

  private val T = "testcat.ns1.ns2.tbl"
  private val testIdent = Identifier.of(Array("ns1", "ns2"), "tbl")

  test("[connect] reused DataFrame reflects external write") {
    withTestSession { session =>
      withTestTableAndViews(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

        val df = session.sql(s"SELECT * FROM $T")
        checkRows(df, Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        // same df object, Connect re-analyzes and sees the new row
        checkRows(df, Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[connect] reused DataFrame reflects external schema change") {
    withTestSession { session =>
      withTestTableAndViews(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

        val df = session.sql(s"SELECT * FROM $T")
        checkRows(df, Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val addCol = TableChange.addColumn(Array("new_col"), IntegerType, true)
        catalog.alterTable(testIdent, addCol)

        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        // same df object, Connect re-analyzes and sees the new schema
        checkRows(df, Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[connect] reused DataFrame reflects external drop/recreate") {
    withTestSession { session =>
      withTestTableAndViews(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

        val df = session.sql(s"SELECT * FROM $T")
        checkRows(df, Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        catalog.dropTable(testIdent)
        catalog.createTable(
          testIdent,
          new TableInfo.Builder()
            .withColumns(Array(
              Column.create("id", IntegerType),
              Column.create("salary", IntegerType)))
            .build())

        // same df object, Connect re-analyzes against the new empty table
        checkRows(df, Seq.empty)
      }
    }
  }
}
