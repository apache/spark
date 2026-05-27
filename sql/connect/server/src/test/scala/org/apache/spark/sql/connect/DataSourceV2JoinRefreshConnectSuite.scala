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
import org.apache.spark.sql.connector.DSv2JoinRefreshTests
import org.apache.spark.sql.connector.catalog.{InMemoryTableCatalog, NullTableIdAndNullColumnIdInMemoryTableCatalog, NullTableIdInMemoryTableCatalog, TableCatalog}

/**
 * Connect-mode runner for [[DSv2JoinRefreshTests]]. All test logic lives in the shared trait;
 * this class only provides the Connect-specific session, catalog access, and result comparison.
 */
class DataSourceV2JoinRefreshConnectSuite
    extends SparkConnectServerTest
    with DSv2JoinRefreshTests {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")
    .set("spark.sql.catalog.nullidcat", classOf[NullTableIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullidcat.copyOnLoad", "true")
    .set(
      "spark.sql.catalog.nullbothidscat",
      classOf[NullTableIdAndNullColumnIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullbothidscat.copyOnLoad", "true")

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
}
