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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.test.SharedSparkSession

trait ShowPartitionsSuiteBase extends command.ShowPartitionsSuiteBase {
  override def version: String = "V1"
  override def catalog: String = CatalogManager.SESSION_CATALOG_NAME
  override def defaultNamespace: Seq[String] = Seq("default")
  override def defaultUsing: String = "USING parquet"

  protected def createDateTable(table: String): Unit = {
    sql(s"""
      |CREATE TABLE $table (price int, qty int, year int, month int)
      |$defaultUsing
      |partitioned by (year, month)""".stripMargin)
    sql(s"INSERT INTO $table PARTITION(year = 2015, month = 1) SELECT 1, 1")
    sql(s"INSERT INTO $table PARTITION(year = 2015, month = 2) SELECT 2, 2")
    sql(s"INSERT INTO $table PARTITION(year = 2016, month = 2) SELECT 3, 3")
    sql(s"INSERT INTO $table PARTITION(year = 2016, month = 3) SELECT 3, 3")
  }

  test("show everything") {
    val table = "dateTable"
    withTable(table) {
      createDateTable(table)
      checkAnswer(
        sql(s"show partitions $table"),
        Row("year=2015/month=1") ::
          Row("year=2015/month=2") ::
          Row("year=2016/month=2") ::
          Row("year=2016/month=3") :: Nil)

      checkAnswer(
        sql(s"show partitions default.$table"),
        Row("year=2015/month=1") ::
          Row("year=2015/month=2") ::
          Row("year=2016/month=2") ::
          Row("year=2016/month=3") :: Nil)
    }
  }

  test("issue exceptions on the temporary view") {
    val viewName = "test_view"
    withTempView(viewName) {
      spark.range(10).createTempView(viewName)
      val errMsg = intercept[NoSuchTableException] {
        sql(s"SHOW PARTITIONS $viewName")
      }.getMessage
      assert(errMsg.contains(s"Table or view '$viewName' not found"))
    }
  }
}

class ShowPartitionsSuite extends ShowPartitionsSuiteBase with SharedSparkSession
