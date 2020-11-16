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

import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.test.SharedSparkSession

trait ShowPartitionsSuiteBase extends command.ShowPartitionsSuiteBase {
  override def version: String = "V1"
  override def catalog: String = CatalogManager.SESSION_CATALOG_NAME
  override def defaultNamespace: Seq[String] = Seq("default")
  override def defaultUsing: String = "USING parquet"

  private def createDateTable(table: String): Unit = {
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

  test("filter by partitions") {
    val table = "dateTable"
    withTable(table) {
      createDateTable(table)
      checkAnswer(
        sql(s"show partitions default.$table PARTITION(year=2015)"),
        Row("year=2015/month=1") ::
          Row("year=2015/month=2") :: Nil)
      checkAnswer(
        sql(s"show partitions default.$table PARTITION(year=2015, month=1)"),
        Row("year=2015/month=1") :: Nil)
      checkAnswer(
        sql(s"show partitions default.$table PARTITION(month=2)"),
        Row("year=2015/month=2") ::
          Row("year=2016/month=2") :: Nil)
    }
  }

  test("show everything more than 5 part keys") {
    val table = "wideTable"
    withTable(table) {
      sql(s"""
        |CREATE TABLE $table (
        |  price int, qty int,
        |  year int, month int, hour int, minute int, sec int, extra int)
        |$defaultUsing
        |PARTITIONED BY (year, month, hour, minute, sec, extra)""".stripMargin)
      sql(s"""
        |INSERT INTO $table
        |PARTITION(year = 2016, month = 3, hour = 10, minute = 10, sec = 10, extra = 1) SELECT 3, 3
      """.stripMargin)
      sql(s"""
        |INSERT INTO $table
        |PARTITION(year = 2016, month = 4, hour = 10, minute = 10, sec = 10, extra = 1) SELECT 3, 3
      """.stripMargin)
      checkAnswer(
        sql(s"show partitions $table"),
        Row("year=2016/month=3/hour=10/minute=10/sec=10/extra=1") ::
          Row("year=2016/month=4/hour=10/minute=10/sec=10/extra=1") :: Nil)
    }
  }

  test("non-partitioning columns") {
    val table = "dateTable"
    withTable(table) {
      createDateTable(table)
      val errMsg = intercept[AnalysisException] {
        sql(s"SHOW PARTITIONS $table PARTITION(abcd=2015, xyz=1)")
      }.getMessage
      assert(errMsg.contains("Non-partitioning column(s) [abcd, xyz] are specified"))
    }
  }

  test("show partitions of non-partitioned table") {
    val table = "not_partitioned_table"
    withTable(table) {
      sql(s"CREATE TABLE $table (col1 int) $defaultUsing")
      val errMsg = intercept[AnalysisException] {
        sql(s"SHOW PARTITIONS $table")
      }.getMessage
      assert(errMsg.contains("not allowed on a table that is not partitioned"))
    }
  }

  test("show partitions of a view") {
    val table = "dateTable"
    withTable(table) {
      createDateTable(table)
      val view = "view1"
      withView(view) {
        sql(s"CREATE VIEW $view as select * from $table")
        val errMsg = intercept[AnalysisException] {
          sql(s"SHOW PARTITIONS $view")
        }.getMessage
        assert(errMsg.contains("is not allowed on a view"))
      }
    }
  }

  test("show partitions of a temporary view") {
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

class ShowPartitionsSuite extends ShowPartitionsSuiteBase with SharedSparkSession {
  // The test is placed here because it fails with `USING HIVE`:
  // org.apache.spark.sql.AnalysisException:
  //   Hive data source can only be used with tables, you can't use it with CREATE TEMP VIEW USING
  test("issue exceptions on the temporary view") {
    val viewName = "test_view"
    withTempView(viewName) {
      sql(s"""
             |CREATE TEMPORARY VIEW $viewName (c1 INT, c2 STRING)
             |$defaultUsing""".stripMargin)
      val errMsg = intercept[NoSuchTableException] {
        sql(s"SHOW PARTITIONS $viewName")
      }.getMessage
      assert(errMsg.contains(s"Table or view '$viewName' not found"))
    }
  }

  test("show partitions from a datasource") {
    import testImplicits._
    withTable("part_datasrc") {
      val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")
      df.write
        .partitionBy("a")
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable("part_datasrc")

      assert(sql("SHOW PARTITIONS part_datasrc").count() == 3)
    }
  }
}
