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
import org.apache.spark.sql.execution.command

trait ShowPartitionsSuiteBase extends command.ShowPartitionsSuiteBase {
  test("show everything in the default database") {
    val table = "dateTable"
    withTable(table) {
      createDateTable(table)
      runShowPartitionsSql(
        s"show partitions default.$table",
        Row("year=2015/month=1") ::
        Row("year=2015/month=2") ::
        Row("year=2016/month=2") ::
        Row("year=2016/month=3") :: Nil)
    }
  }

  // The test fails for V2 Table Catalogs with the exception:
  // org.apache.spark.sql.AnalysisException: CREATE VIEW is only supported with v1 tables.
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
        assert(errMsg.contains("'SHOW PARTITIONS' expects a table"))
      }
    }
  }

  test("show partitions of a temporary view") {
    val viewName = "test_view"
    withTempView(viewName) {
      spark.range(10).createTempView(viewName)
      val errMsg = intercept[AnalysisException] {
        sql(s"SHOW PARTITIONS $viewName")
      }.getMessage
      assert(errMsg.contains("'SHOW PARTITIONS' expects a table"))
    }
  }
}

class ShowPartitionsSuite extends ShowPartitionsSuiteBase with CommandSuiteBase {
  // The test is placed here because it fails with `USING HIVE`:
  // org.apache.spark.sql.AnalysisException:
  //   Hive data source can only be used with tables, you can't use it with CREATE TEMP VIEW USING
  test("issue exceptions on the temporary view") {
    val viewName = "test_view"
    withTempView(viewName) {
      sql(s"""
        |CREATE TEMPORARY VIEW $viewName (c1 INT, c2 STRING)
        |$defaultUsing""".stripMargin)
      val errMsg = intercept[AnalysisException] {
        sql(s"SHOW PARTITIONS $viewName")
      }.getMessage
      assert(errMsg.contains("'SHOW PARTITIONS' expects a table"))
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

  test("null and empty string as partition values") {
    import testImplicits._
    withTable("t") {
      val df = Seq((0, ""), (1, null)).toDF("a", "part")
      df.write
        .partitionBy("part")
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable("t")

      runShowPartitionsSql(
        "SHOW PARTITIONS t",
        Row("part=__HIVE_DEFAULT_PARTITION__") :: Nil)
      checkAnswer(spark.table("t"),
        Row(0, null) ::
        Row(1, null) :: Nil)
    }
  }
}
