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

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class HiveDDLSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext.implicits._

  // check if the directory for recording the data of the table exists.
  private def tableDirectoryExists(tableIdentifier: TableIdentifier): Boolean = {
    val expectedTablePath =
      hiveContext.sessionState.catalog.hiveDefaultTableFilePath(tableIdentifier)
    val filesystemPath = new Path(expectedTablePath)
    val fs = filesystemPath.getFileSystem(sparkContext.hadoopConfiguration)
    fs.exists(filesystemPath)
  }

  test("drop tables") {
    withTable("tab1") {
      val tabName = "tab1"

      assert(!tableDirectoryExists(TableIdentifier(tabName)))
      sql(s"CREATE TABLE $tabName(c1 int)")

      assert(tableDirectoryExists(TableIdentifier(tabName)))
      sql(s"DROP TABLE $tabName")

      assert(!tableDirectoryExists(TableIdentifier(tabName)))
      sql(s"DROP TABLE IF EXISTS $tabName")
      sql(s"DROP VIEW IF EXISTS $tabName")
    }
  }

  test("drop managed tables") {
    withTempDir { tmpDir =>
      val tabName = "tab1"
      withTable(tabName) {
        assert(tmpDir.listFiles.isEmpty)
        sql(
          s"""
             |create table $tabName
             |stored as parquet
             |location '$tmpDir'
             |as select 1, '3'
          """.stripMargin)

        val hiveTable =
          hiveContext.sessionState.catalog
            .getTableMetadata(TableIdentifier(tabName, Some("default")))
        // It is a managed table, although it uses external in SQL
        assert(hiveTable.tableType == CatalogTableType.MANAGED_TABLE)

        assert(tmpDir.listFiles.nonEmpty)
        sql(s"DROP TABLE $tabName")
        // The data are deleted since the table type is not EXTERNAL
        assert(tmpDir.listFiles == null)
      }
    }
  }

  test("drop external data source table") {
    withTempDir { tmpDir =>
      val tabName = "tab1"
      withTable(tabName) {
        assert(tmpDir.listFiles.isEmpty)

        withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "true") {
          Seq(1 -> "a").toDF("i", "j")
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .option("path", tmpDir.toString)
            .saveAsTable(tabName)
        }

        val hiveTable =
          hiveContext.sessionState.catalog
            .getTableMetadata(TableIdentifier(tabName, Some("default")))
        // This data source table is external table
        assert(hiveTable.tableType == CatalogTableType.EXTERNAL_TABLE)

        assert(tmpDir.listFiles.nonEmpty)
        sql(s"DROP TABLE $tabName")
        // The data are not deleted since the table type is EXTERNAL
        assert(tmpDir.listFiles.nonEmpty)
      }
    }
  }

  test("drop views") {
    withTable("tab1") {
      val tabName = "tab1"
      sqlContext.range(10).write.saveAsTable("tab1")
      withView("view1") {
        val viewName = "view1"

        assert(tableDirectoryExists(TableIdentifier(tabName)))
        assert(!tableDirectoryExists(TableIdentifier(viewName)))
        sql(s"CREATE VIEW $viewName AS SELECT * FROM tab1")

        assert(tableDirectoryExists(TableIdentifier(tabName)))
        assert(!tableDirectoryExists(TableIdentifier(viewName)))
        sql(s"DROP VIEW $viewName")

        assert(tableDirectoryExists(TableIdentifier(tabName)))
        sql(s"DROP VIEW IF EXISTS $viewName")
      }
    }
  }

  test("drop table using drop view") {
    withTable("tab1") {
      sql("CREATE TABLE tab1(c1 int)")
      val message = intercept[AnalysisException] {
        sql("DROP VIEW tab1")
      }.getMessage
      assert(message.contains("Cannot drop a table with DROP VIEW. Please use DROP TABLE instead"))
    }
  }

  test("drop view using drop table") {
    withTable("tab1") {
      sqlContext.range(10).write.saveAsTable("tab1")
      withView("view1") {
        sql("CREATE VIEW view1 AS SELECT * FROM tab1")
        val message = intercept[AnalysisException] {
          sql("DROP TABLE view1")
        }.getMessage
        assert(message.contains("Cannot drop a view with DROP TABLE. Please use DROP VIEW instead"))
      }
    }
  }
}
