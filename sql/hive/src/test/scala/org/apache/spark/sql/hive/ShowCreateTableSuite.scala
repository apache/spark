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

package org.apache.spark.sql.hive

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

class ShowCreateTableSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  test("data source table with user specified schema") {
    withTable("ddl_test1") {
      val jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile

      sql(
        s"""CREATE TABLE ddl_test1 (
           |  a STRING,
           |  b STRING,
           |  `extra col` ARRAY<INT>,
           |  `<another>` STRUCT<x: INT, y: ARRAY<BOOLEAN>>
           |)
           |USING json
           |OPTIONS (
           | PATH '$jsonFilePath'
           |)
         """.stripMargin
      )

      checkCreateTable("ddl_test1")
    }
  }

  test("data source table CTAS") {
    withTable("ddl_test2") {
      sql(
        s"""CREATE TABLE ddl_test2
           |USING json
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )

      checkCreateTable("ddl_test2")
    }
  }

  test("partitioned data source table") {
    withTable("ddl_test3") {
      sql(
        s"""CREATE TABLE ddl_test3
           |USING json
           |PARTITIONED BY (b)
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )

      checkCreateTable("ddl_test3")
    }
  }

  test("bucketed data source table") {
    withTable("ddl_test3") {
      sql(
        s"""CREATE TABLE ddl_test3
           |USING json
           |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )

      checkCreateTable("ddl_test3")
    }
  }

  test("partitioned bucketed data source table") {
    withTable("ddl_test4") {
      sql(
        s"""CREATE TABLE ddl_test4
           |USING json
           |PARTITIONED BY (c)
           |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )

      checkCreateTable("ddl_test4")
    }
  }

  test("data source table using Dataset API") {
    withTable("ddl_test5") {
      spark
        .range(3)
        .select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd, 'id as 'e)
        .write
        .mode("overwrite")
        .partitionBy("a", "b")
        .bucketBy(2, "c", "d")
        .saveAsTable("ddl_test5")

      checkCreateTable(TableIdentifier("ddl_test5", Some("default")))
    }
  }

  private def checkCreateTable(table: String): Unit = {
    checkCreateTable(TableIdentifier(table, Some("default")))
  }

  private def checkCreateTable(table: TableIdentifier): Unit = {
    val db = table.database.getOrElse("default")
    val expected = spark.externalCatalog.getTable(db, table.table)
    val shownDDL = sql(s"SHOW CREATE TABLE ${table.quotedString}").head().getString(0)
    sql(s"DROP TABLE ${table.quotedString}")

    withTable(table.table) {
      sql(shownDDL)
      val actual = spark.externalCatalog.getTable(db, table.table)
      checkCatalogTables(expected, actual)
    }
  }

  private def checkCatalogTables(expected: CatalogTable, actual: CatalogTable): Unit = {
    def normalize(table: CatalogTable): CatalogTable = {
      val nondeterministicProps = Set(
        "CreateTime",
        "transient_lastDdlTime",
        "grantTime",
        "lastUpdateTime",
        "last_modified_by",
        "last_modified_time",
        "Owner:",
        "COLUMN_STATS_ACCURATE",
        // The following are hive specific schema parameters which we do not need to match exactly.
        "numFiles",
        "numRows",
        "rawDataSize",
        "totalSize",
        "totalNumberFiles",
        "maxFileSize",
        "minFileSize"
      )

      table.copy(
        createTime = 0L,
        lastAccessTime = 0L,
        properties = table.properties.filterKeys(!nondeterministicProps.contains(_)))
    }

    assert(normalize(expected) == normalize(actual))
  }
}
