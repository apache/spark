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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.util.Utils

class SimpleShowCreateTableSuite extends ShowCreateTableSuite with SharedSQLContext

abstract class ShowCreateTableSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  test("data source table with user specified schema") {
    withTable("ddl_test") {
      val jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile

      sql(
        s"""CREATE TABLE ddl_test (
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

      checkCreateTable("ddl_test")
    }
  }

  test("data source table CTAS") {
    withTable("ddl_test") {
      sql(
        s"""CREATE TABLE ddl_test
           |USING json
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )

      checkCreateTable("ddl_test")
    }
  }

  test("partitioned data source table") {
    withTable("ddl_test") {
      sql(
        s"""CREATE TABLE ddl_test
           |USING json
           |PARTITIONED BY (b)
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )

      checkCreateTable("ddl_test")
    }
  }

  test("bucketed data source table") {
    withTable("ddl_test") {
      sql(
        s"""CREATE TABLE ddl_test
           |USING json
           |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )

      checkCreateTable("ddl_test")
    }
  }

  test("partitioned bucketed data source table") {
    withTable("ddl_test") {
      sql(
        s"""CREATE TABLE ddl_test
           |USING json
           |PARTITIONED BY (c)
           |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )

      checkCreateTable("ddl_test")
    }
  }

  test("data source table with a comment") {
    withTable("ddl_test") {
      sql(
        s"""CREATE TABLE ddl_test
           |USING json
           |COMMENT 'This is a comment'
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )

      checkCreateTable("ddl_test")
    }
  }

  test("data source table with table properties") {
    withTable("ddl_test") {
      sql(
        s"""CREATE TABLE ddl_test
           |USING json
           |TBLPROPERTIES ('a' = '1')
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )

      checkCreateTable("ddl_test")
    }
  }

  test("data source table using Dataset API") {
    withTable("ddl_test") {
      spark
        .range(3)
        .select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd, 'id as 'e)
        .write
        .mode("overwrite")
        .partitionBy("a", "b")
        .bucketBy(2, "c", "d")
        .saveAsTable("ddl_test")

      checkCreateTable("ddl_test")
    }
  }

  test("view") {
    withView("v1") {
      sql("CREATE VIEW v1 AS SELECT 1 AS a")
      checkCreateView("v1")
    }
  }

  test("view with output columns") {
    withView("v1") {
      sql("CREATE VIEW v1 (b) AS SELECT 1 AS a")
      checkCreateView("v1")
    }
  }

  test("SPARK-24911: keep quotes for nested fields") {
    withTable("t1") {
      val createTable = "CREATE TABLE `t1` (`a` STRUCT<`b`: STRING>)"
      sql(s"$createTable USING json")
      val shownDDL = sql(s"SHOW CREATE TABLE t1")
        .head()
        .getString(0)
        .split("\n")
        .head
      assert(shownDDL == createTable)

      checkCreateTable("t1")
    }
  }

  protected def checkCreateTable(table: String): Unit = {
    checkCreateTableOrView(TableIdentifier(table, Some("default")), "TABLE")
  }

  protected def checkCreateView(table: String): Unit = {
    checkCreateTableOrView(TableIdentifier(table, Some("default")), "VIEW")
  }

  private def checkCreateTableOrView(table: TableIdentifier, checkType: String): Unit = {
    val db = table.database.getOrElse("default")
    val expected = spark.sharedState.externalCatalog.getTable(db, table.table)
    val shownDDL = sql(s"SHOW CREATE TABLE ${table.quotedString}").head().getString(0)
    sql(s"DROP $checkType ${table.quotedString}")

    try {
      sql(shownDDL)
      val actual = spark.sharedState.externalCatalog.getTable(db, table.table)
      checkCatalogTables(expected, actual)
    } finally {
      sql(s"DROP $checkType IF EXISTS ${table.table}")
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
        // The following are hive specific schema parameters which we do not need to match exactly.
        "totalNumberFiles",
        "maxFileSize",
        "minFileSize"
      )

      table.copy(
        createTime = 0L,
        lastAccessTime = 0L,
        properties = table.properties.filterKeys(!nondeterministicProps.contains(_)),
        stats = None,
        ignoredProperties = Map.empty
      )
    }
    assert(normalize(actual) == normalize(expected))
  }
}
