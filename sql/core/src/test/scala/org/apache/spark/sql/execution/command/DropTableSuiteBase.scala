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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}

/**
 * This base suite contains unified tests for the `DROP TABLE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.DropTableSuite`
 *   - V1 table catalog tests: `org.apache.spark.sql.execution.command.v1.DropTableSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.DropTableSuite`
 *     - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.DropTableSuite`
 */
trait DropTableSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "DROP TABLE"

  protected def createTable(tableName: String): Unit = {
    sql(s"CREATE TABLE $tableName (c int) $defaultUsing")
    sql(s"INSERT INTO $tableName SELECT 0")
  }

  test("basic") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")

      createTable(s"$catalog.ns.tbl")
      checkTables("ns", "tbl")

      sql(s"DROP TABLE $catalog.ns.tbl")
      checkTables("ns") // no tables
    }
  }

  test("try to drop a nonexistent table") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      checkTables("ns") // no tables

      val e = intercept[AnalysisException] {
        sql(s"DROP TABLE $catalog.ns.tbl")
      }
      checkErrorTableNotFound(e, s"`$catalog`.`ns`.`tbl`")
    }
  }

  test("with IF EXISTS") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")

      createTable(s"$catalog.ns.tbl")
      checkTables("ns", "tbl")
      sql(s"DROP TABLE IF EXISTS $catalog.ns.tbl")
      checkTables("ns")

      // It must not throw any exceptions
      sql(s"DROP TABLE IF EXISTS $catalog.ns.tbl")
      checkTables("ns")
    }
  }

  test("SPARK-33174: DROP TABLE should resolve to a temporary view first") {
    withNamespaceAndTable("ns", "t") { t =>
      withTempView("t") {
        sql(s"CREATE TABLE $t (id bigint) $defaultUsing")
        sql("CREATE TEMPORARY VIEW t AS SELECT 2")
        sql(s"USE $catalog.ns")
        try {
          // Check the temporary view 't' exists.
          checkAnswer(
            sql("SHOW TABLES FROM spark_catalog.default LIKE 't'")
              .select("tableName", "isTemporary"),
            Row("t", true))
          sql("DROP TABLE t")
          // Verify that the temporary view 't' is resolved first and dropped.
          checkAnswer(
            sql("SHOW TABLES FROM spark_catalog.default LIKE 't'")
              .select("tableName", "isTemporary"),
            Seq.empty)
        } finally {
          sql(s"USE spark_catalog")
        }
      }
    }
  }

  test("SPARK-33305: DROP TABLE should also invalidate cache") {
    val t = s"$catalog.ns.tbl"
    val view = "view"
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      withTempView(view, "source") {
        val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
        df.createOrReplaceTempView("source")
        sql(s"CREATE TABLE $t $defaultUsing AS SELECT id, data FROM source")
        sql(s"CACHE TABLE $view AS SELECT id FROM $t")
        checkAnswer(sql(s"SELECT * FROM $t"), spark.table("source").collect())
        checkAnswer(
          sql(s"SELECT * FROM $view"),
          spark.table("source").select("id").collect())

        val oldTable = spark.table(view)
        assert(spark.sharedState.cacheManager.lookupCachedData(oldTable).isDefined)
        sql(s"DROP TABLE $t")
        assert(spark.sharedState.cacheManager.lookupCachedData(oldTable).isEmpty)
      }
    }
  }
}
