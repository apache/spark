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
import org.apache.spark.storage.StorageLevel

/**
 * This base suite contains unified tests for the `ALTER TABLE .. RENAME` command that check V1
 * and V2 table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.AlterTableRenameSuite`
 *   - V1 table catalog tests: `org.apache.spark.sql.execution.command.v1.AlterTableRenameSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.AlterTableRenameSuite`
 *     - V1 Hive External catalog:
 *       `org.apache.spark.sql.hive.execution.command.AlterTableRenameSuite`
 */
trait AlterTableRenameSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE .. RENAME"

  test("rename a table in a database/namespace") {
    withNamespaceAndTable("ns", "dst_tbl") { dst =>
      val src = dst.replace("dst", "src")
      sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
      sql(s"INSERT INTO $src SELECT 0")

      sql(s"ALTER TABLE $src RENAME TO ns.dst_tbl")
      checkTables("ns", "dst_tbl")
      QueryTest.checkAnswer(sql(s"SELECT c0 FROM $dst"), Seq(Row(0)))
    }
  }

  test("table to rename does not exist") {
    val e = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalog.dbx.does_not_exist RENAME TO dbx.tab2")
    }
    checkErrorTableNotFound(e, s"`$catalog`.`dbx`.`does_not_exist`",
      ExpectedContext(s"$catalog.dbx.does_not_exist", 12,
        11 + s"$catalog.dbx.does_not_exist".length))
  }

  test("omit namespace in the destination table") {
    withNamespaceAndTable("ns", "dst_tbl") { dst =>
      val src = dst.replace("dst", "src")
      sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
      sql(s"INSERT INTO $src SELECT 0")

      sql(s"ALTER TABLE $src RENAME TO dst_tbl")
      checkTables("ns", "dst_tbl")
      QueryTest.checkAnswer(sql(s"SELECT c0 FROM $dst"), Seq(Row(0)))
    }
  }

  test("SPARK-33786: Cache's storage level should be respected when a table name is altered") {
    withNamespaceAndTable("ns", "dst_tbl") { dst =>
      val src = dst.replace("dst", "src")
      def getStorageLevel(tableName: String): StorageLevel = {
        val table = spark.table(tableName)
        val cachedData = spark.sharedState.cacheManager.lookupCachedData(table).get
        cachedData.cachedRepresentation.cacheBuilder.storageLevel
      }
      sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
      sql(s"INSERT INTO $src SELECT 0")
      sql(s"CACHE TABLE $src OPTIONS('storageLevel' 'MEMORY_ONLY')")
      val oldStorageLevel = getStorageLevel(src)

      sql(s"ALTER TABLE $src RENAME TO ns.dst_tbl")
      QueryTest.checkAnswer(sql(s"SELECT c0 FROM $dst"), Seq(Row(0)))
      val newStorageLevel = getStorageLevel(dst)
      assert(oldStorageLevel === newStorageLevel)
    }
  }

  test("rename cached table") {
    withNamespaceAndTable("ns", "students") { students =>
      sql(s"CREATE TABLE $students (age INT, name STRING) $defaultUsing")
      sql(s"INSERT INTO $students SELECT 19, 'Ana'")

      spark.catalog.cacheTable(students)
      val expected = Seq(Row(19, "Ana"))
      QueryTest.checkAnswer(spark.table(students), expected)
      assert(spark.catalog.isCached(students),
        "bad test: table was not cached in the first place")
      val teachers = s"$catalog.ns.teachers"
      withTable(teachers) {
        // After the command below we have both students and teachers.
        sql(s"ALTER TABLE $students RENAME TO ns.teachers")
        // The cached data for the old students table should not be read by
        // the new students table.
        sql(s"CREATE TABLE $students (age INT, name STRING) $defaultUsing")
        assert(!spark.catalog.isCached(students))
        assert(spark.catalog.isCached(teachers))
        assert(spark.table(students).collect().isEmpty)
        QueryTest.checkAnswer(spark.table(teachers), expected)
      }
    }
  }

  test("rename without explicitly specifying database") {
    try {
      withNamespaceAndTable("ns", "dst_tbl") { dst =>
        val src = dst.replace("dst", "src")
        sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
        sql(s"INSERT INTO $src SELECT 0")

        sql(s"USE $catalog.ns")
        sql(s"ALTER TABLE src_tbl RENAME TO dst_tbl")
        checkTables("ns", "dst_tbl")
        checkAnswer(sql(s"SELECT c0 FROM $dst"), Seq(Row(0)))
      }
    } finally {
      spark.sessionState.catalogManager.reset()
    }
  }

  test("SPARK-37963: preserve partition info") {
    withNamespaceAndTable("ns", "dst_tbl") { dst =>
      val src = dst.replace("dst", "src")
      sql(s"CREATE TABLE $src (i int, j int) $defaultUsing partitioned by (j)")
      sql(s"insert into table $src partition(j=2) values (1)")
      sql(s"ALTER TABLE $src RENAME TO ns.dst_tbl")
      checkAnswer(spark.table(dst), Row(1, 2))
    }
  }

  test("SPARK-38587: use formatted names") {
    withNamespaceAndTable("CaseUpperCaseLower", "CaseUpperCaseLower") { t =>
      sql(s"CREATE TABLE ${t}_Old (i int) $defaultUsing")
      sql(s"ALTER TABLE ${t}_Old RENAME TO CaseUpperCaseLower.CaseUpperCaseLower")
      assert(spark.table(t).isEmpty)
    }
  }
}
