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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `ALTER TABLE .. RENAME PARTITION` command that
 * check V1 table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog:
 *     `org.apache.spark.sql.execution.command.v1.AlterTableRenamePartitionSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.AlterTableRenamePartitionSuite`
 */
trait AlterTableRenamePartitionSuiteBase extends command.AlterTableRenamePartitionSuiteBase {
  test("with location") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      sql(s"ALTER TABLE $t ADD PARTITION (id = 2) LOCATION 'loc1'")
      sql(s"INSERT INTO $t PARTITION (id = 2) SELECT 'def'")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "2"))
      checkLocation(t, Map("id" -> "2"), "loc1")

      sql(s"ALTER TABLE $t PARTITION (id = 2) RENAME TO PARTITION (id = 3)")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "3"))
      // V1 catalogs rename the partition location of managed tables
      checkLocation(t, Map("id" -> "3"), "id=3")
      checkAnswer(sql(s"SELECT id, data FROM $t WHERE id = 3"), Row(3, "def"))
    }
  }

  test("SPARK-34060, SPARK-34071: update stats of cached table") {
    withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> "true") {
      withNamespaceAndTable("ns", "tbl") { t =>
        sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
        sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
        sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
        assert(!spark.catalog.isCached(t))
        sql(s"CACHE TABLE $t")
        assert(spark.catalog.isCached(t))
        QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(0, 0), Row(1, 1)))
        val tableSize = getTableSize(t)
        assert(tableSize > 0)

        sql(s"ALTER TABLE $t PARTITION (part=0) RENAME TO PARTITION (part=2)")
        assert(spark.catalog.isCached(t))
        assert(tableSize == getTableSize(t))
        QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(0, 2), Row(1, 1)))
      }
    }
  }

  test("SPARK-XXXXX: keep dependents cashed after table altering") {
    def cacheRelation(name: String): Unit = {
      assert(!spark.catalog.isCached(name))
      sql(s"CACHE TABLE $name")
      assert(spark.catalog.isCached(name))
    }
    def checkRelation(name: String, expected: Seq[Row]): Unit = {
      assert(spark.catalog.isCached(name))
      QueryTest.checkAnswer(sql(s"SELECT * FROM $name"), expected)
    }

    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
      cacheRelation(t)

      withView("v0") {
        sql(s"CREATE VIEW v0 AS SELECT * FROM $t")
        cacheRelation("v0")
        sql(s"ALTER TABLE $t PARTITION (part=0) RENAME TO PARTITION (part=2)")
        checkRelation("v0", Seq(Row(0, 2), Row(1, 1)))
      }

      withTempView("v1") {
        sql(s"CREATE TEMP VIEW v1 AS SELECT * FROM $t")
        cacheRelation("v1")
        sql(s"ALTER TABLE $t PARTITION (part=1) RENAME TO PARTITION (part=3)")
        checkRelation("v1", Seq(Row(0, 2), Row(1, 3)))
      }

      val v2 = s"${spark.sharedState.globalTempViewManager.database}.v2"
      withGlobalTempView(v2) {
        sql(s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t")
        cacheRelation(v2)
        sql(s"ALTER TABLE $t PARTITION (part=2) RENAME TO PARTITION (part=4)")
        checkRelation(v2, Seq(Row(0, 4), Row(1, 3)))
      }
    }
  }
}

/**
 * The class contains tests for the `ALTER TABLE .. RENAME PARTITION` command to check
 * V1 In-Memory table catalog.
 */
class AlterTableRenamePartitionSuite
  extends AlterTableRenamePartitionSuiteBase
  with CommandSuiteBase
