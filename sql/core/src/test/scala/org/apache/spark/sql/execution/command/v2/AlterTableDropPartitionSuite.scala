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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `ALTER TABLE .. DROP PARTITION` command
 * to check V2 table catalogs.
 */
class AlterTableDropPartitionSuite
  extends command.AlterTableDropPartitionSuiteBase
  with CommandSuiteBase {
  override protected val notFullPartitionSpecErr = "Partition spec is invalid"
  override protected def nullPartitionValue: String = "null"

  test("SPARK-33650: drop partition into a table which doesn't support partition management") {
    withNamespaceAndTable("ns", "tbl", s"non_part_$catalog") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP PARTITION (id=1)")
      }.getMessage
      assert(errMsg.contains("can not alter partitions"))
    }
  }

  test("purge partition data") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $t ADD PARTITION (id=1)")
      try {
        val errMsg = intercept[UnsupportedOperationException] {
          sql(s"ALTER TABLE $t DROP PARTITION (id=1) PURGE")
        }.getMessage
        assert(errMsg.contains("purge is not supported"))
      } finally {
        sql(s"ALTER TABLE $t DROP PARTITION (id=1)")
      }
    }
  }

  test("empty string as partition value") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 INT, p1 STRING) $defaultUsing PARTITIONED BY (p1)")
      sql(s"ALTER TABLE $t ADD PARTITION (p1 = '')")
      sql(s"ALTER TABLE $t DROP PARTITION (p1 = '')")
      checkPartitions(t)
    }
  }

  test("SPARK-34099, SPARK-34161: keep dependents cached after table altering") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
      sql(s"INSERT INTO $t PARTITION (part=2) SELECT 2")
      sql(s"INSERT INTO $t PARTITION (part=3) SELECT 3")
      cacheRelation(t)
      checkCachedRelation(t, Seq(Row(0, 0), Row(1, 1), Row(2, 2), Row(3, 3)))

      withView("v0") {
        sql(s"CREATE VIEW v0 AS SELECT * FROM $t")
        cacheRelation("v0")
        sql(s"ALTER TABLE $t DROP PARTITION (part=1)")
        checkCachedRelation("v0", Seq(Row(0, 0), Row(2, 2), Row(3, 3)))
      }

      withTempView("v1") {
        sql(s"CREATE TEMP VIEW v1 AS SELECT * FROM $t")
        cacheRelation("v1")
        sql(s"ALTER TABLE $t DROP PARTITION (part=2)")
        checkCachedRelation("v1", Seq(Row(0, 0), Row(3, 3)))
      }

      val v2 = s"${spark.sharedState.globalTempViewManager.database}.v2"
      withGlobalTempView(v2) {
        sql(s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t")
        cacheRelation(v2)
        sql(s"ALTER TABLE $t DROP PARTITION (part=3)")
        checkCachedRelation(v2, Seq(Row(0, 0)))
      }
    }
  }
}
