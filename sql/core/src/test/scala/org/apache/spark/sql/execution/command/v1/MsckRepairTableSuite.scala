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

import java.io.File

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `MSCK REPAIR TABLE` command that
 * check V1 table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog:
 *     `org.apache.spark.sql.execution.command.v1.MsckRepairTableSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.MsckRepairTableSuite`
 */
trait MsckRepairTableSuiteBase extends command.MsckRepairTableSuiteBase {
  def deletePartitionDir(tableName: String, part: String): Unit = {
    val partLoc = getPartitionLocation(tableName, part)
    FileUtils.deleteDirectory(new File(partLoc))
  }

  test("drop partitions") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col INT, part INT) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")

      checkAnswer(spark.table(t), Seq(Row(0, 0), Row(1, 1)))
      deletePartitionDir(t, "part=1")
      sql(s"MSCK REPAIR TABLE $t DROP PARTITIONS")
      checkPartitions(t, Map("part" -> "0"))
      checkAnswer(spark.table(t), Seq(Row(0, 0)))
    }
  }

  test("sync partitions") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col INT, part INT) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")

      checkAnswer(sql(s"SELECT col, part FROM $t"), Seq(Row(0, 0), Row(1, 1)))
      copyPartition(t, "part=0", "part=2")
      deletePartitionDir(t, "part=0")
      sql(s"MSCK REPAIR TABLE $t SYNC PARTITIONS")
      checkPartitions(t, Map("part" -> "1"), Map("part" -> "2"))
      checkAnswer(sql(s"SELECT col, part FROM $t"), Seq(Row(1, 1), Row(0, 2)))
    }
  }
}

/**
 * The class contains tests for the `MSCK REPAIR TABLE` command to check
 * V1 In-Memory table catalog.
 */
class MsckRepairTableSuite extends MsckRepairTableSuiteBase with CommandSuiteBase
