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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.command

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
}

/**
 * The class contains tests for the `ALTER TABLE .. RENAME PARTITION` command to check
 * V1 In-Memory table catalog.
 */
class AlterTableRenamePartitionSuite
  extends AlterTableRenamePartitionSuiteBase
  with CommandSuiteBase
