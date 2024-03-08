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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.execution.command.v1

/**
 * The class contains tests for the `ALTER TABLE .. RENAME PARTITION` command to check
 * V1 Hive external table catalog.
 */
class AlterTableRenamePartitionSuite
  extends v1.AlterTableRenamePartitionSuiteBase
  with CommandSuiteBase {

  test("hive client calls") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")

      checkHiveClientCalls(expected = 19) {
        sql(s"ALTER TABLE $t PARTITION (part=0) RENAME TO PARTITION (part=1)")
      }
      sql(s"CACHE TABLE $t")
      checkHiveClientCalls(expected = 19) {
        sql(s"ALTER TABLE $t PARTITION (part=1) RENAME TO PARTITION (part=2)")
      }
    }

    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, PART int) $defaultUsing PARTITIONED BY (PART)")
      sql(s"INSERT INTO $t PARTITION (PART=0) SELECT 0")
      checkHiveClientCalls(expected = 24) {
        sql(s"ALTER TABLE $t PARTITION (PART=0) RENAME TO PARTITION (PART=1)")
      }
    }
  }
}
