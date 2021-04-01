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
import org.apache.spark.sql.internal.SQLConf

/**
 * The class contains tests for the `ALTER TABLE .. ADD PARTITION` command to check
 * V1 Hive external table catalog.
 */
class AlterTableAddPartitionSuite
  extends v1.AlterTableAddPartitionSuiteBase
  with CommandSuiteBase {

  test("hive client calls") {
    Seq(false, true).foreach { statsOn =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> statsOn.toString) {
        withNamespaceAndTable("ns", "tbl") { t =>
          sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
          sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
          assert(!statsOn || getTableSize(t) > 0)

          checkHiveClientCalls(expected = 17) {
            sql(s"ALTER TABLE $t ADD PARTITION (part=1)")
          }
          sql(s"CACHE TABLE $t")
          checkHiveClientCalls(expected = 17) {
            sql(s"ALTER TABLE $t ADD PARTITION (part=2)")
          }
        }
      }
    }
  }
}
