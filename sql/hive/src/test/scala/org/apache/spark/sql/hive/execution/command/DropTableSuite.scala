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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command.v1

/**
 * The class contains tests for the `DROP TABLE` command to check V1 Hive external table catalog.
 */
class DropTableSuite extends v1.DropTableSuiteBase with CommandSuiteBase {
  test("hive client calls") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")
      // Drop existing table: 3 Hive client calls
      // 1. tableExists (in DropTableExec to check if table exists)
      // 2. getTable (in loadTable -> getTableRawMetadata to get table metadata)
      // 3. dropTable (the actual drop operation)
      checkHiveClientCalls(expected = 3) {
        sql(s"DROP TABLE $t")
      }
    }

    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      // Drop non-existent table with IF EXISTS: 1 Hive client call
      // 1. tableExists (returns false, IF EXISTS allows silent return)
      checkHiveClientCalls(expected = 1) {
        sql(s"DROP TABLE IF EXISTS $catalog.ns.tbl")
      }
      // Drop non-existent table without IF EXISTS: 1 Hive client call
      // 1. tableExists (returns false, throws TABLE_OR_VIEW_NOT_FOUND)
      checkHiveClientCalls(expected = 1) {
        intercept[AnalysisException] {
          sql(s"DROP TABLE $catalog.ns.tbl")
        }
      }
    }

    // Drop table in non-existent database with IF EXISTS: 1 Hive client call
    // 1. tableExists (returns false, IF EXISTS allows silent return)
    checkHiveClientCalls(expected = 1) {
      sql(s"DROP TABLE IF EXISTS $catalog.non_existent_db.tbl")
    }

    // Drop table in non-existent database without IF EXISTS: 1 Hive client call
    // 1. tableExists (returns false, throws TABLE_OR_VIEW_NOT_FOUND)
    checkHiveClientCalls(expected = 1) {
      intercept[AnalysisException] {
        sql(s"DROP TABLE $catalog.non_existent_db.tbl")
      }
    }
  }
}
