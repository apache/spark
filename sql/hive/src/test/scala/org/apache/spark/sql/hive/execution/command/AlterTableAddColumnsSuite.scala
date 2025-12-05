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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.execution.command.v1

/**
 * The class contains tests for the `ALTER TABLE .. ADD COLUMNS` command to check
 * V1 Hive external table catalog.
 */
class AlterTableAddColumnsSuite
  extends v1.AlterTableAddColumnsSuiteBase
  with CommandSuiteBase {

  test("SPARK-36949: Disallow tables with ANSI intervals when the provider is Hive") {
    def check(tbl: String): Unit = {
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          sql(s"ALTER TABLE $tbl ADD COLUMNS (ym INTERVAL YEAR)")
        },
        condition = "UNSUPPORTED_FEATURE.HIVE_WITH_ANSI_INTERVALS",
        parameters = Map("tableName" -> toSQLId(tbl))
      )
    }
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"CREATE TABLE $tbl (id INT) $defaultUsing")
      check(tbl)
    }
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"CREATE TABLE $tbl STORED AS PARQUET AS SELECT 1")
      check(tbl)
    }
  }
}
