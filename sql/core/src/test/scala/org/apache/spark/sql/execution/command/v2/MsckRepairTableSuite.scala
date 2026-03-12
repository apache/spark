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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `MSCK REPAIR TABLE` command
 * to check V2 table catalogs.
 */
class MsckRepairTableSuite
  extends command.MsckRepairTableSuiteBase
  with CommandSuiteBase {

  // TODO(SPARK-34397): Support v2 `MSCK REPAIR TABLE`
  test("repairing of v2 tables is not supported") {
    withNamespaceAndTable("ns", "tbl") { t =>
      spark.sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"MSCK REPAIR TABLE $t")
        },
        condition = "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE",
        parameters = Map("cmd" -> "MSCK REPAIR TABLE")
      )
    }
  }
}
