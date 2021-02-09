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
 * The class contains tests for the `TRUNCATE TABLE` command to check V2 table catalogs.
 */
class TruncateTableSuite extends command.TruncateTableSuiteBase with CommandSuiteBase {

  // TODO(SPARK-34290): Support v2 TRUNCATE TABLE
  test("truncation of v2 tables is not supported") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")

      Seq(
        s"TRUNCATE TABLE $t PARTITION (part=1)",
        s"TRUNCATE TABLE $t").foreach { truncateCmd =>
        val errMsg = intercept[AnalysisException] {
          sql(truncateCmd)
        }.getMessage
        assert(errMsg.contains("TRUNCATE TABLE is not supported for v2 tables"))
      }
    }
  }
}
