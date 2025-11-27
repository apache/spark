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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `TRUNCATE TABLE` command to check V2 table catalogs.
 */
class TruncateTableSuite extends command.TruncateTableSuiteBase with CommandSuiteBase {

  override val invalidPartColumnError = "not a valid partition column in table"

  test("truncate a partition of a table which does not support partitions") {
    withNamespaceAndTable("ns", "tbl", s"non_part_$catalog") { t =>
      sql(s"CREATE TABLE $t (c0 INT) $defaultUsing")
      sql(s"INSERT INTO $t SELECT 0")
      val sqlText = s"TRUNCATE TABLE $t PARTITION (c0=1)"
      val tableName =
        UnresolvedAttribute.parseAttributeName(t).map(quoteIdentifier).mkString(".")

      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED",
        parameters = Map("name" -> tableName),
        context = ExpectedContext(
          fragment = t,
          start = 15,
          stop = 42))
    }
  }
}
