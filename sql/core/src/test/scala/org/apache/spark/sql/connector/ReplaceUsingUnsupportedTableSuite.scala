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

package org.apache.spark.sql.connector

import org.apache.spark.sql.AnalysisException

/**
 * Verifies the analysis gate for `INSERT INTO ... REPLACE USING`: a table that supports row-level
 * operations but does not opt into the REPLACE command (i.e. does not implement
 * `SupportsRowLevelReplace`) must be rejected before its row-level operation builder is invoked.
 *
 * The base [[RowLevelOperationSuiteBase]] leaves `extraTableProps` empty, so the backing table is
 * the unmarked [[org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTable]].
 */
class ReplaceUsingUnsupportedTableSuite extends RowLevelOperationSuiteBase {

  private val schemaString = "pk INT NOT NULL, id INT, dep STRING"

  test("replace using is rejected for a row-level table that does not opt in") {
    createAndInitTable(schemaString,
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    val ex = intercept[AnalysisException] {
      sql(
        s"""INSERT INTO $tableNameAsString REPLACE USING (dep)
           |SELECT * FROM VALUES (10, 10, 'hr') AS t(pk, id, dep)
           |""".stripMargin)
    }
    checkError(
      exception = ex,
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map(
        "tableName" -> "`cat`.`ns1`.`test_table`",
        "operation" -> "INSERT INTO ... REPLACE ON/USING"))
  }
}
