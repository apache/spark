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

import org.apache.spark.sql.{AnalysisException, Row}

abstract class DeltaBasedUpdateTableSuiteBase extends UpdateTableSuiteBase {

  test("nullable row ID attrs") {
    createAndInitTable("pk INT, salary INT, dep STRING",
      """{ "pk": 1, "salary": 300, "dep": 'hr' }
        |{ "pk": 2, "salary": 150, "dep": 'software' }
        |{ "pk": 3, "salary": 120, "dep": 'hr' }
        |""".stripMargin)

    checkErrorMatchPVals(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
      },
      condition = "NULLABLE_ROW_ID_ATTRIBUTES",
      parameters = Map("nullableRowIdAttrs" -> "pk#\\d+")
    )
  }

  test("update with assignments to row ID") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET pk = 10 WHERE id = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(10, 1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("update with nondeterministic conditions") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET dep = 'invalid' WHERE id <= 1 AND rand() > 0.5")
      },
      condition = "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
      parameters = Map("sqlExprs" -> "\"((id <= 1) AND (rand() > 0.5))\""),
      context = ExpectedContext(
        fragment = "UPDATE cat.ns1.test_table SET dep = 'invalid' WHERE id <= 1 AND rand() > 0.5",
        start = 0,
        stop = 75)
    )
  }

  test("update with schema pruning") {
    createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
      """{ "pk": 1, "id": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "id": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    executeAndCheckScan(
      s"UPDATE $tableNameAsString SET salary = -1, id = -1, dep = 'invalid' WHERE pk = 1",
      // `pk` is used in the condition
      // `_partition` is used in the requested write distribution
      expectedScanSchema = "pk INT, _partition STRING")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, -1, "invalid") :: Row(2, 2, 200, "software") :: Row(3, 3, 300, "hr") :: Nil)
  }
}
