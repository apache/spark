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

class DeltaBasedDeleteFromTableSuite extends DeleteFromTableSuiteBase {

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props
  }

  test("delete with nondeterministic conditions") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException](
        sql(s"DELETE FROM $tableNameAsString WHERE id <= 1 AND rand() > 0.5")),
      condition = "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
      parameters = Map(
        "sqlExprs" -> "\"((id <= 1) AND (rand() > 0.5))\""),
      context = ExpectedContext(
        fragment = "DELETE FROM cat.ns1.test_table WHERE id <= 1 AND rand() > 0.5",
        start = 0,
        stop = 60)
    )
  }

  test("nullable row ID attrs") {
    createAndInitTable("pk INT, salary INT, dep STRING",
      """{ "pk": 1, "salary": 300, "dep": 'hr' }
        |{ "pk": 2, "salary": 150, "dep": 'software' }
        |{ "pk": 3, "salary": 120, "dep": 'hr' }
        |""".stripMargin)

    val exception = intercept[AnalysisException] {
      sql(s"DELETE FROM $tableNameAsString WHERE pk = 1")
    }
    assert(exception.message.contains("Row ID attributes cannot be nullable"))
  }

  test("delete with schema pruning") {
    createAndInitTable("pk INT NOT NULL, id INT, country STRING, dep STRING",
      """{ "pk": 1, "id": 1, "country": "uk", "dep": "hr" }
        |{ "pk": 2, "id": 2, "country": "us", "dep": "software" }
        |{ "pk": 3, "id": 3, "country": "canada", "dep": "hr" }
        |""".stripMargin)

    executeAndCheckScan(
      s"DELETE FROM $tableNameAsString WHERE id <= 1",
      // `pk` is used to encode deletes
      // `id` is used in the condition
      // `_partition` is used in the requested write distribution
      expectedScanSchema = "pk INT, id INT, _partition STRING")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, "us", "software") :: Row(3, 3, "canada", "hr") :: Nil)
  }
}
