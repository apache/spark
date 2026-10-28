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

import org.apache.spark.sql.Row

class DeltaBasedRowLevelOperationCatalystRuntimeFilterSuite
  extends RowLevelOperationCatalystRuntimeFilterSuiteBase {

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = super.extraTableProps
    props.put("supports-deltas", "true")
    props
  }

  test("delete does not use group filtering when the group key is not scanned") {
    // the table is partitioned by dep, so hr and software are the two groups
    createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
      """{ "pk": 1, "id": 1, "salary": 300, "dep": "hr" }
        |{ "pk": 2, "id": 2, "salary": 150, "dep": "software" }
        |{ "pk": 3, "id": 3, "salary": 120, "dep": "hr" }
        |""".stripMargin)

    val executedPlan = executeAndKeepPlan {
      sql(s"DELETE FROM $tableNameAsString WHERE salary IN (300, 400, 500)")
    }
    // a delta-based delete scans the row ID, the condition columns and the metadata columns, so
    // `dep` is not read and the scan cannot declare it as a filter attribute
    assertNoCatalystGroupFilter(executedPlan)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, 150, "software") :: Row(3, 3, 120, "hr") :: Nil)
  }
}
