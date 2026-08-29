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
import org.apache.spark.sql.connector.catalog.InMemoryTable
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.DeleteSummary

class GroupBasedRowLevelOperationCatalystRuntimeFilterSuite
  extends RowLevelOperationCatalystRuntimeFilterSuiteBase {

  test("delete runtime group filtering with SupportsRuntimeCatalystFiltering") {
    // the table is partitioned by dep, so hr and software are the two groups
    createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
      """{ "pk": 1, "id": 1, "salary": 300, "dep": "hr" }
        |{ "pk": 2, "id": 2, "salary": 150, "dep": "software" }
        |{ "pk": 3, "id": 3, "salary": 120, "dep": "hr" }
        |""".stripMargin)

    // only pk 1 matches, so hr is rewritten and its other row (pk 3) is copied over
    val executedPlan = executeAndKeepPlan {
      sql(s"DELETE FROM $tableNameAsString WHERE salary IN (300, 400, 500)")
    }
    assertCatalystGroupFilter(
      executedPlan,
      expectedFilterAttrs = Seq("dep"),
      expectedFilter = GroupFilter(scanSchema = "salary INT, dep STRING", groups = Seq("hr")))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, 150, "software") :: Row(3, 3, 120, "hr") :: Nil)

    checkReplacedPartitions(Seq("hr"))
    checkDeleteMetrics(numDeletedRows = 1, numCopiedRows = 1)
  }

  test("delete runtime group filtering by a nested attribute") {
    val schema = "pk INT NOT NULL, id INT, salary INT, " +
      "dep STRUCT<name: STRING, region: STRING>"
    createTable(schema, Array[Transform](identity(reference(Seq("dep", "name")))))
    append(schema,
      """{"pk":1,"id":1,"salary":300,"dep":{"name":"hr","region":"west"}}
        |{"pk":2,"id":2,"salary":150,"dep":{"name":"software","region":"west"}}
        |{"pk":3,"id":3,"salary":120,"dep":{"name":"hr","region":"east"}}
        |""".stripMargin)

    val executedPlan = executeAndKeepPlan {
      sql(s"DELETE FROM $tableNameAsString WHERE salary IN (300, 400, 500)")
    }
    assertCatalystGroupFilter(
      executedPlan,
      expectedFilterAttrs = Seq("dep.name"),
      expectedFilter = GroupFilter(
        scanSchema = "salary INT, dep STRUCT<name: STRING>", groups = Seq("hr")),
      expectedFilterPaths = Some(Seq(Seq("dep", "name"))))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, 150, Row("software", "west")) ::
        Row(3, 3, 120, Row("hr", "east")) :: Nil)

    checkReplacedPartitions(Seq("hr"))
    checkDeleteMetrics(numDeletedRows = 1, numCopiedRows = 1)
  }

  private def checkDeleteMetrics(numDeletedRows: Long, numCopiedRows: Long): Unit = {
    val t = catalog.loadTable(ident).asInstanceOf[InMemoryTable]
    val summary = t.commits.last.writeSummary.get.asInstanceOf[DeleteSummary]
    assert(summary.numDeletedRows() === numDeletedRows,
      s"Expected numDeletedRows=$numDeletedRows, got ${summary.numDeletedRows()}")
    assert(summary.numCopiedRows() === numCopiedRows,
      s"Expected numCopiedRows=$numCopiedRows, got ${summary.numCopiedRows()}")
  }
}
