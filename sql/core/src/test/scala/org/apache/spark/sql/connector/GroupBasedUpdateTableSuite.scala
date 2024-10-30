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
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution.{InSubqueryExec, ReusedSubqueryExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf

class GroupBasedUpdateTableSuite extends UpdateTableSuiteBase {

  import testImplicits._

  test("update runtime group filtering") {
    Seq(true, false).foreach { ddpEnabled =>
      Seq(true, false).foreach { aqeEnabled =>
        withSQLConf(
            SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> ddpEnabled.toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString) {
          checkUpdateRuntimeGroupFiltering()
        }
      }
    }
  }

  private def checkUpdateRuntimeGroupFiltering(): Unit = {
    withTable(tableNameAsString) {
      withTempView("deleted_id") {
        createAndInitTable("id INT, salary INT, dep STRING",
          """{ "id": 1, "salary": 300, "dep": 'hr' }
            |{ "id": 2, "salary": 150, "dep": 'software' }
            |{ "id": 3, "salary": 120, "dep": 'hr' }
            |""".stripMargin)

        val deletedIdDF = Seq(Some(1), None).toDF()
        deletedIdDF.createOrReplaceTempView("deleted_id")

        executeAndCheckScans(
          s"UPDATE $tableNameAsString SET salary = -1 WHERE id IN (SELECT * FROM deleted_id)",
          primaryScanSchema = "id INT, salary INT, dep STRING, _partition STRING",
          groupFilterScanSchema = Some("id INT, dep STRING"))

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Row(1, -1, "hr") :: Row(2, 150, "software") :: Row(3, 120, "hr") :: Nil)

        checkReplacedPartitions(Seq("hr"))
      }
    }
  }

  test("update runtime filter subquery reuse") {
    Seq(true, false).foreach { aqeEnabled =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString) {
        checkUpdateRuntimeFilterSubqueryReuse()
      }
    }
  }

  private def checkUpdateRuntimeFilterSubqueryReuse(): Unit = {
    withTable(tableNameAsString) {
      withTempView("deleted_id") {
        createAndInitTable("id INT, salary INT, dep STRING",
          """{ "id": 1, "salary": 300, "dep": 'hr' }
            |{ "id": 2, "salary": 150, "dep": 'software' }
            |{ "id": 3, "salary": 120, "dep": 'hr' }
            |""".stripMargin)

        val deletedIdDF = Seq(Some(1), None).toDF()
        deletedIdDF.createOrReplaceTempView("deleted_id")

        val plan = executeAndKeepPlan {
          sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE id IN (SELECT * FROM deleted_id)")
        }

        val runtimePredicates = flatMap(plan) {
          case s: BatchScanExec =>
            val dynamicPruningExprs = s.runtimeFilters.asInstanceOf[Seq[DynamicPruningExpression]]
            dynamicPruningExprs.map(_.child)
          case _ =>
            Nil
        }
        val isSubqueryReused = runtimePredicates.exists {
          case InSubqueryExec(_, _: ReusedSubqueryExec, _, _, _, _) => true
          case _ => false
        }
        assert(isSubqueryReused, "runtime filter subquery must be reused")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Row(1, -1, "hr") :: Row(2, 150, "software") :: Row(3, 120, "hr") :: Nil)

        checkReplacedPartitions(Seq("hr"))
      }
    }
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
      parameters = Map(
        "sqlExprs" -> "\"((id <= 1) AND (rand() > 0.5))\", \"((id <= 1) AND (rand() > 0.5))\""),
      context = ExpectedContext(
        fragment = "UPDATE cat.ns1.test_table SET dep = 'invalid' WHERE id <= 1 AND rand() > 0.5",
        start = 0,
        stop = 75)
    )
  }
}
