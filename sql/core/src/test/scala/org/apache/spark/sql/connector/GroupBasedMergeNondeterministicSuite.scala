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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests for SPARK-56729: non-deterministic MERGE source with runtime group filtering.
 *
 * Verifies that the determinism guard in RowLevelOperationRuntimeGroupFiltering
 * prevents lost updates when the source contains non-deterministic expressions.
 */
object GroupBasedMergeNondeterministicSuite {
  val evalCounter = new AtomicInteger(0)
}

class GroupBasedMergeNondeterministicSuite extends MergeIntoTableSuiteBase {

  import GroupBasedMergeNondeterministicSuite.evalCounter

  test("SPARK-56729: non-deterministic source skips group filter and updates correctly") {
    // A non-deterministic UDF that always passes marks the source as non-deterministic.
    // The determinism guard skips the group filter, so the source is scanned only once
    // and both rows are correctly updated. Without the guard, the group filter would
    // create a second independent scan of the source, risking lost updates.
    withSQLConf(
      SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"
    ) {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |""".stripMargin)

        evalCounter.set(0)

        spark.udf.register("nondet_pass",
          udf((pk: Int) => {
            GroupBasedMergeNondeterministicSuite.evalCounter.incrementAndGet()
            true
          }).asNondeterministic())

        sql(
          """CREATE OR REPLACE TEMP VIEW source AS
            |SELECT pk FROM VALUES (1), (2) t(pk) WHERE nondet_pass(pk)
            |""".stripMargin)

        val executedPlan = executeAndKeepPlan {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET t.salary = t.salary + 1
               |""".stripMargin)
        }

        // Both rows updated correctly (no lost update due to partition pruning)
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
          Seq(Row(1, 101, "hr"), Row(2, 201, "software")))

        // Group filter was NOT injected (guard skipped it)
        val dynamicFilters = collect(executedPlan) {
          case b: BatchScanExec if b.runtimeFilters.exists(
            _.isInstanceOf[DynamicPruningExpression]) => b
        }
        assert(dynamicFilters.isEmpty,
          "Group filter should NOT be injected for non-deterministic source")

        // Source scanned only once (2 rows = 2 UDF calls), not twice
        assert(evalCounter.get() == 2,
          s"Expected 2 UDF calls (single scan), got ${evalCounter.get()}")
      }
    }
  }

  test("SPARK-56729: non-deterministic source MERGE passes analysis and produces correct results") {
    // End-to-end: a MERGE with rand() in the source (non-deterministic) previously failed with
    // INVALID_NON_DETERMINISTIC_EXPRESSIONS. Now it passes analysis AND produces correct results
    // with runtime group filter enabled.
    withSQLConf(
      SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"
    ) {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |""".stripMargin)

        sql(
          """CREATE OR REPLACE TEMP VIEW source AS
            |SELECT pk, salary FROM VALUES (1, 150), (2, 250) t(pk, salary)
            |WHERE rand() < 2.0
            |""".stripMargin)

        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET t.salary = s.salary
             |""".stripMargin)

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
          Seq(Row(1, 150, "hr"), Row(2, 250, "software")))
      }
    }
  }

  test("SPARK-56729: deterministic source MERGE still uses runtime group filter") {
    // A deterministic source should still benefit from the group filter optimization.
    withSQLConf(
      SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"
    ) {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |""".stripMargin)

        sql(
          """CREATE OR REPLACE TEMP VIEW source AS
            |SELECT pk, salary FROM VALUES (1, 150) t(pk, salary)
            |""".stripMargin)

        val executedPlan = executeAndKeepPlan {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET t.salary = s.salary
               |""".stripMargin)
        }

        // Results are correct
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
          Seq(Row(1, 150, "hr"), Row(2, 200, "software")))

        // Dynamic pruning was applied (group filter injected for deterministic source)
        val dynamicFilters = collect(executedPlan) {
          case b: BatchScanExec if b.runtimeFilters.exists(
            _.isInstanceOf[DynamicPruningExpression]) => b
        }
        assert(dynamicFilters.nonEmpty,
          "Group filter should be injected for deterministic source MERGE")
      }
    }
  }

  test("SPARK-56729: non-deterministic source does not inject runtime group filter") {
    // Verify the plan-level assertion: group filter is skipped for non-deterministic source.
    withSQLConf(
      SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"
    ) {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |""".stripMargin)

        sql(
          """CREATE OR REPLACE TEMP VIEW source AS
            |SELECT pk, salary FROM VALUES (1, 150), (2, 250) t(pk, salary)
            |WHERE rand() < 2.0
            |""".stripMargin)

        val executedPlan = executeAndKeepPlan {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET t.salary = s.salary
               |""".stripMargin)
        }

        // No dynamic pruning applied (group filter skipped for non-deterministic source)
        val dynamicFilters = collect(executedPlan) {
          case b: BatchScanExec if b.runtimeFilters.exists(
            _.isInstanceOf[DynamicPruningExpression]) => b
        }
        assert(dynamicFilters.isEmpty,
          "Group filter should NOT be injected for non-deterministic source MERGE")
      }
    }
  }
}
