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
 * Delta-based (merge-on-read / WriteDelta) variant of the SPARK-56729 non-deterministic
 * MERGE source tests. Mirrors GroupBasedMergeNondeterministicSuite but exercises the
 * WriteDelta path via DeltaBasedMergeIntoTableSuiteBase.
 */
object DeltaBasedMergeNondeterministicSuite {
  val evalCounter = new AtomicInteger(0)
}

class DeltaBasedMergeNondeterministicSuite extends DeltaBasedMergeIntoTableSuiteBase {

  import DeltaBasedMergeNondeterministicSuite.evalCounter

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props
  }

  test("SPARK-56729: non-deterministic source skips group filter and updates correctly") {
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
            DeltaBasedMergeNondeterministicSuite.evalCounter.incrementAndGet()
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

        // Both rows updated correctly (no lost update)
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

        // Source scanned only once (2 rows = 2 UDF calls)
        assert(evalCounter.get() == 2,
          s"Expected 2 UDF calls (single scan), got ${evalCounter.get()}")
      }
    }
  }
}
