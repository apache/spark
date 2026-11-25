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

package org.apache.spark.sql.analysis.resolver

import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf

/**
 * Parity tests between the single-pass resolver and the fixed-point Analyzer. In dual run, a query
 * that both analyzers resolve must produce the same plan.
 */
class SinglePassResolverDualRunSuite extends HybridAnalyzerSuiteBase {

  testDualRun("Correlated subquery in HAVING references a grouping column") {
    checkAnswer(
      sql(
        """
          |SELECT col1
          |FROM VALUES (1), (2) AS t(col1)
          |GROUP BY col1
          |HAVING EXISTS (SELECT 1 FROM VALUES (1) AS s(c) WHERE s.c = col1)
          |""".stripMargin
      ),
      Row(1)
    )
  }

  testDualRun("Correlated subquery in HAVING binds the grouping column, not a same-named alias") {
    // `a` in the subquery is the grouping column `t.a`, like in HAVING itself, so both rows
    // qualify. Binding it to the SELECT alias `a + 10` would return no rows.
    checkAnswer(
      sql(
        """
          |SELECT a + 10 AS a
          |FROM VALUES (1, 2), (3, 4) AS t(a, b)
          |GROUP BY a
          |HAVING EXISTS (SELECT 1 FROM VALUES (2), (4) AS s(k) WHERE s.k - 1 = a)
          |""".stripMargin
      ),
      Seq(Row(11), Row(13))
    )
  }

  testDualRun("HAVING and ORDER BY over grouping analytics") {
    checkAnswer(
      sql(
        """
          |SELECT a, sum(b) AS s
          |FROM VALUES (1, 10), (2, 20) AS t(a, b)
          |GROUP BY ROLLUP(a)
          |HAVING sum(b) > 10
          |ORDER BY a
          |""".stripMargin
      ),
      Seq(Row(null, 30), Row(2, 20))
    )
  }

  testDualRun("Positional parameters with legacy parameter substitution") {
    withSQLConf(SQLConf.LEGACY_PARAMETER_SUBSTITUTION_CONSTANTS_ONLY.key -> "true") {
      checkAnswer(spark.sql("SELECT 1", Array(1)), Row(1))
      val explainOutput = spark.sql("EXPLAIN SELECT ?", Array(1)).collect()
      assert(explainOutput.head.getString(0).contains("Project [1 AS"))

      // The guard is skipped here, so only analyze: collecting adds operators that the
      // single-pass resolver does not support.
      withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark.sql("SELECT 1", Array(1)).queryExecution.assertAnalyzed()
        spark.sql("EXPLAIN SELECT ?", Array(1)).queryExecution.assertAnalyzed()
      }
    }
  }
}
