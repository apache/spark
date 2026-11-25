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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{Row, StatisticsCollectionTestBase}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryOperator, Expression, IsNotNull, Literal}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.internal.SQLConf.{ADAPTIVE_EXECUTION_ENABLED, ANSI_ENABLED}

abstract class PrunePartitionSuiteBase extends StatisticsCollectionTestBase {

  protected def format: String

  test("SPARK-28169: Convert scan predicate condition to CNF") {
    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempView("temp") {
        withTable("t") {
          sql(
            s"""
               |CREATE TABLE t(i INT, p STRING)
               |USING $format
               |PARTITIONED BY (p)""".stripMargin)

          spark.range(0, 1000, 1).selectExpr("id as col")
            .createOrReplaceTempView("temp")

          for (part <- Seq(1, 2, 3, 4)) {
            sql(
              s"""
                 |INSERT OVERWRITE TABLE t PARTITION (p='$part')
                 |SELECT col FROM temp""".stripMargin)
          }

          assertPrunedPartitions(
            "SELECT * FROM t WHERE p = '1' OR (p = '2' AND i = 1)", 2,
            "((p = '1') || (p = '2'))")
          assertPrunedPartitions(
            "SELECT * FROM t WHERE (p = '1' AND i = 2) OR (i = 1 OR p = '2')", 4,
            "")
          assertPrunedPartitions(
            "SELECT * FROM t WHERE (p = '1' AND i = 2) OR (p = '3' AND i = 3 )", 2,
            "((p = '1') || (p = '3'))")
          assertPrunedPartitions(
            "SELECT * FROM t WHERE (p = '1' AND i = 2) OR (p = '2' OR p = '3')", 3,
            "((p = '1') || ((p = '2') || (p = '3')))")
          assertPrunedPartitions(
            "SELECT * FROM t", 4,
            "")
          assertPrunedPartitions(
            "SELECT * FROM t WHERE p = '1' AND i = 2", 1,
            "(p = '1')")
          assertPrunedPartitions(
            """
              |SELECT i, COUNT(1) FROM (
              |SELECT * FROM t WHERE  p = '1' OR (p = '2' AND i = 1)
              |) tmp GROUP BY i
            """.stripMargin, 2, "((p = '1') || (p = '2'))")
        }
      }
    }
  }

  test("SPARK-62187: a partition filter that can fail prunes partitions over a filter that can " +
    "be NULL") {
    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "false", ANSI_ENABLED.key -> "true") {
      withTable("t") {
        sql(s"CREATE TABLE t(i INT, j INT, p STRING) USING $format PARTITIONED BY (p)")
        sql("INSERT INTO t PARTITION (p = '1') VALUES (2, 3)")
        sql("INSERT INTO t PARTITION (p = '2') VALUES (NULL, 0)")
        sql("INSERT INTO t PARTITION (p = '3') VALUES (5, NULL)")
        // `i > 1 OR j > 2` can be NULL, so the cast, which can fail, is combined into the filter
        // below under an `If` of it, whose parts are still partition filters. Also after
        // `ReplaceNullWithFalseInPredicate` gives the CASE WHEN an ELSE FALSE.
        Seq("CAST(p AS INT) = 1", "CASE WHEN CAST(p AS INT) = 1 THEN true END").foreach { cond =>
          withClue(cond) {
            val df = sql(s"SELECT * FROM (SELECT * FROM t WHERE i > 1 OR j > 2) WHERE $cond")
            assert(getScanExecPartitionSize(df.queryExecution.sparkPlan) == 1)
            checkAnswer(df, Row(2, 3, "1"))
          }
        }
      }
    }
  }

  private def getCleanStringRepresentation(exp: Expression): String = exp match {
    case attr: AttributeReference =>
      attr.sql.replaceAll("spark_catalog.default.t.", "")
    case l: Literal =>
      l.sql
    case e: BinaryOperator =>
      s"(${getCleanStringRepresentation(e.left)} ${e.symbol} " +
        s"${getCleanStringRepresentation(e.right)})"
  }

  protected def assertPrunedPartitions(
      query: String,
      expectedPartitionCount: Long,
      expectedPushedDownFilters: String): Unit = {
    val qe = sql(query).queryExecution
    val plan = qe.sparkPlan
    assert(getScanExecPartitionSize(plan) == expectedPartitionCount)

    val collectFn: PartialFunction[SparkPlan, Seq[Expression]] =
      collectPartitionFiltersFn() orElse {
        case BatchScanExec(_, scan: FileScan, _, _, _, _, _, _) => scan.partitionFilters
      }
    val pushedDownPartitionFilters = plan.collectFirst(collectFn)
      .map(exps => exps.filterNot(e => e.isInstanceOf[IsNotNull]))
    val pushedFilters = pushedDownPartitionFilters.map(filters => {
      filters.foldLeft("")((currentStr, exp) => {
        if (currentStr == "") {
          s"${getCleanStringRepresentation(exp)}"
        } else {
          s"$currentStr AND ${getCleanStringRepresentation(exp)}"
        }
      })
    })

    assert(pushedFilters == Some(expectedPushedDownFilters))
  }

  protected def collectPartitionFiltersFn(): PartialFunction[SparkPlan, Seq[Expression]]

  protected def getScanExecPartitionSize(plan: SparkPlan): Long
}
