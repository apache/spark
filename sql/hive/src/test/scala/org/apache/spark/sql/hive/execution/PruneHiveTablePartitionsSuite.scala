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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.SparkPlan

class PruneHiveTablePartitionsSuite extends PrunePartitionSuiteBase {

  override def format(): String = "hive"

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("PruneHiveTablePartitions", Once,
        EliminateSubqueryAliases, new PruneHiveTablePartitions(spark)) :: Nil
  }

  test("SPARK-15616: statistics pruned after going through PruneHiveTablePartitions") {
    withTable("test", "temp") {
      sql(
        s"""
          |CREATE TABLE test(i int)
          |PARTITIONED BY (p int)
          |STORED AS textfile""".stripMargin)
      spark.range(0, 1000, 1).selectExpr("id as col")
        .createOrReplaceTempView("temp")

      for (part <- Seq(1, 2, 3, 4)) {
        sql(
          s"""
            |INSERT OVERWRITE TABLE test PARTITION (p='$part')
            |select col from temp""".stripMargin)
      }
      val analyzed1 = sql("select i from test where p > 0").queryExecution.analyzed
      val analyzed2 = sql("select i from test where p = 1").queryExecution.analyzed
      assert(Optimize.execute(analyzed1).stats.sizeInBytes / 4 ===
        Optimize.execute(analyzed2).stats.sizeInBytes)
    }
  }

  test("Avoid generating too many predicates in partition pruning") {
    withTempView("temp") {
      withTable("t") {
        sql(
          s"""
             |CREATE TABLE t(i INT, p0 INT, p1 INT)
             |USING $format
             |PARTITIONED BY (p0, p1)""".stripMargin)

        spark.range(0, 10, 1).selectExpr("id as col")
          .createOrReplaceTempView("temp")

        for (part <- (0 to 25)) {
          sql(
            s"""
               |INSERT OVERWRITE TABLE t PARTITION (p0='$part', p1='$part')
               |SELECT col FROM temp""".stripMargin)
        }
        val scale = 20
        val predicate = (1 to scale).map(i => s"(p0 = '$i' AND p1 = '$i')").mkString(" OR ")
        assertPrunedPartitions(s"SELECT * FROM t WHERE $predicate", scale)
      }
    }
  }

  override def getScanExecPartitionSize(plan: SparkPlan): Long = {
    plan.collectFirst {
      case p: HiveTableScanExec => p
    }.get.prunedPartitions.size
  }
}
