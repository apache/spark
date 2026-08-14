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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.test.SharedSparkSession

class TableCacheQueryStageExecSuite  extends SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("SPARK-58767: re-use of exchange involving cached plans ") {
    withTable("t1", "t2", "t3") {
      spark.sql(
          """
            |CREATE TABLE t1 USING PARQUET AS
            |SELECT * FROM VALUES
            |  (1, 2),
            |  (1, 1),
            |  (2, 2),
            |  (2, 2),
            |  (2, 1),
            |  (3, 4),
            |  (4, 3),
            |  (3, 3),
            |  (4, 4),
            |  (5, 6),
            |  (6, 5),
            |  (6, 6),
            |  (5, 5)
            |AS data(c1_1, c2_1)
            |""".stripMargin
        )
        spark.sql(
          """
            |CREATE TABLE t2 USING PARQUET AS
            |SELECT * FROM VALUES
            |  (1, 2),
            |  (1, 1),
            |  (2, 2),
            |  (2, 2),
            |  (2, 1),
            |  (3, 4),
            |  (4, 3),
            |  (3, 3),
            |  (4, 4),
            |  (5, 6),
            |  (6, 5),
            |  (6, 6),
            |  (5, 5)
            |AS data(c1_2, c2_2)
            |""".stripMargin
        )

        spark.sql(
          """
            |CREATE TABLE t3 USING PARQUET AS
            |SELECT * FROM VALUES
            | (1, 2),
            |  (1, 1),
            |  (2, 2),
            |  (2, 2),
            |  (2, 1),
            |  (3, 4),
            |  (4, 3),
            |  (3, 3),
            |  (4, 4),
            |  (5, 6),
            |  (6, 5),
            |  (6, 6),
            |  (5, 5)
            |AS data(c1_3, c2_3)
            |""".stripMargin
        )
        spark.table("t1").where($"c1_1" > 0).createTempView("v1")
        spark.catalog.cacheTable("v1")
        val al1 = spark.table("v1").as("al1")
        val al2 = spark.table("v1").as("al2")
        val df = spark.table("t2").where($"c2_2" > 0).join(
            spark.table("t3"), spark.table("t2").col("c1_2") === spark.table("t3").col("c1_3")).
          join(
            broadcast(al1.where($"al1.c1_1" > 0)), $"al1.c1_1" === spark.table("t2").col("c1_2"))
          .join(broadcast(al2.where($"al2.c1_1" > 0)), $"al2.c1_1" === spark.table("t3").col
          ("c1_3"))
        df.collect()
        val allBroadcastStages = collectAllBroadcastStageExec(df.queryExecution.executedPlan)
        assert(allBroadcastStages.count(_.plan.isInstanceOf[ReusedExchangeExec]) == 1)

    }
  }
  def collectAllBroadcastStageExec(sp: SparkPlan): Seq[BroadcastQueryStageExec] = {
    sp match {
      case ap: AdaptiveSparkPlanExec => collectAllBroadcastStageExec(ap.finalPhysicalPlan)

      case b: BroadcastQueryStageExec => collectAllBroadcastStageExec(b.plan) :+ b

      case other => other.children.flatMap(collectAllBroadcastStageExec)
    }

  }
}
