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

package org.apache.spark.sql.execution

import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ReuseExchangeAndSubquerySuite extends SparkPlanTest with SharedSparkSession {

  val tableFormat: String = "parquet"

  test("SPARK-32041: No reuse interference inside ReuseExchange") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable("df1", "df2") {
        spark.range(100)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df1")

        spark.range(10)
          .select(col("id"), col("id").as("k"))
          .write
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df2")

        val df = sql(
          """
            |WITH t AS (
            |  SELECT df1.id, df2.k
            |  FROM df1 JOIN df2 ON df1.k = df2.k
            |  WHERE df2.id < 2
            |)
            |SELECT * FROM t AS a JOIN t AS b ON a.id = b.id
            |""".stripMargin)

        val plan = df.queryExecution.executedPlan

        val exchangeIds = plan.collectWithSubqueries { case e: Exchange => e.id }
        val reusedExchangeIds = plan.collectWithSubqueries {
          case re: ReusedExchangeExec => re.child.id
        }

        assert(reusedExchangeIds.forall(exchangeIds.contains(_)),
          "ReusedExchangeExec should reuse an existing exchange")
      }
    }
  }

  test("SPARK-32041: No reuse interference between ReuseExchange and ReuseSubquery") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable("df1", "df2") {
        spark.range(100)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df1")

        spark.range(10)
          .select(col("id"), col("id").as("k"))
          .write
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df2")

        val df = sql(
          """
            |WITH t AS (
            |  SELECT df1.id, df2.k
            |  FROM df1 JOIN df2 ON df1.k = df2.k
            |  WHERE df2.id < 2
            |),
            |t2 AS (
            |  SELECT * FROM t
            |  UNION
            |  SELECT * FROM t
            |)
            |SELECT * FROM t2 AS a JOIN t2 AS b ON a.id = b.id
            |""".stripMargin)

        val plan = df.queryExecution.executedPlan

        val exchangeIds = plan.collectWithSubqueries { case e: Exchange => e.id }
        val reusedExchangeIds = plan.collectWithSubqueries {
          case re: ReusedExchangeExec => re.child.id
        }

        assert(reusedExchangeIds.forall(exchangeIds.contains(_)),
          "ReusedExchangeExec should reuse an existing exchange")
      }
    }
  }

  test("SPARK-49618: Reuse of exchange not happening with similar union plans") {
    import testImplicits._
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df1 = Seq(
        (7L, 45, 2.0f, 3.0d)
      ).toDF("l", "i", "f", "d")

      val df2 = Seq(
        (9L, 433, 1.0f, 5.0d)
      ).toDF("l", "i", "f", "d")

      val df3 = Seq(
        (55L, 4543, 635.0f)
      ).toDF("x", "y", "z")

      val df4 = Seq(
        (837L, 433)
      ).toDF("m", "p")

      val u1 = df1.select($"l", $"i").unionAll(df2.select($"l" as "a", $"i" as "b"))
      val u2 = df2.select($"l" as "a", $"i" as "b").unionAll(df1.select($"l", $"i"))
      val join1 = u1.hint("shuffle_merge").join(df3, $"l" === $"x").select($"l", $"y", $"i").
        groupBy($"y").agg(sum($"l"), sum($"i"))
      val join2 = u2.hint("shuffle_merge").join(df4, $"a" === $"m").select($"a", $"p", $"b").
        groupBy($"p").agg(sum($"a"), sum($"b"))
      val result = join1.unionAll(join2)
      val plan = result.queryExecution.executedPlan
      val exchangeIds = plan.collectWithSubqueries { case e: Exchange => e.id }
      val reusedExchangeIds = plan.collectWithSubqueries {
        case re: ReusedExchangeExec => re.child.id
      }
      assert(reusedExchangeIds.forall(exchangeIds.contains(_)),
        "ReusedExchangeExec should reuse an existing exchange")
      assert(reusedExchangeIds.size == 1)
    }
  }
}
