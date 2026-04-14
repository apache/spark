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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class PushDownJoinThroughUnionSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.PUSH_DOWN_JOIN_THROUGH_UNION_ENABLED.key, "true")

  test("UNION ALL + broadcast JOIN produces correct results") {
    withTempView("fact1", "fact2", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        val fact1 = Seq((1, "a"), (2, "b")).toDF("id", "val1")
        val fact2 = Seq((3, "c"), (4, "d")).toDF("id", "val1")
        val dim = Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "label")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, f.val1, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |JOIN dim d ON f.id = d.id
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, "a", "x"),
          Row(2, "b", "y"),
          Row(3, "c", "z")
        ))
      }
    }
  }

  test("3-way UNION ALL + broadcast JOIN (TPC-DS pattern)") {
    withTempView("fact1", "fact2", "fact3", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        val fact1 = Seq((1, 10), (2, 20)).toDF("id", "amount")
        val fact2 = Seq((3, 30), (4, 40)).toDF("id", "amount")
        val fact3 = Seq((1, 50), (5, 60)).toDF("id", "amount")
        val dim = Seq((1, "web"), (2, "store"), (3, "catalog"), (5, "other"))
          .toDF("id", "channel")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        fact3.createOrReplaceTempView("fact3")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, f.amount, d.channel
            |FROM (
            |  SELECT * FROM fact1
            |  UNION ALL SELECT * FROM fact2
            |  UNION ALL SELECT * FROM fact3
            |) f
            |JOIN dim d ON f.id = d.id
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, 10, "web"),
          Row(2, 20, "store"),
          Row(3, 30, "catalog"),
          Row(1, 50, "web"),
          Row(5, 60, "other")
        ))
      }
    }
  }

  test("LeftOuter Join through UNION ALL produces correct results") {
    withTempView("fact1", "fact2", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        val fact1 = Seq((1, "a"), (2, "b")).toDF("id", "val1")
        val fact2 = Seq((3, "c"), (99, "d")).toDF("id", "val1")
        val dim = Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "label")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, f.val1, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |LEFT OUTER JOIN dim d ON f.id = d.id
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, "a", "x"),
          Row(2, "b", "y"),
          Row(3, "c", "z"),
          Row(99, "d", null)
        ))
      }
    }
  }

  test("Optimization disabled produces same results") {
    withTempView("fact1", "fact2", "dim") {
      val fact1 = Seq((1, "a"), (2, "b")).toDF("id", "val1")
      val fact2 = Seq((3, "c"), (4, "d")).toDF("id", "val1")
      val dim = Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "label")

      fact1.createOrReplaceTempView("fact1")
      fact2.createOrReplaceTempView("fact2")
      dim.createOrReplaceTempView("dim")

      val query =
        """SELECT f.id, f.val1, d.label
          |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
          |JOIN dim d ON f.id = d.id
        """.stripMargin

      val expected = Seq(
        Row(1, "a", "x"),
        Row(2, "b", "y"),
        Row(3, "c", "z")
      )

      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        checkAnswer(sql(query), expected)
      }

      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.PushDownJoinThroughUnion") {
        checkAnswer(sql(query), expected)
      }
    }
  }

  test("ColumnPruning works after join push down") {
    withTempView("fact1", "fact2", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        val fact1 = Seq((1, "a", 100), (2, "b", 200)).toDF("id", "val1", "val2")
        val fact2 = Seq((3, "c", 300), (4, "d", 400)).toDF("id", "val1", "val2")
        val dim = Seq((1, "x", "extra1"), (2, "y", "extra2"), (3, "z", "extra3"))
          .toDF("id", "label", "info")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |JOIN dim d ON f.id = d.id
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, "x"),
          Row(2, "y"),
          Row(3, "z")
        ))
      }
    }
  }

  test("2-way UNION ALL reuses broadcast exchange") {
    withTempView("fact1", "fact2", "dim") {
      val fact1 = Seq((1, "a"), (2, "b")).toDF("id", "val1")
      val fact2 = Seq((3, "c"), (4, "d")).toDF("id", "val1")
      val dim = Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "label")

      fact1.createOrReplaceTempView("fact1")
      fact2.createOrReplaceTempView("fact2")
      dim.createOrReplaceTempView("dim")

      val result = sql(
        """SELECT /*+ BROADCAST(d) */ f.id, f.val1, d.label
          |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
          |JOIN dim d ON f.id = d.id
        """.stripMargin)

      result.collect()
      val plan = result.queryExecution.executedPlan

      val broadcastStages = collect(plan) {
        case b: BroadcastQueryStageExec => b
      }
      val reusedBroadcasts = collectWithSubqueries(plan) {
        case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _) => e
      }

      assert(broadcastStages.size == 2,
        "Expected 2 BroadcastQueryStageExec (1 original + 1 reused) but found " +
          broadcastStages.size)
      assert(reusedBroadcasts.size == 1,
        "Expected exactly 1 ReusedExchangeExec inside BroadcastQueryStageExec but found " +
          reusedBroadcasts.size)
    }
  }

  test("PushPredicateThroughJoin works after join push down") {
    withTempView("fact1", "fact2", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        val fact1 = Seq((1, "a"), (2, "b")).toDF("id", "val1")
        val fact2 = Seq((3, "c"), (4, "d")).toDF("id", "val1")
        val dim = Seq((1, "x"), (2, "y"), (3, "z"), (4, "w")).toDF("id", "label")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, f.val1, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |JOIN dim d ON f.id = d.id
            |WHERE d.label IN ('x', 'z')
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, "a", "x"),
          Row(3, "c", "z")
        ))
      }
    }
  }
}
