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
package org.apache.spark.sql.execution.joins

import java.lang.Long

import org.apache.spark.sql.{DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{And, IsNull}
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class InferAntiJoinSQLSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  def checkIt(df: DataFrame, checkAntiJoin: Boolean,
              expectedResult: Array[Row], checkFilter: Boolean): SparkPlan = {
    val thePlan = df.queryExecution.executedPlan
    val v = collect(thePlan)({ case j: BaseJoinExec if j.joinType == LeftAnti => j }).size
    val w = collect(thePlan)({ case f@FilterExec(_: IsNull, _: BaseJoinExec) => f }).size
    val res = df.collect()
    assert(res.sameElements(expectedResult),
      s"Results didn't match ${res.mkString} => ${expectedResult.mkString}")
    assert(v == (if (checkAntiJoin) 1 else 0),
      if (checkAntiJoin) "AntiJoin Not Found" else "Unexpected AntiJoin")
    assert(w == (if (checkFilter) 1 else 0),
      if (checkFilter) "Filter Not Found" else "Filter Not Removed")
    thePlan
  }

  test("check antijoin behavior with null on lhs") {
    val left = spark.range(2)
    val right = spark.range(2)

    val lCol = "case when id = 0 then null else id end as l1"
    val rCol = "id as r1"
    val result = Array(Row(null))
    checkIt(left.selectExpr(lCol)
      .join(right.selectExpr(rCol), $"l1" === $"r1", "leftAnti").select("l1"),
      checkAntiJoin = true, result, checkFilter = false)
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        checkIt(left.selectExpr(lCol)
          .join(right.selectExpr(rCol), $"l1" === $"r1", "leftOuter")
          .filter($"r1".isNull).select("l1"), inferAntiJoin, result, !inferAntiJoin)
      }
    }
  }

  test("check antijoin behavior with empty set on rhs") {
    val left = spark.range(2)
    val right = spark.range(2)

    val lCol = "case when id = 0 then null else id end as l1"
    val rCol = "id as r1"
    val result = Array(Row(null), Row(1))

    checkIt(left.selectExpr(lCol)
      .join(right.selectExpr(rCol).filter("r1 > 7"), $"l1" === $"r1", "leftAnti"),
      checkAntiJoin = true, result, checkFilter = false)
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        checkIt(left.selectExpr(lCol)
          .join(right.selectExpr(rCol).filter("r1 > 7"), $"l1" === $"r1", "leftOuter")
          .filter($"r1".isNull).select("l1"), inferAntiJoin, result, !inferAntiJoin)
      }
    }
  }

  test("check antijoin behavior with nulls on lhs and rhs") {
    val left = spark.range(2)
    val right = spark.range(2)
    val lCol = "case when id = 0 then null else id end as l1"
    val rCol = "case when id = 0 then null else id end as r1"
    val result = Array(Row(null))

    checkIt(left.alias("main").selectExpr(lCol)
      .join(right.selectExpr(rCol)
        , $"l1" === $"r1", "leftAnti"),
      checkAntiJoin = true, result, checkFilter = false)

    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        checkIt(left.alias("main").selectExpr(lCol)
          .join(right.selectExpr(rCol)
            , $"l1" === $"r1", "leftOuter")
          .filter($"r1".isNull).select("l1"), inferAntiJoin, result, !inferAntiJoin)
      }
    }
  }

  test("check antijoin behavior with all nulls on rhs") {
    val left = spark.range(2)
    val right = spark.range(2)
    val lCol = "case when id = 0 then null else id end as l1"
    val rCol = "case when id > -1 then null else id end as r1"
    val result = Array(Row(null), Row(1))

    checkIt(left.alias("main").selectExpr(lCol)
      .join(right.selectExpr(rCol), $"l1" === $"r1", "leftAnti"),
      checkAntiJoin = true, result, checkFilter = false)
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        checkIt(left.selectExpr(lCol)
          .join(right.selectExpr(rCol)
            , $"l1" === $"r1", "leftOuter")
          .filter($"r1".isNull).select("l1"), inferAntiJoin, result, !inferAntiJoin)
      }
    }
  }

  test("infer antijoin on clause on one colum is null on another but the same through aliasing") {
    val left = spark.range(2)
    val right = spark.range(2)
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        checkIt(left.alias("main")
          .join(right.selectExpr("id as r1", "id as r2"), $"id" === $"r1", "leftOuter")
          .filter("r2 is null").select("main.*"), inferAntiJoin, Array[Row](), !inferAntiJoin)
      }
    }
  }

  test("infer antijoin with distinct and CollapseProject - not effective") {
    val left = spark.range(2)
    val right = Seq(0, 1).toDF("id")
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
      val df = left.alias("main")
          .join(right.distinct()
            .selectExpr("id as r1", "id as r2"),
            $"id" === $"r1", "leftOuter")
          .filter("r2 is null").select("main.*")
        checkIt(df, checkAntiJoin = false, Array[Row](), checkFilter = true)
        val rules = df.queryExecution.tracker.rules
        val rsAj = rules("org.apache.spark.sql.catalyst.optimizer.InferAntiJoin")
        // because L & R columns are of Long/Int a Cast is added to the ON clause but not to
        // the isnull argument so InferAntiJoin doesn't rewrite in either of the invocations
        // but in the first one the RHS join child is a Project but in the 2nd it's Aggregate
        // due to CollapseProject
        assert(rsAj.numInvocations > 1)
        assert(rsAj.numEffectiveInvocations == 0)
        val rsCp = rules("org.apache.spark.sql.catalyst.optimizer.CollapseProject")
        assert(rsCp.numInvocations > 0)
        assert(rsCp.numEffectiveInvocations > 0)
      }
    }
  }

  test("infer antijoin with distinct and CollapseProject - effective") {
    val left = spark.range(2)
    val right = spark.range(2)
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        val df = left.alias("main")
          .join(right.distinct()
            .selectExpr("id as r1", "id as r2"),
            $"id" === $"r1", "leftOuter")
          .filter("r2 is null").select("main.*")
        checkIt(df, inferAntiJoin, Array[Row](), !inferAntiJoin)
        val rules = df.queryExecution.tracker.rules
        val rsAj = rules("org.apache.spark.sql.catalyst.optimizer.InferAntiJoin")
        assert(rsAj.numEffectiveInvocations == (if (inferAntiJoin) 1 else 0))
        val rsCp = rules("org.apache.spark.sql.catalyst.optimizer.CollapseProject")
        assert(rsCp.numEffectiveInvocations > 0)
      }
    }
  }

  test("infer antijoin on clause on one colum is null another but" +
    " the same through aliasing with function") {
    val left = spark.range(3)
    val right = spark.range(2)
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        checkIt(left.alias("main")
          .join(right.selectExpr("id + 1 as r1")
            .selectExpr("r1", "r1 as r2"), $"id" === $"r1", "leftOuter")
          .filter("r2 is null").select("main.*"), inferAntiJoin, Array(Row(0)), !inferAntiJoin)
      }
    }
  }

  test("infer antijoin with function") {
    val left = spark.range(3)
    val right = spark.range(2)
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        checkIt(left.alias("main")
          .join(right.alias("right"), $"main.id" === $"right.id" + 1, "leftOuter")
          .filter("right.id + 1 is null").select("main.*"),
          inferAntiJoin, Array(Row(0)), !inferAntiJoin)
      }
    }
  }

  test("infer antijoin check metrics metrics") {
    val left = spark.range(2).alias("left")
    val right = spark.range(2).alias("right")
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        val thePlan = checkIt(left
          .join(right, $"left.id" === $"right.id", "leftOuter")
          .filter("right.id is null").select("left.*"),
          inferAntiJoin, Array(), !inferAntiJoin)
        val j = collect(thePlan)({ case j: BaseJoinExec => j })
        assert(j.head.metrics("numOutputRows").value == (if (inferAntiJoin) 0 else 2))
      }
    }
  }

  test("infer antijoin with conjunction match by alias") {
    val left = spark.range(3).selectExpr("id", "id + 1 as l1").alias("left")
    val right = spark.range(2).selectExpr("id", "id as r1").alias("right")
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        checkIt(left
          .join(right, $"left.id" === $"right.id" && $"left.l1" === $"right.r1" + 1
            , "leftOuter")
          .filter("right.r1 is null").select("left.*"),
          inferAntiJoin, Array(Row(2, 3)), !inferAntiJoin)
      }
    }
  }

  test("infer antijoin with conjunction") {
    val left = spark.range(3).selectExpr("id", "id + 1 as l1").alias("left")
    val right = spark.range(2).selectExpr("id", "id as r1").alias("right")
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        checkIt(left
          .join(right, $"left.id" === $"right.id" && $"left.l1" === $"right.r1" + 1
            , "leftOuter")
          .filter("right.r1 + 1 is null").select("left.*"),
          inferAntiJoin, Array(Row(2, 3)), !inferAntiJoin)
      }
    }
  }

  // since AJ doesn't output any inner side columns
  test("don't infer antijoin due to not-removable Filter condition") {
    val left = spark.range(3)
    val right = spark.range(2)
    for (inferAntiJoin <- Seq(true, false)) {
      withSQLConf(SQLConf.INFER_ANTI_JOIN.key -> inferAntiJoin.toString) {
        val thePlan = checkIt(left.alias("left")
          .join(right.alias("right"), $"left.id" === $"right.id" + 1, "leftOuter")
          .filter("right.id + 1 is null AND left.id + right.id is null and left.id > -17")
          .select("left.*"),
          checkAntiJoin = false, Array(Row(0)), checkFilter = false)
        val w = collect(thePlan)({
          case f@FilterExec(And(_: IsNull, _: IsNull), _: BaseJoinExec) => f }).size
        assert(w == 1, "Original Filter not found")
      }
    }
  }

  def reuseExchangeCase(R: Dataset[Long],
                        S: Dataset[Long],
                        T: Dataset[Long],
                        pushThroughUnion: Boolean): Int = {
    withSQLConf(
      // to test with Shuffle joins
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PUSH_LEFT_SEMI_ANTI_THROUGH_UNION.key -> pushThroughUnion.toString) {

      val df = R.alias("R").selectExpr("id", "id as r1")
        .union(S.alias("S").selectExpr("id", "id as s1"))
        .join(T.alias("T").selectExpr("id").hint("shuffle_hash"), $"R.id" === $"T.id", "leftOuter")
        .filter("T.id is null").selectExpr("R.*")
      val thePlan = df.queryExecution.executedPlan
      val result = df.collect()
      collect(thePlan)({ case re: ReusedExchangeExec => re }).size
    }
  }

  test("push through union with reuse 1") {
    val R = spark.range(2)
    val S = spark.range(3)
    val T = spark.range(4)

    for( pushThroughUnion <- Seq(true, false)) {
      val v = reuseExchangeCase(R, S, T, pushThroughUnion)
      val numExpectedReuseExch = if (pushThroughUnion) 1 else 0
      assert(v == numExpectedReuseExch, "Unexpected number of ReusedExchange")
    }
  }

  test("push through union with reuse 2") {
    val R = spark.range(2)
    val S = spark.range(2)
    val T = spark.range(4)

    for( pushThroughUnion <- Seq(true, false)) {
      val v = reuseExchangeCase(R, S, T, pushThroughUnion)
      val numExpectedReuseExch = if (pushThroughUnion) 2 else 0
      assert(v == numExpectedReuseExch, "Unexpected number of ReusedExchange")
    }
  }
  test("push through union with reuse 3") {
    val R = spark.range(2)
    val S = spark.range(2)
    val T = spark.range(2)

    for( pushThroughUnion <- Seq(true, false)) {
      val v = reuseExchangeCase(R, S, T, pushThroughUnion)
      val numExpectedReuseExch = if (pushThroughUnion) 3 else 0
      assert(v == numExpectedReuseExch, "Unexpected number of ReusedExchange")
    }
  }
}
