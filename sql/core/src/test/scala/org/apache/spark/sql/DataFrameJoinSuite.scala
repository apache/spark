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

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Project, Join}
import org.apache.spark.sql.execution.joins.BroadcastHashJoin
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameJoinSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("join - join using") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")

    checkAnswer(
      df.join(df2, "int"),
      Row(1, "1", "2") :: Row(2, "2", "3") :: Row(3, "3", "4") :: Nil)
  }

  test("join - join using multiple columns") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1, i.toString)).toDF("int", "int2", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, i + 1, (i + 1).toString)).toDF("int", "int2", "str")

    checkAnswer(
      df.join(df2, Seq("int", "int2")),
      Row(1, 2, "1", "2") :: Row(2, 3, "2", "3") :: Row(3, 4, "3", "4") :: Nil)
  }

  test("join - join using multiple columns and specifying join type") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str")
    val df2 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str")

    checkAnswer(
      df.join(df2, Seq("int", "str"), "inner"),
      Row(1, "1", 2, 3) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "left"),
      Row(1, "1", 2, 3) :: Row(3, "3", 4, null) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "right"),
      Row(1, "1", 2, 3) :: Row(5, "5", null, 6) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "outer"),
      Row(1, "1", 2, 3) :: Row(3, "3", 4, null) :: Row(5, "5", null, 6) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "left_semi"),
      Row(1, "1", 2) :: Nil)
  }

  test("join - join using self join") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")

    // self join
    checkAnswer(
      df.join(df, "int"),
      Row(1, "1", "1") :: Row(2, "2", "2") :: Row(3, "3", "3") :: Nil)
  }

  test("join - self join") {
    val df1 = testData.select(testData("key")).as('df1)
    val df2 = testData.select(testData("key")).as('df2)

    checkAnswer(
      df1.join(df2, $"df1.key" === $"df2.key"),
      sql("SELECT a.key, b.key FROM testData a JOIN testData b ON a.key = b.key")
        .collect().toSeq)
  }

  test("join - using aliases after self join") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    checkAnswer(
      df.as('x).join(df.as('y), $"x.str" === $"y.str").groupBy("x.str").count(),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)

    checkAnswer(
      df.as('x).join(df.as('y), $"x.str" === $"y.str").groupBy("y.str").count(),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
  }

  test("[SPARK-6231] join - self join auto resolve ambiguity") {
    val df = Seq((1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.join(df, df("key") === df("key")),
      Row(1, "1", 1, "1") :: Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df.filter($"value" === "2"), df("key") === df("key")),
      Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df, df("key") === df("key") && df("value") === 1),
      Row(1, "1", 1, "1") :: Nil)

    val left = df.groupBy("key").agg(count("*"))
    val right = df.groupBy("key").agg(sum("key"))
    checkAnswer(
      left.join(right, left("key") === right("key")),
      Row(1, 1, 1, 1) :: Row(2, 1, 2, 2) :: Nil)
  }

  test("broadcast join hint") {
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")

    // equijoin - should be converted into broadcast join
    val plan1 = df1.join(broadcast(df2), "key").queryExecution.executedPlan
    assert(plan1.collect { case p: BroadcastHashJoin => p }.size === 1)

    // no join key -- should not be a broadcast join
    val plan2 = df1.join(broadcast(df2)).queryExecution.executedPlan
    assert(plan2.collect { case p: BroadcastHashJoin => p }.size === 0)

    // planner should not crash without a join
    broadcast(df1).queryExecution.executedPlan

    // SPARK-12275: no physical plan for BroadcastHint in some condition
    withTempPath { path =>
      df1.write.parquet(path.getCanonicalPath)
      val pf1 = sqlContext.read.parquet(path.getCanonicalPath)
      assert(df1.join(broadcast(pf1)).count() === 4)
    }
  }

  test("join - left outer to inner by the parent join's join condition") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str").as("a")
    val df2 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str").as("b")
    val df3 = Seq((1, 3, "1"), (3, 6, "5")).toDF("int", "int2", "str").as("c")

    // Left -> Inner
    val right = df.join(df2, $"a.int" === $"b.int", "left")
    val left2Inner =
      df3.join(right, $"c.int" === $"b.int", "inner").select($"a.*", $"b.*", $"c.*")

    left2Inner.explain(true)

    // The order before conversion: Left Then Inner
    assert(left2Inner.queryExecution.analyzed.collect {
      case j@Join(_, Join(_, _, LeftOuter, _), Inner, _) => j
    }.size === 1)

    // The order after conversion: Inner Then Inner
    assert(left2Inner.queryExecution.optimizedPlan.collect {
      case j@Join(_, Join(_, _, Inner, _), Inner, _) => j
    }.size === 1)

    checkAnswer(
      left2Inner,
      Row(1, 2, "1", 1, 3, "1", 1, 3, "1") :: Nil)
  }

  test("join - right outer to inner by the parent join's join condition") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str").as("a")
    val df2 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str").as("b")
    val df3 = Seq((1, 9, "8"), (5, 0, "4")).toDF("int", "int2", "str").as("c")

    // Right Then Inner -> Inner Then Right
    val right2Inner = df.join(df2, $"a.int" === $"b.int", "right")
      .join(df3, $"a.int" === $"b.int", "inner").select($"a.*", $"b.*", $"c.*")

    // The order before conversion: Left Then Inner
    assert(right2Inner.queryExecution.analyzed.collect {
      case j@Join(Join(_, _, RightOuter, _), _, Inner, _) => j
    }.size === 1)

    // The order after conversion: Inner Then Inner
    assert(right2Inner.queryExecution.optimizedPlan.collect {
      case j@Join(Join(_, _, Inner, _), _, Inner, _) => j
    }.size === 1)

    checkAnswer(
      right2Inner,
      Row(1, 2, "1", 1, 3, "1", 1, 9, "8") ::
      Row(1, 2, "1", 1, 3, "1", 5, 0, "4") :: Nil)
  }

  test("join - full outer to inner by the parent join's join condition") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str").as("a")
    val df2 = Seq((1, 2, "1"), (5, 6, "5")).toDF("int", "int2", "str").as("b")
    val df3 = Seq((1, 3, "1"), (3, 6, "5")).toDF("int", "int2", "str").as("c")

    // Full -> Inner
    val right = df.join(df2, $"a.int" === $"b.int", "full")
    val full2Inner = df3.join(right, $"c.int" === $"a.int" && $"b.int" === 1, "inner")
      .select($"a.*", $"b.*", $"c.*")

    // The order before conversion: Left Then Inner
    assert(full2Inner.queryExecution.analyzed.collect {
      case j@Join(_, Join(_, _, FullOuter, _), Inner, _) => j
    }.size === 1)

    // The order after conversion: Inner Then Inner
    assert(full2Inner.queryExecution.optimizedPlan.collect {
      case j@Join(_, Join(_, _, Inner, _), Inner, _) => j
    }.size === 1)

    checkAnswer(
      full2Inner,
      Row(1, 2, "1", 1, 2, "1", 1, 3, "1") :: Nil)
  }

  test("join - full outer to right by the parent join's join condition") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str").as("a")
    val df2 = Seq((1, 2, "1"), (5, 6, "5")).toDF("int", "int2", "str").as("b")
    val df3 = Seq((1, 3, "1"), (3, 6, "5")).toDF("int", "int2", "str").as("c")

    // Full -> Right
    val right = df.join(df2, $"a.int" === $"b.int", "full")
    val full2Right = df3.join(right, $"b.int" === 1, "leftsemi")

    // The order before conversion: Left Then Inner
    assert(full2Right.queryExecution.analyzed.collect {
      case j@Join(_, Join(_, _, FullOuter, _), LeftSemi, _) => j
    }.size === 1)

    // The order after conversion: Inner Then Inner
    assert(full2Right.queryExecution.optimizedPlan.collect {
      case j@Join(_, Project(_, Join(_, _, RightOuter, _)), LeftSemi, _) => j
    }.size === 1)

    checkAnswer(
      full2Right,
      Row(1, 3, "1") :: Row(3, 6, "5") :: Nil)
  }


  test("join - full outer to left by the parent join's join condition #1") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str").as("a")
    val df2 = Seq((1, 2, "1"), (5, 6, "5")).toDF("int", "int2", "str").as("b")
    val df3 = Seq((1, 3, "1"), (4, 6, "5")).toDF("int", "int2", "str").as("c")

    // Full -> Left
    val right = df.join(df2, $"a.int" === $"b.int", "full")
    val full2Left = df3.join(right, $"c.int" === $"a.int", "left")
      .select($"a.*", $"b.*", $"c.*")

    // The order before conversion: Full Then Left
    assert(full2Left.queryExecution.analyzed.collect {
      case j@Join(_, Join(_, _, FullOuter, _), LeftOuter, _) => j
    }.size === 1)

    // The order after conversion: Left Then Left
    assert(full2Left.queryExecution.optimizedPlan.collect {
      case j@Join(_, Join(_, _, LeftOuter, _), LeftOuter, _) => j
    }.size === 1)

    checkAnswer(
      full2Left,
      Row(1, 2, "1", 1, 2, "1", 1, 3, "1") ::
      Row(null, null, null, null, null, null, 4, 6, "5") :: Nil)
  }

  test("join - full outer to left by the parent join's join condition #2") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str").as("a")
    val df2 = Seq((1, 2, "1"), (5, 6, "5")).toDF("int", "int2", "str").as("b")
    val df3 = Seq((1, 3, "1"), (4, 6, "5")).toDF("int", "int2", "str").as("c")

    // Full -> Left
    val full2Left = df.join(df2, $"a.int" === $"b.int", "full")
      .join(df3, $"c.int" === $"a.int", "right").select($"a.*", $"b.*", $"c.*")

    // The order before conversion: Full Then Right
    assert(full2Left.queryExecution.analyzed.collect {
      case j@Join(Join(_, _, FullOuter, _), _, RightOuter, _) => j
    }.size === 1)

    // The order after conversion: Left Then Right
    assert(full2Left.queryExecution.optimizedPlan.collect {
      case j@Join(Join(_, _, LeftOuter, _), _, RightOuter, _) => j
    }.size === 1)

    checkAnswer(
      full2Left,
      Row(1, 2, "1", 1, 2, "1", 1, 3, "1") ::
      Row(null, null, null, null, null, null, 4, 6, "5") :: Nil)
  }
}
