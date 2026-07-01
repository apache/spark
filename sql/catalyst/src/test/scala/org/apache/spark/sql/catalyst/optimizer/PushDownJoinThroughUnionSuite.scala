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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Explode, Rand}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class PushDownJoinThroughUnionSuite extends PlanTest {

  private val testConf = new SQLConf()
  testConf.setConf(SQLConf.PUSH_DOWN_JOIN_THROUGH_UNION_ENABLED, true)
  testConf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, 1000L)

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PushDownJoinThroughUnion", FixedPoint(10),
      PushDownJoinThroughUnion(testConf)) :: Nil
  }

  val testRelation1 = LocalRelation($"a".int, $"b".int)
  val testRelation2 = LocalRelation($"c".int, $"d".int)
  val testRelation3 = LocalRelation($"e".int, $"f".int)
  val testRelation4 = LocalRelation($"g".int, $"h".int)

  test("Push down Inner Join through Union when right side is small") {
    val union = Union(testRelation1, testRelation2)
    val query = union.join(testRelation3, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    val expected = Union(
      testRelation1.join(testRelation3, Inner, Some($"a" === $"e")),
      testRelation2.join(testRelation3, Inner, Some($"c" === $"e"))
    ).analyze

    comparePlans(optimized, expected)
  }

  test("Push down Left Outer Join through Union when right side is small") {
    val union = Union(testRelation1, testRelation2)
    val query = union.join(testRelation3, LeftOuter, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    val expected = Union(
      testRelation1.join(testRelation3, LeftOuter, Some($"a" === $"e")),
      testRelation2.join(testRelation3, LeftOuter, Some($"c" === $"e"))
    ).analyze

    comparePlans(optimized, expected)
  }

  test("Do not push down when right side is too large (broadcast disabled)") {
    val noBroadcastConf = new SQLConf()
    noBroadcastConf.setConf(SQLConf.PUSH_DOWN_JOIN_THROUGH_UNION_ENABLED, true)
    noBroadcastConf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    val optimizer = new RuleExecutor[LogicalPlan] {
      val batches = Batch("PushDownJoinThroughUnion", FixedPoint(10),
        PushDownJoinThroughUnion(noBroadcastConf)) :: Nil
    }
    val union = Union(testRelation1, testRelation2)
    val query = union.join(testRelation3, Inner, Some($"a" === $"e"))
    val optimized = optimizer.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("Correctly rewrite attributes in join condition") {
    val union = Union(testRelation1, testRelation2)
    val query = union.join(testRelation3, Inner, Some($"a" === $"e" && $"b" > 10))
    val optimized = Optimize.execute(query.analyze)

    val expected = Union(
      testRelation1.join(testRelation3, Inner, Some($"a" === $"e" && $"b" > 10)),
      testRelation2.join(testRelation3, Inner, Some($"c" === $"e" && $"d" > 10))
    ).analyze

    comparePlans(optimized, expected)
  }

  test("Push down Inner Join through 3-way Union (TPC-DS pattern)") {
    val union = Union(Seq(testRelation1, testRelation2, testRelation4))
    val query = union.join(testRelation3, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    val expected = Union(Seq(
      testRelation1.join(testRelation3, Inner, Some($"a" === $"e")),
      testRelation2.join(testRelation3, Inner, Some($"c" === $"e")),
      testRelation4.join(testRelation3, Inner, Some($"g" === $"e"))
    )).analyze

    comparePlans(optimized, expected)
  }

  test("Do not push down unsupported join types") {
    val union = Union(testRelation1, testRelation2)
    Seq(RightOuter, FullOuter, LeftSemi, LeftAnti).foreach { joinType =>
      val query = union.join(testRelation3, joinType, Some($"a" === $"e"))
      val optimized = Optimize.execute(query.analyze)
      comparePlans(optimized, query.analyze)
    }
  }

  test("Do not push down Cross Join (no join condition)") {
    val union = Union(testRelation1, testRelation2)
    val query = union.join(testRelation3, Inner, None)
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("Do not push down when Union is on the right side") {
    val union = Union(testRelation1, testRelation2)
    val query = testRelation3.join(union, Inner, Some($"e" === $"a"))
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("Push down when right side is a complex subplan") {
    val complexRight = testRelation3
      .where($"f" > 0)
      .select($"e", ($"f" + 1).as("f_plus_1"))
    val union = Union(testRelation1, testRelation2)
    val query = union.join(complexRight, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    // Verify the optimization was applied (Union should be the root)
    assert(optimized.isInstanceOf[Union])
    // Verify no duplicate ExprIds across Union children's top-level output.
    // Each branch should have independent ExprIds for the right side.
    val childOutputs = optimized.asInstanceOf[Union].children.map(_.output)
    for (i <- childOutputs.indices; j <- (i + 1) until childOutputs.length) {
      val ids_i = childOutputs(i).map(_.exprId).toSet
      val ids_j = childOutputs(j).map(_.exprId).toSet
      assert(ids_i.intersect(ids_j).isEmpty,
        s"Union children $i and $j share ExprIds: ${ids_i.intersect(ids_j)}")
    }
  }

  test("Push down when right side contains Generate (Explode)") {
    val arrayRelation = LocalRelation($"k".int, $"arr".array(IntegerType))
    val rightWithGenerate = arrayRelation
      .generate(Explode($"arr"), outputNames = Seq("exploded_val"))
      .select($"k", $"exploded_val")
    val union = Union(testRelation1, testRelation2)
    val query = union.join(rightWithGenerate, Inner, Some($"a" === $"k"))
    val optimized = Optimize.execute(query.analyze)

    // Verify the optimization was applied
    assert(optimized.isInstanceOf[Union])
    // Verify no duplicate ExprIds across Union children's output
    val childOutputs = optimized.asInstanceOf[Union].children.map(_.output)
    for (i <- childOutputs.indices; j <- (i + 1) until childOutputs.length) {
      val ids_i = childOutputs(i).map(_.exprId).toSet
      val ids_j = childOutputs(j).map(_.exprId).toSet
      assert(ids_i.intersect(ids_j).isEmpty,
        s"Union children $i and $j share ExprIds: ${ids_i.intersect(ids_j)}")
    }
  }

  test("Push down when right side contains SubqueryAlias") {
    val rightWithAlias = testRelation3.subquery("dim")
    val union = Union(testRelation1, testRelation2)
    val query = union.join(rightWithAlias, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    // Verify the optimization was applied
    assert(optimized.isInstanceOf[Union])
    // Verify no duplicate ExprIds across Union children's output
    val childOutputs = optimized.asInstanceOf[Union].children.map(_.output)
    for (i <- childOutputs.indices; j <- (i + 1) until childOutputs.length) {
      val ids_i = childOutputs(i).map(_.exprId).toSet
      val ids_j = childOutputs(j).map(_.exprId).toSet
      assert(ids_i.intersect(ids_j).isEmpty,
        s"Union children $i and $j share ExprIds: ${ids_i.intersect(ids_j)}")
    }
  }

  test("Push down when right side contains Project with Alias") {
    val rightWithAlias = testRelation3
      .select($"e", ($"f" + 1).as("f_plus_1"))
    val union = Union(testRelation1, testRelation2)
    val query = union.join(rightWithAlias, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    assert(optimized.isInstanceOf[Union])
    val childOutputs = optimized.asInstanceOf[Union].children.map(_.output)
    for (i <- childOutputs.indices; j <- (i + 1) until childOutputs.length) {
      val ids_i = childOutputs(i).map(_.exprId).toSet
      val ids_j = childOutputs(j).map(_.exprId).toSet
      assert(ids_i.intersect(ids_j).isEmpty,
        s"Union children $i and $j share ExprIds: ${ids_i.intersect(ids_j)}")
    }
  }

  test("Push down when right side contains Aggregate") {
    val rightWithAgg = testRelation3
      .groupBy($"e")(count($"f").as("cnt"), $"e")
    val union = Union(testRelation1, testRelation2)
    val query = union.join(rightWithAgg, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    assert(optimized.isInstanceOf[Union])
    val childOutputs = optimized.asInstanceOf[Union].children.map(_.output)
    for (i <- childOutputs.indices; j <- (i + 1) until childOutputs.length) {
      val ids_i = childOutputs(i).map(_.exprId).toSet
      val ids_j = childOutputs(j).map(_.exprId).toSet
      assert(ids_i.intersect(ids_j).isEmpty,
        s"Union children $i and $j share ExprIds: ${ids_i.intersect(ids_j)}")
    }
  }

  test("Do not push down when right side contains non-deterministic expressions") {
    val rightWithRand = testRelation3
      .select($"e", Rand(10).as("rand_val"))
    val union = Union(testRelation1, testRelation2)
    val query = union.join(rightWithRand, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }
}
