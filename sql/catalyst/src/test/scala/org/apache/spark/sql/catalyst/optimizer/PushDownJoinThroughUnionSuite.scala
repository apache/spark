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
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, Explode, Rand}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, HintInfo, Join, JoinHint, LocalRelation,
  LogicalPlan, SHUFFLE_HASH, SHUFFLE_MERGE, SHUFFLE_REPLICATE_NL, Union}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
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

  // A Union whose branches are far too large to broadcast. The 0-byte empty LocalRelations above
  // cannot express that: any non-leaf right side is estimated at 1 byte at least, so
  // `getSmallerSide` picks the 0-byte left one and the rule does not fire. A `def` so that every
  // test gets fresh ExprIds.
  private def largeStatsUnion = Union(
    StatsTestPlan(Seq($"a".int, $"b".int), 1000000, AttributeMap.empty,
      Some(100 * 1024 * 1024)),
    StatsTestPlan(Seq($"c".int, $"d".int), 1000000, AttributeMap.empty,
      Some(100 * 1024 * 1024)))

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
    val union = largeStatsUnion
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
    val union = largeStatsUnion
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
    val union = largeStatsUnion
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
    val union = largeStatsUnion
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
    val union = largeStatsUnion
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
    val union = largeStatsUnion
    val query = union.join(rightWithRand, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("SPARK-58449: do not push down Inner Join when only the Union side is broadcastable") {
    // For an inner join the planner may broadcast either side, so a broadcastable Union on the
    // left is enough to make the join a broadcast hash join. Pushing down then clones the large
    // right side once per branch, and nothing reuses those scans, so the right side is read N
    // times instead of once.
    val smallUnion = Union(
      StatsTestPlan(Seq($"a".int, $"b".int), 10, AttributeMap.empty, Some(100)),
      StatsTestPlan(Seq($"c".int, $"d".int), 10, AttributeMap.empty, Some(100)))
    val largeRight = StatsTestPlan(Seq($"e".int, $"f".int), 1000000, AttributeMap.empty,
      Some(100 * 1024 * 1024))

    val query = smallUnion.join(largeRight, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("SPARK-58449: push down Inner Join when the right side is broadcastable") {
    // Control for the case above: the same shape with a small right side is still pushed down.
    val smallRight = StatsTestPlan(Seq($"e".int, $"f".int), 10, AttributeMap.empty, Some(100))

    val query = largeStatsUnion.join(smallRight, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    val union = optimized.asInstanceOf[Union]
    assert(union.children.size == 2)
    assert(union.children.forall(_.isInstanceOf[Join]))
  }

  test("SPARK-58449: do not push down when the right side is the build side only for the Union") {
    // The right side is smaller than the whole Union but larger than either branch, so the join
    // broadcasts the right side before the rewrite and would build from the left after it. A guard
    // that checks the whole join instead of each branch misses this.
    val union = Union(
      StatsTestPlan(Seq($"a".int, $"b".int), 10, AttributeMap.empty, Some(400)),
      StatsTestPlan(Seq($"c".int, $"d".int), 10, AttributeMap.empty, Some(400)))
    val right = StatsTestPlan(Seq($"e".int, $"f".int), 10, AttributeMap.empty, Some(500))

    val query = union.join(right, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("SPARK-58449: push down Left Outer Join when the right side is the build side only for " +
    "the Union") {
    // Only an inner join can build from the left, so a left outer join broadcasts the right side
    // whatever the sizes are. The shape that blocks the inner join above is still pushed down.
    val union = Union(
      StatsTestPlan(Seq($"a".int, $"b".int), 10, AttributeMap.empty, Some(400)),
      StatsTestPlan(Seq($"c".int, $"d".int), 10, AttributeMap.empty, Some(400)))
    val right = StatsTestPlan(Seq($"e".int, $"f".int), 10, AttributeMap.empty, Some(500))

    val query = union.join(right, LeftOuter, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    val optimizedUnion = optimized.asInstanceOf[Union]
    assert(optimizedUnion.children.size == 2)
    assert(optimizedUnion.children.forall(_.isInstanceOf[Join]))
  }

  test("SPARK-58449: push down when a broadcast hint names the right side") {
    // A hint is honored ahead of the sizes, so the right side is the build side even though it is
    // the larger one.
    val largeRight = StatsTestPlan(Seq($"e".int, $"f".int), 1000000, AttributeMap.empty,
      Some(100 * 1024 * 1024))
    val hint = JoinHint(None, Some(HintInfo(Some(BROADCAST))))

    val query = Join(largeStatsUnion, largeRight, Inner, Some($"a" === $"e"), hint)
    val optimized = Optimize.execute(query.analyze)

    val union = optimized.asInstanceOf[Union]
    assert(union.children.size == 2)
    assert(union.children.forall(_.isInstanceOf[Join]))
  }

  test("SPARK-58449: do not push down when a hint picks a non-broadcast strategy") {
    // The planner honors the merge hint over a size-based broadcast, so no branch would broadcast
    // the right side and the rewrite would only multiply the joins.
    val smallRight = StatsTestPlan(Seq($"e".int, $"f".int), 10, AttributeMap.empty, Some(100))
    val hint = JoinHint(None, Some(HintInfo(Some(SHUFFLE_MERGE))))

    val query = Join(largeStatsUnion, smallRight, Inner, Some($"a" === $"e"), hint)
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("SPARK-58449: do not push down when a hint asks to replicate the right side") {
    // A cartesian product requires no distribution on either side, so nothing would broadcast and
    // nothing would put the duplicated right side behind a reusable exchange.
    val smallRight = StatsTestPlan(Seq($"e".int, $"f".int), 10, AttributeMap.empty, Some(100))
    val hint = JoinHint(None, Some(HintInfo(Some(SHUFFLE_REPLICATE_NL))))

    val query = Join(largeStatsUnion, smallRight, Inner, Some($"a" === $"e"), hint)
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("SPARK-58449: do not push down when a shuffle hash hint decided the strategy") {
    // The co-guard rejects the join once a shuffle hash hint applies, so the rewrite never gets to
    // ask about the build side. Nothing else covers that delegation.
    val smallRight = StatsTestPlan(Seq($"e".int, $"f".int), 10, AttributeMap.empty, Some(100))
    val hint = JoinHint(None, Some(HintInfo(Some(SHUFFLE_HASH))))

    val query = Join(largeStatsUnion, smallRight, Inner, Some($"a" === $"e"), hint)
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("SPARK-58449: do not push down when only some branches would broadcast the right side") {
    // The first branch is far too large to build from, the second is smaller than the right side.
    // The rewrite is all or nothing: the second branch would probe its own copy of the right side,
    // so neither branch is pushed down.
    val union = Union(
      StatsTestPlan(Seq($"a".int, $"b".int), 1000000, AttributeMap.empty,
        Some(100 * 1024 * 1024)),
      StatsTestPlan(Seq($"c".int, $"d".int), 10, AttributeMap.empty, Some(100)))
    val right = StatsTestPlan(Seq($"e".int, $"f".int), 10, AttributeMap.empty, Some(500))

    val query = union.join(right, Inner, Some($"a" === $"e"))
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }

  test("SPARK-58449: a broadcast hint outranks a non-broadcast hint on the other side") {
    // The planner tries a hinted broadcast before the merge hint, so the right side is still the
    // build side and the rewrite applies.
    val largeRight = StatsTestPlan(Seq($"e".int, $"f".int), 1000000, AttributeMap.empty,
      Some(100 * 1024 * 1024))
    val hint = JoinHint(Some(HintInfo(Some(SHUFFLE_MERGE))), Some(HintInfo(Some(BROADCAST))))

    val query = Join(largeStatsUnion, largeRight, Inner, Some($"a" === $"e"), hint)
    val optimized = Optimize.execute(query.analyze)

    val union = optimized.asInstanceOf[Union]
    assert(union.children.size == 2)
    assert(union.children.forall(_.isInstanceOf[Join]))
  }
}
