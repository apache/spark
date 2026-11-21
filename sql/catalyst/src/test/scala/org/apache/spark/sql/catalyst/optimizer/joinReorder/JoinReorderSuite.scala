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

package org.apache.spark.sql.catalyst.optimizer.joinReorder

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Divide, EvalMode, Expression, If, IsNull, Literal, NumericEvalContext, RaiseError}
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.optimizer.JoinReorderDP.JoinPlan
import org.apache.spark.sql.catalyst.plans.{Cross, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf.{CBO_ENABLED, JOIN_REORDER_ENABLED}


class JoinReorderSuite extends JoinReorderPlanTestBase with StatsEstimationTestBase {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Resolve Hints", Once,
        EliminateResolvedHint) ::
      Batch("Operator Optimizations", FixedPoint(100),
        CombineFilters,
        PushPredicateThroughNonJoin,
        ReorderJoin,
        PushPredicateThroughJoin,
        ColumnPruning,
        CollapseProject) ::
      Batch("Join Reorder", FixedPoint(1),
        CostBasedJoinReorder) :: Nil
  }

  var originalConfCBOEnabled = false
  var originalConfJoinReorderEnabled = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConfCBOEnabled = conf.cboEnabled
    originalConfJoinReorderEnabled = conf.joinReorderEnabled
    conf.setConf(CBO_ENABLED, true)
    conf.setConf(JOIN_REORDER_ENABLED, true)
  }

  override def afterAll(): Unit = {
    try {
      conf.setConf(CBO_ENABLED, originalConfCBOEnabled)
      conf.setConf(JOIN_REORDER_ENABLED, originalConfJoinReorderEnabled)
    } finally {
      super.afterAll()
    }
  }

  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("t1.k-1-2") -> rangeColumnStat(2, 0),
    attr("t1.v-1-10") -> rangeColumnStat(10, 0),
    attr("t2.k-1-5") -> rangeColumnStat(5, 0),
    attr("t3.v-1-100") -> rangeColumnStat(100, 0),
    attr("t4.k-1-2") -> rangeColumnStat(2, 0),
    attr("t4.v-1-10") -> rangeColumnStat(10, 0),
    attr("t5.k-1-5") -> rangeColumnStat(5, 0),
    attr("t5.v-1-5") -> rangeColumnStat(5, 0)
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  // Table t1/t4: big table with two columns
  private val t1 = StatsTestPlan(
    outputList = Seq("t1.k-1-2", "t1.v-1-10").map(nameToAttr),
    rowCount = 1000,
    // size = rows * (overhead + column length)
    size = Some(1000 * (8 + 4 + 4)),
    attributeStats = AttributeMap(Seq("t1.k-1-2", "t1.v-1-10").map(nameToColInfo)))

  private val t4 = StatsTestPlan(
    outputList = Seq("t4.k-1-2", "t4.v-1-10").map(nameToAttr),
    rowCount = 2000,
    size = Some(2000 * (8 + 4 + 4)),
    attributeStats = AttributeMap(Seq("t4.k-1-2", "t4.v-1-10").map(nameToColInfo)))

  // Table t2/t3: small table with only one column
  private val t2 = StatsTestPlan(
    outputList = Seq("t2.k-1-5").map(nameToAttr),
    rowCount = 20,
    size = Some(20 * (8 + 4)),
    attributeStats = AttributeMap(Seq("t2.k-1-5").map(nameToColInfo)))

  private val t3 = StatsTestPlan(
    outputList = Seq("t3.v-1-100").map(nameToAttr),
    rowCount = 100,
    size = Some(100 * (8 + 4)),
    attributeStats = AttributeMap(Seq("t3.v-1-100").map(nameToColInfo)))

  // Table t5: small table with two columns
  private val t5 = StatsTestPlan(
    outputList = Seq("t5.k-1-5", "t5.v-1-5").map(nameToAttr),
    rowCount = 20,
    size = Some(20 * (8 + 4)),
    attributeStats = AttributeMap(Seq("t5.k-1-5", "t5.v-1-5").map(nameToColInfo)))

  test("reorder 3 tables") {
    val originalPlan =
      t1.join(t2).join(t3).where((nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")) &&
        (nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))

    // The cost of original plan (use only cardinality to simplify explanation):
    // cost = cost(t1 J t2) = 1000 * 20 / 5 = 4000
    // In contrast, the cost of the best plan:
    // cost = cost(t1 J t3) = 1000 * 100 / 100 = 1000 < 4000
    // so (t1 J t3) J t2 is better (has lower cost, i.e. intermediate result size) than
    // the original order (t1 J t2) J t3.
    val bestPlan =
      t1.join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(outputsOf(t1, t2, t3): _*)

    assertEqualJoinPlans(Optimize, originalPlan, bestPlan)
  }

  test("put unjoinable item at the end and reorder 3 joinable tables") {
    // The ReorderJoin rule puts the unjoinable item at the end, and then CostBasedJoinReorder
    // reorders other joinable items.
    val originalPlan =
      t1.join(t2).join(t4).join(t3).where((nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")) &&
        (nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))

    val bestPlan =
      t1.join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(outputsOf(t1, t2, t3): _*) // this is redundant but we'll take it for now
        .join(t4)
        .select(outputsOf(t1, t2, t4, t3): _*)

    assertEqualJoinPlans(Optimize, originalPlan, bestPlan)
  }

  test("reorder 3 tables with pure-attribute project") {
    val originalPlan =
      t1.join(t2).join(t3).where((nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")) &&
        (nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .select(nameToAttr("t1.v-1-10"))

    val bestPlan =
      t1.join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .select(nameToAttr("t1.k-1-2"), nameToAttr("t1.v-1-10"))
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(nameToAttr("t1.v-1-10"))

    assertEqualJoinPlans(Optimize, originalPlan, bestPlan)
  }

  test("reorder 3 tables - one of the leaf items is a project") {
    val originalPlan =
      t1.join(t5).join(t3).where((nameToAttr("t1.k-1-2") === nameToAttr("t5.k-1-5")) &&
        (nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .select(nameToAttr("t1.v-1-10"))

    // Items: t1, t3, project(t5.k-1-5, t5)
    val bestPlan =
      t1.join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .select(nameToAttr("t1.k-1-2"), nameToAttr("t1.v-1-10"))
        .join(t5.select(nameToAttr("t5.k-1-5")), Inner,
          Some(nameToAttr("t1.k-1-2") === nameToAttr("t5.k-1-5")))
        .select(nameToAttr("t1.v-1-10"))

    assertEqualJoinPlans(Optimize, originalPlan, bestPlan)
  }

  test("don't reorder if project contains non-attribute") {
    val originalPlan =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select((nameToAttr("t1.k-1-2") + nameToAttr("t2.k-1-5")) as "key", nameToAttr("t1.v-1-10"))
        .join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .select("key".attr)

    assertEqualJoinPlans(Optimize, originalPlan, originalPlan)
  }

  test("reorder 4 tables (bushy tree)") {
    val originalPlan =
      t1.join(t4).join(t2).join(t3).where((nameToAttr("t1.k-1-2") === nameToAttr("t4.k-1-2")) &&
        (nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")) &&
        (nameToAttr("t4.v-1-10") === nameToAttr("t3.v-1-100")))

    // The cost of original plan (use only cardinality to simplify explanation):
    // cost(t1 J t4) = 1000 * 2000 / 2 = 1000000, cost(t1t4 J t2) = 1000000 * 20 / 5 = 4000000,
    // cost = cost(t1 J t4) + cost(t1t4 J t2) = 5000000
    // In contrast, the cost of the best plan (a bushy tree):
    // cost(t1 J t2) = 1000 * 20 / 5 = 4000, cost(t4 J t3) = 2000 * 100 / 100 = 2000,
    // cost = cost(t1 J t2) + cost(t4 J t3) = 6000 << 5000000.
    val bestPlan =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .join(t4.join(t3, Inner, Some(nameToAttr("t4.v-1-10") === nameToAttr("t3.v-1-100"))),
          Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t4.k-1-2")))
        .select(outputsOf(t1, t4, t2, t3): _*)

    assertEqualJoinPlans(Optimize, originalPlan, bestPlan)
  }

  test("keep the order of attributes in the final output") {
    val outputLists = Seq("t1.k-1-2", "t1.v-1-10", "t3.v-1-100").permutations
    while (outputLists.hasNext) {
      val expectedOrder = outputLists.next().map(nameToAttr)
      val expectedPlan =
        t1.join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
          .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
          .select(expectedOrder: _*)
      // The plan should not change after optimization
      assertEqualJoinPlans(Optimize, expectedPlan, expectedPlan)
    }
  }

  test("SPARK-26352: join reordering should not change the order of attributes") {
    // This test case does not rely on CBO.
    // It's similar to the test case above, but catches a reordering bug that the one above doesn't
    val tab1 = LocalRelation($"x".int, $"y".int)
    val tab2 = LocalRelation($"i".int, $"j".int)
    val tab3 = LocalRelation($"a".int, $"b".int)
    val original =
      tab1.join(tab2, Cross)
          .join(tab3, Inner, Some($"a" === $"x" && $"b" === $"i"))
    val expected =
      tab1.join(tab3, Inner, Some($"a" === $"x"))
          .join(tab2, Cross, Some($"b" === $"i"))
          .select(outputsOf(tab1, tab2, tab3): _*)

    assertEqualJoinPlans(Optimize, original, expected)
  }

  test("reorder recursively") {
    // Original order:
    //          Join
    //          / \
    //      Union  t5
    //       / \
    //     Join t4
    //     / \
    //   Join t3
    //   / \
    //  t1  t2
    val bottomJoins =
      t1.join(t2).join(t3).where((nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")) &&
        (nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .select(nameToAttr("t1.v-1-10"))

    val originalPlan = bottomJoins
      .union(t4.select(nameToAttr("t4.v-1-10")))
      .join(t5, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t5.v-1-5")))

    // Should be able to reorder the bottom part.
    // Best order:
    //          Join
    //          / \
    //      Union  t5
    //       / \
    //     Join t4
    //     / \
    //   Join t2
    //   / \
    //  t1  t3
    val bestBottomPlan =
      t1.join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .select(nameToAttr("t1.k-1-2"), nameToAttr("t1.v-1-10"))
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(nameToAttr("t1.v-1-10"))

    val bestPlan = bestBottomPlan
      .union(t4.select(nameToAttr("t4.v-1-10")))
      .join(t5, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t5.v-1-5")))

    assertEqualJoinPlans(Optimize, originalPlan, bestPlan)
  }

  test("don't reorder if hints present") {
    val originalPlan =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .hint("broadcast")
        .join(
          t4.join(t3, Inner, Some(nameToAttr("t4.v-1-10") === nameToAttr("t3.v-1-100")))
            .hint("broadcast"),
          Inner,
          Some(nameToAttr("t1.k-1-2") === nameToAttr("t4.k-1-2")))

    assertEqualJoinPlans(Optimize, originalPlan, originalPlan)

    val originalPlan2 =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .hint("broadcast")
        .join(t4, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t4.k-1-2")))
        .hint("broadcast")
        .join(t3, Inner, Some(nameToAttr("t4.v-1-10") === nameToAttr("t3.v-1-100")))

    assertEqualJoinPlans(Optimize, originalPlan2, originalPlan2)

    val originalPlan3 =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .join(t4).hint("broadcast")
        .join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t5, Inner, Some(nameToAttr("t5.v-1-5") === nameToAttr("t3.v-1-100")))

    assertEqualJoinPlans(Optimize, originalPlan3, originalPlan3)
  }

  test("reorder below and above the hint node") {
    val originalPlan =
      t1.join(t2).join(t3)
        .where((nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")) &&
          (nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .hint("broadcast").join(t4)

    val bestPlan =
    t1.join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
      .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
      .select(outputsOf(t1, t2, t3): _*)
      .hint("broadcast").join(t4)

    assertEqualJoinPlans(Optimize, originalPlan, bestPlan)

    val originalPlan2 =
      t1.join(t2).join(t3)
        .where((nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")) &&
          (nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t4.hint("broadcast"))

    val bestPlan2 =
      t1.join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(outputsOf(t1, t2, t3): _*)
        .join(t4.hint("broadcast"))

    assertEqualJoinPlans(Optimize, originalPlan2, bestPlan2)

    val originalPlan3 =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .hint("broadcast")
        .join(t4, Inner, Some(nameToAttr("t4.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t5, Inner, Some(nameToAttr("t5.v-1-5") === nameToAttr("t3.v-1-100")))

    val bestPlan3 =
      t1.join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(outputsOf(t1, t2, t3): _*)
        .hint("broadcast")
        .join(t4, Inner, Some(nameToAttr("t4.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t5, Inner, Some(nameToAttr("t5.v-1-5") === nameToAttr("t3.v-1-100")))

    assertEqualJoinPlans(Optimize, originalPlan3, bestPlan3)
  }

  test("SPARK-33935: betterThan should be consistent") {
    val plan1 = JoinPlan(null, null, null, Cost(300, 80))
    val plan2 = JoinPlan(null, null, null, Cost(500, 30))

    assert(!plan1.betterThan(plan1, conf))
    assert(!plan2.betterThan(plan2, conf))

    assert(plan1.betterThan(plan2, conf))
    assert(!plan2.betterThan(plan1, conf))
  }

  test("SPARK-34354: join reorder with self-join") {
    val plan = t2.join(t1, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
      .select(nameToAttr("t1.v-1-10"))
      .join(t2, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t2.k-1-5")))

    // this can fail before the fix
    Optimize.execute(plan.analyze)
  }

  private def raiseUnless(cond: Expression): Expression =
    If(cond, Literal(true), IsNull(RaiseError(Literal("boom"))))

  // A division in ANSI mode, where it can fail.
  private def divide(l: Expression, r: Expression): Expression =
    Divide(l, r, NumericEvalContext(EvalMode.ANSI))

  private def attrs(names: String*): Seq[Attribute] = names.map(nameToAttr)

  // The conjuncts of the top filter in their written order, which `comparePlans` does not check.
  private def topFilterConjuncts(plan: LogicalPlan): Seq[Expression] =
    normalizeExprIds(plan).collectFirst { case Filter(condition, _) => condition }
      .map(splitConjunctivePredicates(_).map(_.canonicalized)).getOrElse(Nil)

  test("SPARK-62054: put the conditions that no join may take in a filter above the joins") {
    // The project, which prunes t2.k-1-5, keeps ReorderJoin from flattening the joins. No join
    // takes overT1, which is over t1 only, nor overT1T3 and the conditions after it, which may
    // not move: overT1T3 is throwable, the division can fail, and so can `q`, which overQ reads.
    // They would otherwise be evaluated on rows that the join with t2 removes, or on which
    // overT1T3 raises its error. The filter is above all the joins, so none of them is a join
    // key, and it reads columns that the query does not output.
    val x3 = t3.select(nameToAttr("t3.v-1-100"),
      divide(Literal(100), nameToAttr("t3.v-1-100")).as("q"))
    val overT1 = raiseUnless(nameToAttr("t1.k-1-2") > 0)
    val overT1T3 = raiseUnless(nameToAttr("t1.v-1-10") + nameToAttr("t3.v-1-100") > 0)
    val failable = divide(nameToAttr("t1.v-1-10"), nameToAttr("t3.v-1-100")) > 0
    val overQ = nameToAttr("t1.k-1-2") < $"q"
    val originalPlan =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(nameToAttr("t1.k-1-2"), nameToAttr("t1.v-1-10"))
        .join(x3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100") &&
          overT1 && overT1T3 && failable && overQ))
        .select(nameToAttr("t3.v-1-100"))

    val bestPlan =
      t1.join(x3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .where(overT1 && overT1T3 && failable && overQ)
        .select(nameToAttr("t3.v-1-100"))

    val optimized = Optimize.execute(originalPlan.analyze)
    comparePlans(optimized, bestPlan.analyze)
    assert(topFilterConjuncts(optimized) === topFilterConjuncts(bestPlan.analyze))
  }

  test("SPARK-62054: keep a lower join with a condition that may not move as one item") {
    // The lower join removes a row on which its condition is NULL, so no condition above it is
    // evaluated on the row. It is one item, which is joined with t3 before t5, and its own joins
    // are reordered by themselves, with its condition that may not move in a filter above them.
    // The projects, which prune t4.k-1-2 and t5.v-1-5, keep ReorderJoin from flattening the joins.
    val throwing = raiseUnless(nameToAttr("t1.v-1-10") > nameToAttr("t2.k-1-5"))
    val lowerOutput = attrs("t1.k-1-2", "t1.v-1-10", "t4.v-1-10", "t2.k-1-5")
    val originalPlan =
      t1.join(t4, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t4.v-1-10")))
        .select(attrs("t1.k-1-2", "t1.v-1-10", "t4.v-1-10"): _*)
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5") && throwing))
        .join(t5, Inner, Some(nameToAttr("t5.k-1-5") === nameToAttr("t2.k-1-5")))
        .select(lowerOutput ++ attrs("t5.k-1-5"): _*)
        .join(t3, Inner, Some(nameToAttr("t3.v-1-100") === nameToAttr("t1.v-1-10")))

    val bestPlan =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .join(t4.select(attrs("t4.v-1-10"): _*), Inner,
          Some(nameToAttr("t1.v-1-10") === nameToAttr("t4.v-1-10")))
        .where(throwing)
        .select(lowerOutput: _*)
        .join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t5.select(attrs("t5.k-1-5"): _*), Inner,
          Some(nameToAttr("t5.k-1-5") === nameToAttr("t2.k-1-5")))
        .select(lowerOutput ++ attrs("t5.k-1-5", "t3.v-1-100"): _*)

    comparePlans(Optimize.execute(originalPlan.analyze), bestPlan.analyze)
  }

  test("SPARK-62054: keep a lower join with a condition that may not move on the right side as " +
    "one item") {
    // ReorderJoin flattens only the left side of a join. The lower join, under a project of
    // columns, is one item on the right side of the join with t5, and its own joins are reordered
    // by themselves, with its condition that may not move in a filter above them.
    val throwing = raiseUnless(nameToAttr("t1.v-1-10") > nameToAttr("t2.k-1-5"))
    val lowerOutput = attrs("t1.v-1-10", "t4.v-1-10", "t2.k-1-5")
    val originalPlan = t5.join(
      t1.join(t4, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t4.v-1-10")))
        .select(attrs("t1.k-1-2", "t1.v-1-10", "t4.v-1-10"): _*)
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5") && throwing))
        .select(lowerOutput: _*),
      Inner, Some(nameToAttr("t5.k-1-5") === nameToAttr("t2.k-1-5")))

    val bestPlan = t5.join(
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(attrs("t1.v-1-10", "t2.k-1-5"): _*)
        .join(t4.select(attrs("t4.v-1-10"): _*), Inner,
          Some(nameToAttr("t1.v-1-10") === nameToAttr("t4.v-1-10")))
        .where(throwing)
        .select(lowerOutput: _*),
      Inner, Some(nameToAttr("t5.k-1-5") === nameToAttr("t2.k-1-5")))

    comparePlans(Optimize.execute(originalPlan.analyze), bestPlan.analyze)
  }

  test("SPARK-62054: keep only the top join in place if the join conditions do not connect all " +
    "items") {
    // The only condition of the join with t3 is throwable, so no join can take t3, and the top
    // join stays with its condition. The joins of its side with t1, t4 and t2, which the project
    // keeps ReorderJoin from flattening, are reordered by themselves, on either side.
    val throwing = raiseUnless(nameToAttr("t1.v-1-10") + nameToAttr("t3.v-1-100") > 0)
    val side =
      t1.join(t4, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t4.v-1-10")))
        .select(attrs("t1.k-1-2", "t1.v-1-10", "t4.v-1-10"): _*)
        .join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(nameToAttr("t1.v-1-10"))
    val reorderedSide =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select(nameToAttr("t1.v-1-10"))
        .join(t4.select(attrs("t4.v-1-10"): _*), Inner,
          Some(nameToAttr("t1.v-1-10") === nameToAttr("t4.v-1-10")))
        .select(nameToAttr("t1.v-1-10"))

    Seq(
      side.join(t3, Inner, Some(throwing)) -> reorderedSide.join(t3, Inner, Some(throwing)),
      t3.join(side, Inner, Some(throwing)) -> t3.join(reorderedSide, Inner, Some(throwing))
    ).foreach { case (originalPlan, bestPlan) =>
      comparePlans(Optimize.execute(originalPlan.analyze), bestPlan.analyze)
    }
  }
}
