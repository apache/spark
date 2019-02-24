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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf.{CBO_ENABLED, JOIN_REORDER_ENABLED}


class JoinReorderSuite extends PlanTest with StatsEstimationTestBase {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Resolve Hints", Once,
        EliminateResolvedHint) ::
      Batch("Operator Optimizations", FixedPoint(100),
        CombineFilters,
        PushDownPredicate,
        ReorderJoin,
        PushPredicateThroughJoin,
        ColumnPruning,
        CollapseProject) ::
      Batch("Join Reorder", Once,
        CostBasedJoinReorder) :: Nil
  }

  object ResolveHints extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Resolve Hints", Once,
        EliminateResolvedHint) :: Nil
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

    assertEqualPlans(originalPlan, bestPlan)
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

    assertEqualPlans(originalPlan, bestPlan)
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

    assertEqualPlans(originalPlan, bestPlan)
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

    assertEqualPlans(originalPlan, bestPlan)
  }

  test("don't reorder if project contains non-attribute") {
    val originalPlan =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .select((nameToAttr("t1.k-1-2") + nameToAttr("t2.k-1-5")) as "key", nameToAttr("t1.v-1-10"))
        .join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .select("key".attr)

    assertEqualPlans(originalPlan, originalPlan)
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

    assertEqualPlans(originalPlan, bestPlan)
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
      assertEqualPlans(expectedPlan, expectedPlan)
    }
  }

  test("SPARK-26352: join reordering should not change the order of attributes") {
    // This test case does not rely on CBO.
    // It's similar to the test case above, but catches a reordering bug that the one above doesn't
    val tab1 = LocalRelation('x.int, 'y.int)
    val tab2 = LocalRelation('i.int, 'j.int)
    val tab3 = LocalRelation('a.int, 'b.int)
    val original =
      tab1.join(tab2, Cross)
          .join(tab3, Inner, Some('a === 'x && 'b === 'i))
    val expected =
      tab1.join(tab3, Inner, Some('a === 'x))
          .join(tab2, Cross, Some('b === 'i))
          .select(outputsOf(tab1, tab2, tab3): _*)

    assertEqualPlans(original, expected)
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

    assertEqualPlans(originalPlan, bestPlan)
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

    assertEqualPlans(originalPlan, originalPlan)

    val originalPlan2 =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .hint("broadcast")
        .join(t4, Inner, Some(nameToAttr("t4.v-1-10") === nameToAttr("t3.v-1-100")))
        .hint("broadcast")
        .join(t3, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t4.k-1-2")))

    assertEqualPlans(originalPlan2, originalPlan2)

    val originalPlan3 =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .join(t4).hint("broadcast")
        .join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .join(t5, Inner, Some(nameToAttr("t5.v-1-5") === nameToAttr("t3.v-1-100")))

    assertEqualPlans(originalPlan3, originalPlan3)
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

    assertEqualPlans(originalPlan, bestPlan)

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

    assertEqualPlans(originalPlan2, bestPlan2)

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

    assertEqualPlans(originalPlan3, bestPlan3)
  }

  private def assertEqualPlans(
      originalPlan: LogicalPlan,
      groundTruthBestPlan: LogicalPlan): Unit = {
    val analyzed = originalPlan.analyze
    val optimized = Optimize.execute(analyzed)
    val expected = ResolveHints.execute(groundTruthBestPlan.analyze)

    assert(analyzed.sameOutput(expected)) // if this fails, the expected plan itself is incorrect
    assert(analyzed.sameOutput(optimized))

    compareJoinOrder(optimized, expected)
  }

  private def outputsOf(plans: LogicalPlan*): Seq[Attribute] = {
    plans.map(_.output).reduce(_ ++ _)
  }
}
