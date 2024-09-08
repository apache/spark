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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf._


class StarJoinCostBasedReorderSuite extends JoinReorderPlanTestBase with StatsEstimationTestBase {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
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
  var originalConfJoinReorderDPStarFilter = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConfCBOEnabled = conf.cboEnabled
    originalConfJoinReorderEnabled = conf.joinReorderEnabled
    originalConfJoinReorderDPStarFilter = conf.joinReorderDPStarFilter
    conf.setConf(CBO_ENABLED, true)
    conf.setConf(JOIN_REORDER_ENABLED, true)
    conf.setConf(JOIN_REORDER_DP_STAR_FILTER, true)
  }

  override def afterAll(): Unit = {
    try {
      conf.setConf(CBO_ENABLED, originalConfCBOEnabled)
      conf.setConf(JOIN_REORDER_ENABLED, originalConfJoinReorderEnabled)
      conf.setConf(JOIN_REORDER_DP_STAR_FILTER, originalConfJoinReorderDPStarFilter)
    } finally {
      super.afterAll()
    }
  }

  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    // F1 (fact table)
    attr("f1_fk1") -> rangeColumnStat(100, 0),
    attr("f1_fk2") -> rangeColumnStat(100, 0),
    attr("f1_fk3") -> rangeColumnStat(100, 0),
    attr("f1_c1") -> rangeColumnStat(100, 0),
    attr("f1_c2") -> rangeColumnStat(100, 0),

    // D1 (dimension)
    attr("d1_pk") -> rangeColumnStat(100, 0),
    attr("d1_c2") -> rangeColumnStat(50, 0),
    attr("d1_c3") -> rangeColumnStat(50, 0),

    // D2 (dimension)
    attr("d2_pk") -> rangeColumnStat(20, 0),
    attr("d2_c2") -> rangeColumnStat(10, 0),
    attr("d2_c3") -> rangeColumnStat(10, 0),

    // D3 (dimension)
    attr("d3_pk") -> rangeColumnStat(10, 0),
    attr("d3_c2") -> rangeColumnStat(5, 0),
    attr("d3_c3") -> rangeColumnStat(5, 0),

    // T1 (regular table i.e. outside star)
    attr("t1_c1") -> rangeColumnStat(20, 1),
    attr("t1_c2") -> rangeColumnStat(10, 1),
    attr("t1_c3") -> rangeColumnStat(10, 1),

    // T2 (regular table)
    attr("t2_c1") -> rangeColumnStat(5, 1),
    attr("t2_c2") -> rangeColumnStat(5, 1),
    attr("t2_c3") -> rangeColumnStat(5, 1),

    // T3 (regular table)
    attr("t3_c1") -> rangeColumnStat(5, 1),
    attr("t3_c2") -> rangeColumnStat(5, 1),
    attr("t3_c3") -> rangeColumnStat(5, 1),

    // T4 (regular table)
    attr("t4_c1") -> rangeColumnStat(5, 1),
    attr("t4_c2") -> rangeColumnStat(5, 1),
    attr("t4_c3") -> rangeColumnStat(5, 1),

    // T5 (regular table)
    attr("t5_c1") -> rangeColumnStat(5, 1),
    attr("t5_c2") -> rangeColumnStat(5, 1),
    attr("t5_c3") -> rangeColumnStat(5, 1),

    // T6 (regular table)
    attr("t6_c1") -> rangeColumnStat(5, 1),
    attr("t6_c2") -> rangeColumnStat(5, 1),
    attr("t6_c3") -> rangeColumnStat(5, 1)

  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  private val f1 = StatsTestPlan(
    outputList = Seq("f1_fk1", "f1_fk2", "f1_fk3", "f1_c1", "f1_c2").map(nameToAttr),
    rowCount = 1000,
    size = Some(1000 * (8 + 4 * 5)),
    attributeStats = AttributeMap(Seq("f1_fk1", "f1_fk2", "f1_fk3", "f1_c1", "f1_c2")
      .map(nameToColInfo)))

  // To control the layout of the join plans, keep the size for the non-fact tables constant
  // and vary the rowcount and the number of distinct values of the join columns.
  private val d1 = StatsTestPlan(
    outputList = Seq("d1_pk", "d1_c2", "d1_c3").map(nameToAttr),
    rowCount = 100,
    size = Some(3000),
    attributeStats = AttributeMap(Seq("d1_pk", "d1_c2", "d1_c3").map(nameToColInfo)))

  private val d2 = StatsTestPlan(
    outputList = Seq("d2_pk", "d2_c2", "d2_c3").map(nameToAttr),
    rowCount = 20,
    size = Some(3000),
    attributeStats = AttributeMap(Seq("d2_pk", "d2_c2", "d2_c3").map(nameToColInfo)))

  private val d3 = StatsTestPlan(
    outputList = Seq("d3_pk", "d3_c2", "d3_c3").map(nameToAttr),
    rowCount = 10,
    size = Some(3000),
    attributeStats = AttributeMap(Seq("d3_pk", "d3_c2", "d3_c3").map(nameToColInfo)))

  private val t1 = StatsTestPlan(
    outputList = Seq("t1_c1", "t1_c2", "t1_c3").map(nameToAttr),
    rowCount = 50,
    size = Some(3000),
    attributeStats = AttributeMap(Seq("t1_c1", "t1_c2", "t1_c3").map(nameToColInfo)))

  private val t2 = StatsTestPlan(
    outputList = Seq("t2_c1", "t2_c2", "t2_c3").map(nameToAttr),
    rowCount = 10,
    size = Some(3000),
    attributeStats = AttributeMap(Seq("t2_c1", "t2_c2", "t2_c3").map(nameToColInfo)))

  private val t3 = StatsTestPlan(
    outputList = Seq("t3_c1", "t3_c2", "t3_c3").map(nameToAttr),
    rowCount = 10,
    size = Some(3000),
    attributeStats = AttributeMap(Seq("t3_c1", "t3_c2", "t3_c3").map(nameToColInfo)))

  private val t4 = StatsTestPlan(
    outputList = Seq("t4_c1", "t4_c2", "t4_c3").map(nameToAttr),
    rowCount = 10,
    size = Some(3000),
    attributeStats = AttributeMap(Seq("t4_c1", "t4_c2", "t4_c3").map(nameToColInfo)))

  private val t5 = StatsTestPlan(
    outputList = Seq("t5_c1", "t5_c2", "t5_c3").map(nameToAttr),
    rowCount = 10,
    size = Some(3000),
    attributeStats = AttributeMap(Seq("t5_c1", "t5_c2", "t5_c3").map(nameToColInfo)))

  private val t6 = StatsTestPlan(
    outputList = Seq("t6_c1", "t6_c2", "t6_c3").map(nameToAttr),
    rowCount = 10,
    size = Some(3000),
    attributeStats = AttributeMap(Seq("t6_c1", "t6_c2", "t6_c3").map(nameToColInfo)))

  test("Test 1: Star query with two dimensions and two regular tables") {

    // d1     t1
    //   \   /
    //    f1
    //   /  \
    // d2    t2
    //
    // star: {f1, d1, d2}
    // non-star: {t1, t2}
    //
    // level 0: (t2 ), (d2 ), (f1 ), (d1 ), (t1 )
    // level 1: {f1 d1 }, {d2 f1 }
    // level 2: {d2 f1 d1 }
    // level 3: {t2 d1 d2 f1 }, {t1 d1 d2 f1 }
    // level 4: {f1 t1 t2 d1 d2 }
    //
    // Number of generated plans: 11 (vs. 20 w/o filter)
    val query =
      f1.join(t1).join(t2).join(d1).join(d2)
        .where((nameToAttr("f1_c1") === nameToAttr("t1_c1")) &&
          (nameToAttr("f1_c2") === nameToAttr("t2_c1")) &&
          (nameToAttr("f1_fk1") === nameToAttr("d1_pk")) &&
          (nameToAttr("f1_fk2") === nameToAttr("d2_pk")))

    val expected =
      f1.join(d2, Inner, Some(nameToAttr("f1_fk2") === nameToAttr("d2_pk")))
        .join(d1, Inner, Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk")))
        .join(t2, Inner, Some(nameToAttr("f1_c2") === nameToAttr("t2_c1")))
        .join(t1, Inner, Some(nameToAttr("f1_c1") === nameToAttr("t1_c1")))
        .select(outputsOf(f1, t1, t2, d1, d2): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 2: Star with a linear branch") {
    //
    //  t1   d1 - t2 - t3
    //   \  /
    //    f1
    //    |
    //    d2
    //
    // star: {d1, f1, d2}
    // non-star: {t2, t1, t3}
    //
    // level 0: (f1 ), (d2 ), (t3 ), (d1 ), (t1 ), (t2 )
    // level 1: {t3 t2 }, {f1 d2 }, {f1 d1 }
    // level 2: {d2 f1 d1 }
    // level 3: {t1 d1 f1 d2 }, {t2 d1 f1 d2 }
    // level 4: {d1 t2 f1 t1 d2 }, {d1 t3 t2 f1 d2 }
    // level 5: {d1 t3 t2 f1 t1 d2 }
    //
    // Number of generated plans: 15 (vs 24)
    val query =
      d1.join(t1).join(t2).join(f1).join(d2).join(t3)
        .where((nameToAttr("d1_pk") === nameToAttr("f1_fk1")) &&
          (nameToAttr("t1_c1") === nameToAttr("f1_c1")) &&
          (nameToAttr("d2_pk") === nameToAttr("f1_fk2")) &&
          (nameToAttr("f1_fk2") === nameToAttr("d2_pk")) &&
          (nameToAttr("d1_c2") === nameToAttr("t2_c1")) &&
          (nameToAttr("t2_c2") === nameToAttr("t3_c1")))

    val expected =
      f1.join(d2, Inner, Some(nameToAttr("d2_pk") === nameToAttr("f1_fk2")))
        .join(d1, Inner, Some(nameToAttr("d1_pk") === nameToAttr("f1_fk1")))
        .join(t1, Inner, Some(nameToAttr("t1_c1") === nameToAttr("f1_c1")))
        .join(t2.join(t3, Inner, Some(nameToAttr("t3_c1") === nameToAttr("t2_c2"))), Inner,
          Some(nameToAttr("d1_c2") === nameToAttr("t2_c1")))
        .select(outputsOf(d1, t1, t2, f1, d2, t3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 3: Star with derived branches") {
    //         t3   t2
    //         |    |
    //    d1 - t4 - t1
    //    |
    //    f1
    //    |
    //    d2
    //
    // star:  (d1 f1 d2 )
    // non-star: (t4 t1 t2 t3 )
    //
    // level 0: (t1 ), (t3 ), (f1 ), (d1 ), (t2 ), (d2 ), (t4 )
    // level 1: {f1 d2 }, {t1 t4 }, {t1 t2 }, {f1 d1 }, {t3 t4 }
    // level 2: {d1 f1 d2 }, {t2 t1 t4 }, {t1 t3 t4 }
    // level 3: {t4 d1 f1 d2 }, {t3 t4 t1 t2 }
    // level 4: {d1 f1 t4 d2 t3 }, {d1 f1 t4 d2 t1 }
    // level 5: {d1 f1 t4 d2 t1 t2 }, {d1 f1 t4 d2 t1 t3 }
    // level 6: {d1 f1 t4 d2 t1 t2 t3 }
    //
    // Number of generated plans: 22 (vs. 34)
    val query =
      d1.join(t1).join(t2).join(t3).join(t4).join(f1).join(d2)
        .where((nameToAttr("t1_c1") === nameToAttr("t2_c1")) &&
          (nameToAttr("t3_c1") === nameToAttr("t4_c1")) &&
          (nameToAttr("t1_c2") === nameToAttr("t4_c2")) &&
          (nameToAttr("d1_c2") === nameToAttr("t4_c3")) &&
          (nameToAttr("f1_fk1") === nameToAttr("d1_pk")) &&
          (nameToAttr("f1_fk2") === nameToAttr("d2_pk")))

    val expected =
      t4
        .join(t3, Inner, Some(nameToAttr("t4_c1") === nameToAttr("t3_c1")))
        .join(
          t1.join(t2, Inner, Some(nameToAttr("t2_c1") === nameToAttr("t1_c1"))),
          Inner, Some(nameToAttr("t4_c2") === nameToAttr("t1_c2")))
        .join(
          f1.join(d2, Inner, Some(nameToAttr("d2_pk") === nameToAttr("f1_fk2")))
            .join(d1, Inner, Some(nameToAttr("d1_pk") === nameToAttr("f1_fk1"))), Inner,
          Some(nameToAttr("d1_c2") === nameToAttr("t4_c3")))
        .select(outputsOf(d1, t1, t2, t3, t4, f1, d2): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 4: Star with several branches") {
    //
    //    d1 - t3 - t4
    //    |
    //    f1 - d3 - t1 - t2
    //    |
    //    d2 - t5 - t6
    //
    // star: {d1 f1 d2 d3 }
    // non-star: {t5 t3 t6 t2 t4 t1}
    //
    // level 0: (t4 ), (d2 ), (t5 ), (d3 ), (d1 ), (f1 ), (t2 ), (t6 ), (t1 ), (t3 )
    // level 1: {t5 t6 }, {t4 t3 }, {d3 f1 }, {t2 t1 }, {d2 f1 }, {d1 f1 }
    // level 2: {d2 d1 f1 }, {d2 d3 f1 }, {d3 d1 f1 }
    // level 3: {d2 d1 d3 f1 }
    // level 4: {d1 t3 d3 f1 d2 }, {d1 d3 f1 t1 d2 }, {d1 t5 d3 f1 d2 }
    // level 5: {d1 t5 d3 f1 t1 d2 }, {d1 t3 t4 d3 f1 d2 }, {d1 t5 t6 d3 f1 d2 },
    //          {d1 t5 t3 d3 f1 d2 }, {d1 t3 d3 f1 t1 d2 }, {d1 t2 d3 f1 t1 d2 }
    // level 6: {d1 t5 t3 t4 d3 f1 d2 }, {d1 t3 t2 d3 f1 t1 d2 }, {d1 t5 t6 d3 f1 t1 d2 },
    //          {d1 t5 t3 d3 f1 t1 d2 }, {d1 t5 t2 d3 f1 t1 d2 }, ...
    // ...
    // level 9: {d1 t5 t3 t6 t2 t4 d3 f1 t1 d2 }
    //
    // Number of generated plans: 46 (vs. 82)
    // TODO(SPARK-32687): find a way to make optimization result of `CostBasedJoinReorder`
    //  deterministic even if the input order is different.
    val query =
      d1.join(t3).join(t4).join(f1).join(d3).join(d2).join(t5).join(t6).join(t1).join(t2)
        .where((nameToAttr("d1_c2") === nameToAttr("t3_c1")) &&
          (nameToAttr("t3_c2") === nameToAttr("t4_c2")) &&
          (nameToAttr("d1_pk") === nameToAttr("f1_fk1")) &&
          (nameToAttr("f1_fk2") === nameToAttr("d2_pk")) &&
          (nameToAttr("d2_c2") === nameToAttr("t5_c1")) &&
          (nameToAttr("t5_c2") === nameToAttr("t6_c2")) &&
          (nameToAttr("f1_fk3") === nameToAttr("d3_pk")) &&
          (nameToAttr("d3_c2") === nameToAttr("t1_c1")) &&
          (nameToAttr("t1_c2") === nameToAttr("t2_c2")))

    val expected =
      f1.join(d2, Inner, Some(nameToAttr("d2_pk") === nameToAttr("f1_fk2")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("d3_pk")))
        .join(d1, Inner, Some(nameToAttr("d1_pk") === nameToAttr("f1_fk1")))
        .join(t5.join(t6, Inner, Some(nameToAttr("t6_c2") === nameToAttr("t5_c2"))), Inner,
          Some(nameToAttr("t5_c1") === nameToAttr("d2_c2")))
        .join(t3.join(t4, Inner, Some(nameToAttr("t4_c2") === nameToAttr("t3_c2"))), Inner,
          Some(nameToAttr("t3_c1") === nameToAttr("d1_c2")))
        .join(t1.join(t2, Inner, Some(nameToAttr("t2_c2") === nameToAttr("t1_c2"))), Inner,
          Some(nameToAttr("t1_c1") === nameToAttr("d3_c2")))
        .select(outputsOf(d1, t3, t4, f1, d3, d2, t5, t6, t1, t2): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 5: RI star only") {
    //    d1
    //    |
    //    f1
    //   /  \
    // d2    d3
    //
    // star: {f1, d1, d2, d3}
    // non-star: {}
    // level 0: (d1), (f1), (d2), (d3)
    // level 1: {f1 d3 }, {f1 d2 }, {d1 f1 }
    // level 2: {d1 f1 d2 }, {d2 f1 d3 }, {d1 f1 d3 }
    // level 3: {d1 d2 f1 d3 }
    // Number of generated plans: 11 (= 11)
    val query =
      d1.join(d2).join(f1).join(d3)
        .where((nameToAttr("f1_fk1") === nameToAttr("d1_pk")) &&
          (nameToAttr("f1_fk2") === nameToAttr("d2_pk")) &&
          (nameToAttr("f1_fk3") === nameToAttr("d3_pk")))

    val expected =
      f1.join(d3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("d3_pk")))
        .join(d2, Inner, Some(nameToAttr("f1_fk2") === nameToAttr("d2_pk")))
        .join(d1, Inner, Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk")))
        .select(outputsOf(d1, d2, f1, d3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 6: No RI star") {
    //
    // f1 - t1 - t2 - t3
    //
    // star: {}
    // non-star: {f1, t1, t2, t3}
    // level 0: (t1), (f1), (t2), (t3)
    // level 1: {f1 t3 }, {f1 t2 }, {t1 f1 }
    // level 2: {t1 f1 t2 }, {t2 f1 t3 }, {dt f1 t3 }
    // level 3: {t1 t2 f1 t3 }
    // Number of generated plans: 11 (= 11)
    val query =
      t1.join(f1).join(t2).join(t3)
        .where((nameToAttr("f1_fk1") === nameToAttr("t1_c1")) &&
          (nameToAttr("f1_fk2") === nameToAttr("t2_c1")) &&
          (nameToAttr("f1_fk3") === nameToAttr("t3_c1")))

    val expected =
      f1.join(t3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("t3_c1")))
        .join(t2, Inner, Some(nameToAttr("f1_fk2") === nameToAttr("t2_c1")))
        .join(t1, Inner, Some(nameToAttr("f1_fk1") === nameToAttr("t1_c1")))
        .select(outputsOf(t1, f1, t2, t3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }
}
