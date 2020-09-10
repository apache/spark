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
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf._

class StarJoinReorderSuite extends JoinReorderPlanTestBase with StatsEstimationTestBase {

  var originalConfStarSchemaDetection = false
  var originalConfCBOEnabled = true

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConfStarSchemaDetection = conf.starSchemaDetection
    originalConfCBOEnabled = conf.cboEnabled
    conf.setConf(STARSCHEMA_DETECTION, true)
    conf.setConf(CBO_ENABLED, false)
  }

  override def afterAll(): Unit = {
    try {
      conf.setConf(STARSCHEMA_DETECTION, originalConfStarSchemaDetection)
      conf.setConf(CBO_ENABLED, originalConfCBOEnabled)
    } finally {
      super.afterAll()
    }
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator Optimizations", FixedPoint(100),
        CombineFilters,
        PushPredicateThroughNonJoin,
        ReorderJoin,
        PushPredicateThroughJoin,
        ColumnPruning,
        CollapseProject) :: Nil
  }

  // Table setup using star schema relationships:
  //
  // d1 - f1 - d2
  //      |
  //      d3 - s3
  //
  // Table f1 is the fact table. Tables d1, d2, and d3 are the dimension tables.
  // Dimension d3 is further joined/normalized into table s3.
  // Tables' cardinality: f1 > d3 > d1 > d2 > s3
  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    // F1
    attr("f1_fk1") -> rangeColumnStat(3, 0),
    attr("f1_fk2") -> rangeColumnStat(3, 0),
    attr("f1_fk3") -> rangeColumnStat(4, 0),
    attr("f1_c4") -> rangeColumnStat(4, 0),
    // D1
    attr("d1_pk1") -> rangeColumnStat(4, 0),
    attr("d1_c2") -> rangeColumnStat(3, 0),
    attr("d1_c3") -> rangeColumnStat(4, 0),
    attr("d1_c4") -> ColumnStat(distinctCount = Some(2), min = Some("2"), max = Some("3"),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    // D2
    attr("d2_c2") -> ColumnStat(distinctCount = Some(3), min = Some("1"), max = Some("3"),
      nullCount = Some(1), avgLen = Some(4), maxLen = Some(4)),
    attr("d2_pk1") -> rangeColumnStat(3, 0),
    attr("d2_c3") -> rangeColumnStat(3, 0),
    attr("d2_c4") -> ColumnStat(distinctCount = Some(2), min = Some("3"), max = Some("4"),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    // D3
    attr("d3_fk1") -> rangeColumnStat(3, 0),
    attr("d3_c2") -> rangeColumnStat(3, 0),
    attr("d3_pk1") -> rangeColumnStat(5, 0),
    attr("d3_c4") -> ColumnStat(distinctCount = Some(2), min = Some("2"), max = Some("3"),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    // S3
    attr("s3_pk1") -> rangeColumnStat(2, 0),
    attr("s3_c2") -> rangeColumnStat(1, 0),
    attr("s3_c3") -> rangeColumnStat(1, 0),
    attr("s3_c4") -> ColumnStat(distinctCount = Some(2), min = Some("3"), max = Some("4"),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    // F11
    attr("f11_fk1") -> rangeColumnStat(3, 0),
    attr("f11_fk2") -> rangeColumnStat(3, 0),
    attr("f11_fk3") -> rangeColumnStat(4, 0),
    attr("f11_c4") -> rangeColumnStat(4, 0)
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  private val f1 = StatsTestPlan(
    outputList = Seq("f1_fk1", "f1_fk2", "f1_fk3", "f1_c4").map(nameToAttr),
    rowCount = 6,
    size = Some(48),
    attributeStats = AttributeMap(Seq("f1_fk1", "f1_fk2", "f1_fk3", "f1_c4").map(nameToColInfo)))

  private val d1 = StatsTestPlan(
    outputList = Seq("d1_pk1", "d1_c2", "d1_c3", "d1_c4").map(nameToAttr),
    rowCount = 4,
    size = Some(32),
    attributeStats = AttributeMap(Seq("d1_pk1", "d1_c2", "d1_c3", "d1_c4").map(nameToColInfo)))

  private val d2 = StatsTestPlan(
    outputList = Seq("d2_c2", "d2_pk1", "d2_c3", "d2_c4").map(nameToAttr),
    rowCount = 3,
    size = Some(24),
    attributeStats = AttributeMap(Seq("d2_c2", "d2_pk1", "d2_c3", "d2_c4").map(nameToColInfo)))

  private val d3 = StatsTestPlan(
    outputList = Seq("d3_fk1", "d3_c2", "d3_pk1", "d3_c4").map(nameToAttr),
    rowCount = 5,
    size = Some(40),
    attributeStats = AttributeMap(Seq("d3_fk1", "d3_c2", "d3_pk1", "d3_c4").map(nameToColInfo)))

  private val s3 = StatsTestPlan(
    outputList = Seq("s3_pk1", "s3_c2", "s3_c3", "s3_c4").map(nameToAttr),
    rowCount = 2,
    size = Some(17),
    attributeStats = AttributeMap(Seq("s3_pk1", "s3_c2", "s3_c3", "s3_c4").map(nameToColInfo)))

  private val d3_ns = LocalRelation('d3_fk1.int, 'd3_c2.int, 'd3_pk1.int, 'd3_c4.int)

  private val f11 = StatsTestPlan(
    outputList = Seq("f11_fk1", "f11_fk2", "f11_fk3", "f11_c4").map(nameToAttr),
    rowCount = 6,
    size = Some(48),
    attributeStats = AttributeMap(Seq("f11_fk1", "f11_fk2", "f11_fk3", "f11_c4")
      .map(nameToColInfo)))

  private val subq = d3.select(sum('d3_fk1).as('col))

  test("Test 1: Selective star-join on all dimensions") {
    // Star join:
    //   (=)  (=)
    // d1 - f1 - d2
    //      | (=)
    //      s3 - d3
    //
    // Query:
    //  select f1_fk1, f1_fk3
    //  from d1, d2, f1, d3, s3
    //  where f1_fk2 = d2_pk1 and d2_c2 < 2
    //  and f1_fk1 = d1_pk1
    //  and f1_fk3 = d3_pk1
    //  and d3_fk1 = s3_pk1
    //
    // Positional join reordering: d1, f1, d2, d3, s3
    // Star join reordering: f1, d2, d1, d3, s3
    val query =
      d1.join(d2).join(f1).join(d3).join(s3)
        .where((nameToAttr("f1_fk2") === nameToAttr("d2_pk1")) &&
          (nameToAttr("d2_c2") === 2) &&
          (nameToAttr("f1_fk1") === nameToAttr("d1_pk1")) &&
          (nameToAttr("f1_fk3") === nameToAttr("d3_pk1")) &&
          (nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))

    val expected =
      f1.join(d2.where(nameToAttr("d2_c2") === 2), Inner,
          Some(nameToAttr("f1_fk2") === nameToAttr("d2_pk1")))
        .join(d1, Inner, Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk1")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("d3_pk1")))
        .join(s3, Inner, Some(nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))
        .select(outputsOf(d1, d2, f1, d3, s3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 2: Star join on a subset of dimensions due to inequality joins") {
    // Star join:
    //   (=)  (<)
    // d1 - f1 - d2
    //      |
    //      | (=)
    //      d3 - s3
    //        (=)
    //
    // Query:
    //  select f1_fk1, f1_fk3
    //  from d1, f1, d2, s3, d3
    //  where f1_fk2 < d2_pk1
    //  and f1_fk1 = d1_pk1 and d1_c2 = 2
    //  and f1_fk3 = d3_pk1
    //  and d3_fk1 = s3_pk1
    //
    // Default join reordering: d1, f1, d2, d3, s3
    // Star join reordering: f1, d1, d3, d2, s3

    val query =
      d1.join(f1).join(d2).join(s3).join(d3)
        .where((nameToAttr("f1_fk2") < nameToAttr("d2_pk1")) &&
          (nameToAttr("f1_fk1") === nameToAttr("d1_pk1")) &&
          (nameToAttr("d1_c2") === 2) &&
          (nameToAttr("f1_fk3") === nameToAttr("d3_pk1")) &&
          (nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))

    val expected =
      f1.join(d1.where(nameToAttr("d1_c2") === 2), Inner,
          Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk1")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("d3_pk1")))
        .join(d2, Inner, Some(nameToAttr("f1_fk2") < nameToAttr("d2_pk1")))
        .join(s3, Inner, Some(nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))
        .select(outputsOf(d1, f1, d2, s3, d3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 3: Star join on a subset of dimensions since join column is not unique") {
    // Star join:
    //   (=)  (=)
    // d1 - f1 - d2
    //      | (=)
    //      d3 - s3
    //
    // Query:
    //  select f1_fk1, f1_fk3
    //  from d1, f1, d2, s3, d3
    //  where f1_fk2 = d2_c4
    //  and f1_fk1 = d1_pk1 and d1_c2 = 2
    //  and f1_fk3 = d3_pk1
    //  and d3_fk1 = s3_pk1
    //
    // Default join reordering: d1, f1, d2, d3, s3
    // Star join reordering: f1, d1, d3, d2, s3
    val query =
      d1.join(f1).join(d2).join(s3).join(d3)
        .where((nameToAttr("f1_fk1") === nameToAttr("d1_pk1")) &&
          (nameToAttr("d1_c2") === 2) &&
          (nameToAttr("f1_fk2") === nameToAttr("d2_c4")) &&
          (nameToAttr("f1_fk3") === nameToAttr("d3_pk1")) &&
          (nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))

    val expected =
      f1.join(d1.where(nameToAttr("d1_c2") === 2), Inner,
          Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk1")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("d3_pk1")))
        .join(d2, Inner, Some(nameToAttr("f1_fk2") === nameToAttr("d2_c4")))
        .join(s3, Inner, Some(nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))
        .select(outputsOf(d1, f1, d2, s3, d3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 4: Star join on a subset of dimensions since join column is nullable") {
    // Star join:
    //   (=)  (=)
    // d1 - f1 - d2
    //      | (=)
    //      s3 - d3
    //
    // Query:
    //  select f1_fk1, f1_fk3
    //  from d1, f1, d2, s3, d3
    //  where f1_fk2 = d2_c2
    //  and f1_fk1 = d1_pk1 and d1_c2 = 2
    //  and f1_fk3 = d3_pk1
    //  and d3_fk1 = s3_pk1
    //
    // Default join reordering: d1, f1, d2, d3, s3
    // Star join reordering: f1, d1, d3, d2, s3

    val query =
      d1.join(f1).join(d2).join(s3).join(d3)
        .where((nameToAttr("f1_fk1") === nameToAttr("d1_pk1")) &&
          (nameToAttr("d1_c2") === 2) &&
          (nameToAttr("f1_fk2") === nameToAttr("d2_c2")) &&
          (nameToAttr("f1_fk3") === nameToAttr("d3_pk1")) &&
          (nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))

    val expected =
      f1.join(d1.where(nameToAttr("d1_c2") === 2), Inner,
          Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk1")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("d3_pk1")))
        .join(d2, Inner, Some(nameToAttr("f1_fk2") === nameToAttr("d2_c2")))
        .join(s3, Inner, Some(nameToAttr("d3_fk1") < nameToAttr("s3_pk1")))
        .select(outputsOf(d1, f1, d2, s3, d3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 5: Table stats not available for some of the joined tables") {
    // Star join:
    //   (=)  (=)
    // d1 - f1 - d2
    //      | (=)
    //      d3_ns - s3
    //
    //  select f1_fk1, f1_fk3
    //  from d3_ns, f1, d1, d2, s3
    //  where f1_fk2 = d2_pk1 and d2_c2 = 2
    //  and f1_fk1 = d1_pk1
    //  and f1_fk3 = d3_pk1
    //  and d3_fk1 = s3_pk1
    //
    // Positional join reordering: d3_ns, f1, d1, d2, s3
    // Star join reordering: empty

    val d3_pk1 = d3_ns.output.find(_.name == "d3_pk1").get
    val d3_fk1 = d3_ns.output.find(_.name == "d3_fk1").get

    val query =
      d3_ns.join(f1).join(d1).join(d2).join(s3)
        .where((nameToAttr("f1_fk2") === nameToAttr("d2_pk1")) &&
          (nameToAttr("d2_c2") === 2) &&
          (nameToAttr("f1_fk1") === nameToAttr("d1_pk1")) &&
          (nameToAttr("f1_fk3") === d3_pk1) &&
          (d3_fk1 === nameToAttr("s3_pk1")))

    val equivQuery =
      d3_ns.join(f1, Inner, Some(nameToAttr("f1_fk3") === d3_pk1))
        .join(d1, Inner, Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk1")))
        .join(d2.where(nameToAttr("d2_c2") === 2), Inner,
          Some(nameToAttr("f1_fk2") === nameToAttr("d2_pk1")))
        .join(s3, Inner, Some(d3_fk1 === nameToAttr("s3_pk1")))

    assertEqualJoinPlans(Optimize, query, equivQuery)
  }

  test("Test 6: Join with complex plans") {
    // Star join:
    //   (=)  (=)
    // d1 - f1 - d2
    //      | (=)
    //      (sub-query)
    //
    //  select f1_fk1, f1_fk3
    //  from (select sum(d3_fk1) as col from d3) subq, f1, d1, d2
    //  where f1_fk2 = d2_pk1 and d2_c2 < 2
    //  and f1_fk1 = d1_pk1
    //  and f1_fk3 = sq.col
    //
    // Positional join reordering: d3, f1, d1, d2
    // Star join reordering: empty

    val query =
      subq.join(f1).join(d1).join(d2)
        .where((nameToAttr("f1_fk2") === nameToAttr("d2_pk1")) &&
          (nameToAttr("d2_c2") === 2) &&
          (nameToAttr("f1_fk1") === nameToAttr("d1_pk1")) &&
          (nameToAttr("f1_fk3") === "col".attr))

    val expected =
      d3.select('d3_fk1).select(sum('d3_fk1).as('col))
        .join(f1, Inner, Some(nameToAttr("f1_fk3") === "col".attr))
        .join(d1, Inner, Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk1")))
        .join(d2.where(nameToAttr("d2_c2") === 2), Inner,
          Some(nameToAttr("f1_fk2") === nameToAttr("d2_pk1")))

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 7: Comparable fact table sizes") {
    // Star join:
    //   (=)  (=)
    // d1 - f1 - d2
    //      | (=)
    //      f11 - s3
    //
    // select f1.f1_fk1, f1.f1_fk3
    // from d1, f11, f1, d2, s3
    // where f1.f1_fk2 = d2_pk1 and d2_c2 = 2
    // and f1.f1_fk1 = d1_pk1
    // and f1.f1_fk3 = f11.f1_fk3
    // and f11.f1_fk1 = s3_pk1
    //
    // Positional join reordering: d1, f1, f11, d2, s3
    // Star join reordering: empty

    val query =
      d1.join(f11).join(f1).join(d2).join(s3)
        .where((nameToAttr("f1_fk2") === nameToAttr("d2_pk1")) &&
          (nameToAttr("d2_c2") === 2) &&
          (nameToAttr("f1_fk1") === nameToAttr("d1_pk1")) &&
          (nameToAttr("f1_fk3") === nameToAttr("f11_fk3")) &&
          (nameToAttr("f11_fk1") === nameToAttr("s3_pk1")))

    val equivQuery =
      d1.join(f1, Inner, Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk1")))
        .join(f11, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("f11_fk3")))
        .join(d2.where(nameToAttr("d2_c2") === 2), Inner,
          Some(nameToAttr("f1_fk2") === nameToAttr("d2_pk1")))
        .join(s3, Inner, Some(nameToAttr("f11_fk1") === nameToAttr("s3_pk1")))
        .select(outputsOf(d1, f11, f1, d2, s3): _*)

    assertEqualJoinPlans(Optimize, query, equivQuery)
  }

  test("Test 8: No RI joins") {
    // Star join:
    //   (=)  (=)
    // d1 - f1 - d2
    //      | (=)
    //      d3 - s3
    //
    //  select f1_fk1, f1_fk3
    //  from d1, d3, f1, d2, s3
    //  where f1_fk2 = d2_c4 and d2_c2 = 2
    //  and f1_fk1 = d1_c4
    //  and f1_fk3 = d3_c4
    //  and d3_fk1 = s3_pk1
    //
    // Positional/default join reordering: d1, f1, d3, d2, s3
    // Star join reordering: empty

    val query =
      d1.join(d3).join(f1).join(d2).join(s3)
        .where((nameToAttr("f1_fk2") === nameToAttr("d2_c4")) &&
          (nameToAttr("d2_c2") === 2) &&
          (nameToAttr("f1_fk1") === nameToAttr("d1_c4")) &&
          (nameToAttr("f1_fk3") === nameToAttr("d3_c4")) &&
          (nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))

    val expected =
      d1.join(f1, Inner, Some(nameToAttr("f1_fk1") === nameToAttr("d1_c4")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("d3_c4")))
        .join(d2.where(nameToAttr("d2_c2") === 2), Inner,
          Some(nameToAttr("f1_fk2") === nameToAttr("d2_c4")))
        .join(s3, Inner, Some(nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))
        .select(outputsOf(d1, d3, f1, d2, s3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 9: Complex join predicates") {
    // Star join:
    //   (=)  (=)
    // d1 - f1 - d2
    //      | (=)
    //      d3 - s3
    //
    // select f1_fk1, f1_fk3
    // from d1, d3, f1, d2, s3
    // where f1_fk2 = d2_pk1 and d2_c2 = 2
    // and abs(f1_fk1) = d1_pk1
    // and f1_fk3 = d3_pk1
    // and d3_fk1 = s3_pk1
    //
    // Positional/default join reordering: d1, f1, d3, d2, s3
    // Star join reordering: empty

    val query =
      d1.join(d3).join(f1).join(d2).join(s3)
        .where((nameToAttr("f1_fk2") === nameToAttr("d2_pk1")) &&
          (nameToAttr("d2_c2") === 2) &&
          (abs(nameToAttr("f1_fk1")) === nameToAttr("d1_pk1")) &&
          (nameToAttr("f1_fk3") === nameToAttr("d3_pk1")) &&
          (nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))

    val expected =
      d1.join(f1, Inner, Some(abs(nameToAttr("f1_fk1")) === nameToAttr("d1_pk1")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("d3_pk1")))
        .join(d2.where(nameToAttr("d2_c2") === 2), Inner,
          Some(nameToAttr("f1_fk2") === nameToAttr("d2_pk1")))
        .join(s3, Inner, Some(nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))
        .select(outputsOf(d1, d3, f1, d2, s3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 10: Less than two dimensions") {
    // Star join:
    //   (<)  (=)
    // d1 - f1 - d2
    //      |(<)
    //      d3 - s3
    //
    // select f1_fk1, f1_fk3
    // from d1, d3, f1, d2, s3
    // where f1_fk2 = d2_pk1 and d2_c2 = 2
    // and f1_fk1 < d1_pk1
    // and f1_fk3 < d3_pk1
    //
    // Positional join reordering: d1, f1, d3, d2, s3
    // Star join reordering: empty

    val query =
      d1.join(d3).join(f1).join(d2).join(s3)
        .where((nameToAttr("f1_fk2") === nameToAttr("d2_pk1")) &&
          (nameToAttr("d2_c2") === 2) &&
          (nameToAttr("f1_fk1") < nameToAttr("d1_pk1")) &&
          (nameToAttr("f1_fk3") < nameToAttr("d3_pk1")) &&
          (nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))

    val expected =
      d1.join(f1, Inner, Some(nameToAttr("f1_fk1") < nameToAttr("d1_pk1")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") < nameToAttr("d3_pk1")))
        .join(d2.where(nameToAttr("d2_c2") === 2),
          Inner, Some(nameToAttr("f1_fk2") === nameToAttr("d2_pk1")))
        .join(s3, Inner, Some(nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))
        .select(outputsOf(d1, d3, f1, d2, s3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 11: Expanding star join") {
    // Star join:
    //   (<)  (<)
    // d1 - f1 - d2
    //      | (<)
    //      d3 - s3
    //
    // select f1_fk1, f1_fk3
    // from d1, d3, f1, d2, s3
    // where f1_fk2 < d2_pk1
    // and f1_fk1 < d1_pk1
    // and f1_fk3 < d3_pk1
    // and d3_fk1 < s3_pk1
    //
    // Positional join reordering: d1, f1, d3, d2, s3
    // Star join reordering: empty

    val query =
      d1.join(d3).join(f1).join(d2).join(s3)
        .where((nameToAttr("f1_fk2") < nameToAttr("d2_pk1")) &&
          (nameToAttr("f1_fk1") < nameToAttr("d1_pk1")) &&
          (nameToAttr("f1_fk3") < nameToAttr("d3_pk1")) &&
          (nameToAttr("d3_fk1") < nameToAttr("s3_pk1")))

    val expected =
      d1.join(f1, Inner, Some(nameToAttr("f1_fk1") < nameToAttr("d1_pk1")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") < nameToAttr("d3_pk1")))
        .join(d2, Inner, Some(nameToAttr("f1_fk2") < nameToAttr("d2_pk1")))
        .join(s3, Inner, Some(nameToAttr("d3_fk1") < nameToAttr("s3_pk1")))
        .select(outputsOf(d1, d3, f1, d2, s3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }

  test("Test 12: Non selective star join") {
    // Star join:
    //   (=)  (=)
    // d1 - f1 - d2
    //      | (=)
    //      d3 - s3
    //
    //  select f1_fk1, f1_fk3
    //  from d1, d3, f1, d2, s3
    //  where f1_fk2 = d2_pk1
    //  and f1_fk1 = d1_pk1
    //  and f1_fk3 = d3_pk1
    //  and d3_fk1 = s3_pk1
    //
    // Positional join reordering: d1, f1, d3, d2, s3
    // Star join reordering: empty

    val query =
      d1.join(d3).join(f1).join(d2).join(s3)
        .where((nameToAttr("f1_fk2") === nameToAttr("d2_pk1")) &&
          (nameToAttr("f1_fk1") === nameToAttr("d1_pk1")) &&
          (nameToAttr("f1_fk3") === nameToAttr("d3_pk1")) &&
          (nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))

    val expected =
      d1.join(f1, Inner, Some(nameToAttr("f1_fk1") === nameToAttr("d1_pk1")))
        .join(d3, Inner, Some(nameToAttr("f1_fk3") === nameToAttr("d3_pk1")))
        .join(d2, Inner, Some(nameToAttr("f1_fk2") === nameToAttr("d2_pk1")))
        .join(s3, Inner, Some(nameToAttr("d3_fk1") === nameToAttr("s3_pk1")))
        .select(outputsOf(d1, d3, f1, d2, s3): _*)

    assertEqualJoinPlans(Optimize, query, expected)
  }
}
