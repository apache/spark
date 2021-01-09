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

import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf.CBO_ENABLED
import org.apache.spark.sql.types.{IntegerType, StringType}

class PredicateReorderSuite extends PlanTest with StatsEstimationTestBase with PredicateHelper
  with Matchers {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator Optimizations", FixedPoint(100),
        CombineFilters,
        PushPredicateThroughNonJoin,
        PushPredicateThroughJoin,
        ColumnPruning,
        CollapseProject) ::
      Batch("Predicate Reorder", FixedPoint(1),
        PredicateReorder) :: Nil
  }

  var originalConfCBOEnabled = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConfCBOEnabled = conf.cboEnabled
    conf.setConf(CBO_ENABLED, true)
  }

  override def afterAll(): Unit = {
    try {
      conf.setConf(CBO_ENABLED, originalConfCBOEnabled)
    } finally {
      super.afterAll()
    }
  }

  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("t1.k-1-2") -> rangeColumnStat(2, 0),
    attr("t1.v-1-10") -> rangeColumnStat(10, 0)
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  private val t1 = StatsTestPlan(
    outputList = Seq("t1.k-1-2", "t1.v-1-10").map(nameToAttr),
    rowCount = 1000,
    // size = rows * (overhead + column length)
    size = Some(1000 * (8 + 4 + 4)),
    attributeStats = AttributeMap(Seq("t1.k-1-2", "t1.v-1-10").map(nameToColInfo)))

  private def assertPredicatesOrder(
      originalCondition: Expression, expectedCondition: Expression) = {
    val originalPlan = Filter(originalCondition, t1)
    val expectedPlan = Filter(expectedCondition, t1)
    if (conf.cboEnabled) {
      val filterEstimation = FilterEstimation(originalPlan)
      val orderedPredicates = splitConjunctivePredicates(expectedCondition)
        .map(e => filterEstimation.calculateFilterSelectivity(e).getOrElse(1.0))
      assert(orderedPredicates.head <= orderedPredicates.last,
        "Predicates should be ordered by selectivity.")
      orderedPredicates shouldBe sorted
    }
    assert(normalizeExprIds(Optimize.execute(originalPlan)) === normalizeExprIds(expectedPlan))
  }

  test("reorder predicates by selectivity case 1") {
    val originalCondition = nameToAttr("t1.v-1-10") > 1 && nameToAttr("t1.v-1-10") > 2 &&
      nameToAttr("t1.v-1-10") > 8 && nameToAttr("t1.v-1-10") > 4
    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        if (cboEnabled) {
          assertPredicatesOrder(
            originalCondition,
            nameToAttr("t1.v-1-10") > 8 && nameToAttr("t1.v-1-10") > 4 &&
              nameToAttr("t1.v-1-10") > 2 && nameToAttr("t1.v-1-10") > 1)
        } else {
          assertPredicatesOrder(originalCondition, originalCondition)
        }
      }
    }
  }

  test("reorder predicates by selectivity case 2") {
    val originalCondition = nameToAttr("t1.k-1-2").isNotNull && nameToAttr("t1.k-1-2") === 1 &&
      nameToAttr("t1.v-1-10").isNotNull && nameToAttr("t1.v-1-10") > 5
    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        if (cboEnabled) {
          assertPredicatesOrder(
            originalCondition,
            nameToAttr("t1.k-1-2") === 1 && nameToAttr("t1.v-1-10") > 5 &&
              nameToAttr("t1.k-1-2").isNotNull && nameToAttr("t1.v-1-10").isNotNull)
        } else {
          assertPredicatesOrder(originalCondition, originalCondition)
        }
      }
    }
  }

  test("Should not reorder predicates if selectivity are same") {
    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        assertPredicatesOrder(
          nameToAttr("t1.k-1-2").isNull && nameToAttr("t1.k-1-2") === 10,
          nameToAttr("t1.k-1-2").isNull && nameToAttr("t1.k-1-2") === 10)
      }
    }
  }

  test("Reduce CaseWhen priority") {
    val caseWhen = CaseWhen(Seq(
      (nameToAttr("t1.k-1-2") > Literal(10)) -> (nameToAttr("t1.v-1-10") < Literal(1)),
      (nameToAttr("t1.k-1-2") > Literal(11)) -> (nameToAttr("t1.v-1-10") < Literal(2))))
    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        assertPredicatesOrder(
          caseWhen && nameToAttr("t1.k-1-2") > 0,
          nameToAttr("t1.k-1-2") > 0 && caseWhen)
      }
    }
  }

  test("Do not reduce CaseWhen priority if only one branch") {
    val caseWhen = CaseWhen(Seq(
      (nameToAttr("t1.k-1-2") > Literal(10)) -> (nameToAttr("t1.v-1-10") < Literal(1))))
    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        assertPredicatesOrder(
          caseWhen && nameToAttr("t1.k-1-2") > 0,
          caseWhen && nameToAttr("t1.k-1-2") > 0)
      }
    }
  }

  test("Reduce MultiLikeBase priority") {
    val likeAny = nameToAttr("t1.k-1-2").cast(StringType).likeAny(Literal("%1%"), Literal("%2%"))
    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        assertPredicatesOrder(
          likeAny && nameToAttr("t1.k-1-2") > 0,
          nameToAttr("t1.k-1-2") > 0 && likeAny)
      }
    }
  }

  test("Reduce SubqueryExpression priority") {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    import org.apache.spark.sql.catalyst.dsl.plans._
    val subPlan = table("t1")
      .where("v-1-10 > 1")
      .select(max("k-1-2"))
    val originPlan =
      table("t1")
        .where(ScalarSubquery(subPlan) === 1 && "k-1-2 >2")
        .select("v-1-10")
    val expectedPlan =
      table("t1")
        .where( "k-1-2 >2" && ScalarSubquery(subPlan) === 1)
        .select("v-1-10")

    withSQLConf(CBO_ENABLED.key -> false.toString) {
      assert(normalizeExprIds(Optimize.execute(originPlan)) === normalizeExprIds(expectedPlan))
    }
  }

  test("Should not reduce MultiLikeBase priority if only one expression") {
    val likeAll = nameToAttr("t1.k-1-2").cast(StringType).likeAll(Literal("%1%"))
    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        assertPredicatesOrder(
          likeAll && nameToAttr("t1.k-1-2") > 0,
          likeAll && nameToAttr("t1.k-1-2") > 0)
      }
    }
  }

  test("Reduce UserDefinedExpression priority") {
    val intUdf = ScalaUDF((i: Int) => i + 1, IntegerType, Literal(1) :: Nil,
      Option(ExpressionEncoder[Int]()) :: Nil)
    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        assertPredicatesOrder(
          nameToAttr("t1.k-1-2") > intUdf && nameToAttr("t1.k-1-2") > 0,
          nameToAttr("t1.k-1-2") > 0 && nameToAttr("t1.k-1-2") > intUdf)
      }
    }
  }

  test("Mixed case: UserDefinedExpression, MultiLikeBase, CaseWhen and binary predicates") {
    val intUdf = ScalaUDF((i: Int) => i + 1, IntegerType, Literal(1) :: Nil,
      Option(ExpressionEncoder[Int]()) :: Nil)
    val likeAny = nameToAttr("t1.k-1-2").cast(StringType)
      .likeAny(Literal("%1%"), Literal("%2%"), Literal("%3%"))
    val caseWhen = CaseWhen(Seq(
      (nameToAttr("t1.k-1-2") > Literal(10)) -> (nameToAttr("t1.v-1-10") < Literal(1)),
      (nameToAttr("t1.k-1-2") > Literal(11)) -> (nameToAttr("t1.v-1-10") < Literal(2))))

    val originalCondition = nameToAttr("t1.k-1-2") > intUdf && likeAny && caseWhen &&
      nameToAttr("t1.v-1-10") > 3 && nameToAttr("t1.v-1-10") > 7

    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        if (cboEnabled) {
          assertPredicatesOrder(
            originalCondition,
            nameToAttr("t1.v-1-10") > 7 && nameToAttr("t1.v-1-10") > 3 && caseWhen && likeAny &&
              nameToAttr("t1.k-1-2") > intUdf)
        } else {
          assertPredicatesOrder(
            originalCondition,
            nameToAttr("t1.v-1-10") > 3 && nameToAttr("t1.v-1-10") > 7 && caseWhen && likeAny &&
              nameToAttr("t1.k-1-2") > intUdf)
        }
      }
    }
  }

  test("Reorder disjunctive predicates") {
    val originalCondition = (nameToAttr("t1.v-1-10") > 1 && nameToAttr("t1.v-1-10") > 2) ||
      (nameToAttr("t1.v-1-10") > 8 && nameToAttr("t1.v-1-10") > 4)
    Seq(true, false).foreach { cboEnabled =>
      withSQLConf(CBO_ENABLED.key -> cboEnabled.toString) {
        if (cboEnabled) {
          assertPredicatesOrder(
            originalCondition,
            (nameToAttr("t1.v-1-10") > 2 && nameToAttr("t1.v-1-10") > 1) ||
              (nameToAttr("t1.v-1-10") > 8 && nameToAttr("t1.v-1-10") > 4))
        } else {
          assertPredicatesOrder(originalCondition, originalCondition)
        }
      }
    }
  }
}
