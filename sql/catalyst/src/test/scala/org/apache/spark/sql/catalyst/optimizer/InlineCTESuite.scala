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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.TestRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualTo, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.{OuterReference, OuterScopeReference, Rand}
import org.apache.spark.sql.catalyst.expressions.{RowOrderIndepTestExpr, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CTERelationDef, CTERelationRef}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project, Union, WithCTE}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class InlineCTESuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("inline CTE", FixedPoint(100), InlineCTE()) :: Nil
  }

  test("SPARK-48307: not-inlined CTE relation in command") {
    val cteDef = CTERelationDef(OneRowRelation().select(rand(0).as("a")))
    val cteRef = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val plan = AppendData.byName(
      TestRelation(Seq($"a".double)),
      WithCTE(cteRef.except(cteRef, isAll = true), Seq(cteDef))
    ).analyze
    comparePlans(Optimize.execute(plan), plan)
  }

  test("SPARK-52818: ReplaceCTERefWithRepartition asserts on orphaned CTERelationRef") {
    val cteDef = CTERelationDef(OneRowRelation().select(rand(0).as("a")))
    val cteRef = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    // A CTERelationRef without an enclosing WithCTE should produce a clear error.
    val e = intercept[SparkException] {
      ReplaceCTERefWithRepartition(cteRef.select($"a"))
    }
    assert(e.getMessage.contains("No CTERelationDef found"))
  }

  test("SPARK-58006: forceSkipInline keeps a single-reference deterministic CTE") {
    // A single-reference deterministic CTE is normally inlined, but `forceSkipInline` should
    // keep it materialized in the `WithCTE` node.
    val cteDef = CTERelationDef(
      TestRelation(Seq($"a".int)).select($"a"), forceSkipInline = true)
    val cteRef = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val plan = WithCTE(cteRef.select($"a"), Seq(cteDef)).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.collectFirst { case _: WithCTE => true }.isDefined,
      "CTE with forceSkipInline should not be inlined")
  }

  test("SPARK-58006: forceSkipInline is inlined normally when not set") {
    // The same single-reference deterministic CTE is inlined when `forceSkipInline` is false.
    val cteDef = CTERelationDef(TestRelation(Seq($"a".int)).select($"a"))
    val cteRef = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val plan = WithCTE(cteRef.select($"a"), Seq(cteDef)).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.collectFirst { case _: WithCTE => true }.isEmpty,
      "CTE without forceSkipInline should be inlined")
  }

  test("SPARK-58006: forceSkipInline CTE with an outer reference across its boundary fails") {
    // A force-materialized CTE cannot carry an outer reference across its boundary, because after
    // materialization there is no surrounding operator to resolve it against.
    val relation = TestRelation(Seq($"a".int))
    val cteChild = relation.where(OuterReference($"a".int) === 1)
    val cteDef = CTERelationDef(cteChild, forceSkipInline = true)
    val cteRef = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val plan = WithCTE(cteRef.select($"a"), Seq(cteDef))
    val e = intercept[SparkException] {
      Optimize.execute(plan)
    }
    assert(e.getCondition == "INTERNAL_ERROR")
    assert(e.getMessage.contains(
      "A force-materialized CTE cannot carry an outer reference across its boundary"))
  }

  test("SPARK-58006: forceSkipInline CTE with an outer-scope subquery reference fails") {
    // Exercises the second validation branch: a subquery inside the CTE child carries an
    // outer-scope reference (nested correlation), which also cannot cross the CTE boundary.
    val relation = TestRelation(Seq($"a".int))
    val subquery = ScalarSubquery(
      TestRelation(Seq($"c".int)).select($"c"),
      outerAttrs = Seq(OuterScopeReference($"a".int)))
    val cteChild = relation.select(subquery.as("s"))
    val cteDef = CTERelationDef(cteChild, forceSkipInline = true)
    val cteRef = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val plan = WithCTE(cteRef.select($"s"), Seq(cteDef))
    val e = intercept[SparkException] {
      Optimize.execute(plan)
    }
    assert(e.getCondition == "INTERNAL_ERROR")
    assert(e.getMessage.contains(
      "found a subquery with outer-scope reference"))
  }

  test("MATERIALIZED keeps a single-reference deterministic CTE") {
    val cteDef = CTERelationDef(
      TestRelation(Seq($"a".int)).select($"a"), materialized = Some(true))
    val cteRef = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val plan = WithCTE(cteRef.select($"a"), Seq(cteDef)).analyze
    assert(Optimize.execute(plan).exists(_.isInstanceOf[WithCTE]),
      "MATERIALIZED CTE should not be inlined")
    // `alwaysInline` mode still inlines it, e.g. to restore the plan shape for analysis checks.
    assert(!InlineCTE(alwaysInline = true).apply(plan).exists(_.isInstanceOf[WithCTE]),
      "MATERIALIZED CTE should be inlined in alwaysInline mode")
  }

  test("MATERIALIZED CTE with an outer reference across its boundary fails") {
    // Analysis rejects this for SQL; the optimizer guard covers plans built programmatically,
    // like it does for `forceSkipInline`.
    val relation = TestRelation(Seq($"a".int))
    val cteDef = CTERelationDef(
      relation.where(OuterReference($"a".int) === 1), materialized = Some(true))
    val cteRef = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val plan = WithCTE(cteRef.select($"a"), Seq(cteDef))
    val e = intercept[SparkException] {
      Optimize.execute(plan)
    }
    assert(e.getCondition == "INTERNAL_ERROR")
    assert(e.getMessage.contains(
      "A force-materialized CTE cannot carry an outer reference across its boundary"))
  }

  test("NOT MATERIALIZED inlines a multi-reference non-deterministic CTE") {
    val cteDef = CTERelationDef(
      OneRowRelation().select(rand(0).as("a")), materialized = Some(false))
    val cteRef = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val plan = WithCTE(cteRef.union(cteRef), Seq(cteDef)).analyze
    assert(!Optimize.execute(plan).exists(_.isInstanceOf[WithCTE]),
      "NOT MATERIALIZED CTE should be inlined")
  }

  test("SPARK-58779: optimizer InlineCTE (isAnalysis = false) fails on a ref with no definition") {
    // During analysis a CTERelationRef whose definition is not in the plan is tolerated -- it is
    // owned by a surrounding scope (e.g. when `ResolveSQLTableFunctions` runs `checkAnalysis` on a
    // table-function subplan whose argument references an outer CTE). In the optimizer the plan is
    // complete, so a missing definition indicates corruption and must fail loudly rather than be
    // dropped.
    val defX = CTERelationDef(TestRelation(Seq($"a".int)).select($"a"))
    val refX = CTERelationRef(defX.id, defX.resolved, defX.output, defX.isStreaming)
    val danglingRef = CTERelationRef(defX.id + 1000, true, Seq($"a".int), false)
    val plan = WithCTE(refX.union(danglingRef), Seq(defX))
    val e = intercept[SparkException] {
      InlineCTE(isAnalysis = false).apply(plan)
    }
    assert(e.getCondition == "INTERNAL_ERROR")
    // The analysis path tolerates the out-of-scope reference (no throw).
    InlineCTE(isAnalysis = true).apply(plan)
  }

  test("InlineCTE does not inline multi-ref CTE with " +
    "rowOrderIndependent nondet expr") {
    val cteDef = CTERelationDef(
      OneRowRelation().select(RowOrderIndepTestExpr(Literal(1))().as("v")))
    val cteRef1 = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val cteRef2 = CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
    val plan = WithCTE(
      cteRef1.join(cteRef2, condition = Some(cteRef1.output.head === cteRef2.output.head)),
      Seq(cteDef)).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.isInstanceOf[WithCTE])
    comparePlans(optimized, plan)
  }

  test("multi-ref CTE with rowOrderIndependent projection stays un-inlined while " +
    "PushdownPredicatesAndPruneColumnsForCTEDef pushes disjoint ref predicates through Project") {
    object OptimizeWithCTEPushdown extends RuleExecutor[LogicalPlan] {
      val batches =
        Batch("Inline CTE", Once, InlineCTE()) ::
        Batch("Operator Optimization", FixedPoint(100),
          PushPredicateThroughNonJoin,
          PushdownPredicatesAndPruneColumnsForCTEDef) ::
        Batch("Clean Up Temporary CTE Info", Once, CleanUpTempCTEInfo) :: Nil
    }

    val a = AttributeReference("a", IntegerType, nullable = true)()
    val b = AttributeReference("b", IntegerType, nullable = true)()
    val relation = LocalRelation(a, b)

    def buildPlan(projExpr: Alias, filterOnValueCol: Boolean = false): LogicalPlan = {
      val cteDef = CTERelationDef(Project(Seq(a, projExpr), relation))
      val ref1Out = cteDef.output.map(_.newInstance())
      val ref2Out = cteDef.output.map(_.newInstance())
      val idx = if (filterOnValueCol) 1 else 0
      val ref1 = Project(ref1Out, Filter(EqualTo(ref1Out(idx), Literal(1)),
        CTERelationRef(cteDef.id, true, ref1Out, false)))
      val ref2 = Project(ref2Out, Filter(EqualTo(ref2Out(idx), Literal(2)),
        CTERelationRef(cteDef.id, true, ref2Out, false)))
      WithCTE(Union(Seq(ref1, ref2)), Seq(cteDef))
    }

    // 1. rowOrderIndependent nondeterministic projection: stays un-inlined (1x execution), and
    //    outer disjoint predicates (a = 1 OR a = 2) push through Project onto the underlying scan.
    val roiAlias = Alias(RowOrderIndepTestExpr(b)(), "v")()
    val roiPlan = buildPlan(roiAlias)
    val optimizedRoi = OptimizeWithCTEPushdown.execute(roiPlan)
    assert(optimizedRoi.isInstanceOf[WithCTE])
    val optimizedRoiDef = optimizedRoi.asInstanceOf[WithCTE].cteDefs.head
    comparePlans(
      optimizedRoiDef.child,
      Project(
        Seq(a, roiAlias),
        Filter(Or(EqualTo(a, Literal(1)), EqualTo(a, Literal(2))), relation)),
      checkAnalysis = false)

    // 2. Kill-switch disabled: CTE stays un-inlined, and (a = 1 OR a = 2) stops above Project.
    withSQLConf(SQLConf.ROW_ORDER_INDEPENDENT_NONDETERMINISTIC_PUSHDOWN_ENABLED.key -> "false") {
      val optimizedDisabled = OptimizeWithCTEPushdown.execute(roiPlan)
      assert(optimizedDisabled.isInstanceOf[WithCTE])
      val disabledDef = optimizedDisabled.asInstanceOf[WithCTE].cteDefs.head
      comparePlans(
        disabledDef.child,
        Filter(
          Or(EqualTo(a, Literal(1)), EqualTo(a, Literal(2))),
          Project(Seq(a, roiAlias), relation)),
        checkAnalysis = false)
    }

    // 3. Outer predicates referencing the ROI output column stay above Project (no duplication).
    val roiValPredPlan = buildPlan(roiAlias, filterOnValueCol = true)
    val optimizedRoiValPred = OptimizeWithCTEPushdown.execute(roiValPredPlan)
    assert(optimizedRoiValPred.isInstanceOf[WithCTE])
    val roiValPredDef = optimizedRoiValPred.asInstanceOf[WithCTE].cteDefs.head
    comparePlans(
      roiValPredDef.child,
      Filter(
        Or(EqualTo(roiAlias.toAttribute, Literal(1)), EqualTo(roiAlias.toAttribute, Literal(2))),
        Project(Seq(a, roiAlias), relation)),
      checkAnalysis = false)

    // 4. Stateful nondeterministic projection (rand(0)): stays un-inlined, and outer disjoint
    //    predicates (a = 1 OR a = 2) are blocked above Project.
    val randAlias = Alias(Rand(0), "v")()
    val randPlan = buildPlan(randAlias)
    val optimizedRand = OptimizeWithCTEPushdown.execute(randPlan)
    assert(optimizedRand.isInstanceOf[WithCTE])
    val optimizedRandDef = optimizedRand.asInstanceOf[WithCTE].cteDefs.head
    comparePlans(
      optimizedRandDef.child,
      Filter(
        Or(EqualTo(a, Literal(1)), EqualTo(a, Literal(2))),
        Project(Seq(a, randAlias), relation)),
      checkAnalysis = false)
  }
}
