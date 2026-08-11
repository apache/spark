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
import org.apache.spark.sql.catalyst.expressions.{OuterReference, OuterScopeReference, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CTERelationDef, CTERelationRef, LogicalPlan, OneRowRelation, WithCTE}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

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
}
