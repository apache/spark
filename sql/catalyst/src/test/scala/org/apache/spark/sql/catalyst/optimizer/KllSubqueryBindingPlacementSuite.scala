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

import org.apache.spark.sql.catalyst.analysis.KllSubqueryBindingPlacement
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.KllSketchAggBigint
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.LongType

class KllSubqueryBindingPlacementSuite extends PlanTest {
  private val a = $"a".long
  private val b = $"b".long
  private val c = $"c".long
  private def component = Join(LocalRelation(a), LocalRelation(b), Cross, None, JoinHint.NONE)
  private def binding = Alias(ScalarSubquery(OneRowRelation().groupBy()(
    KllSketchAggBigint(Literal(1L), legacyDeterministic = false)
      .toAggregateExpression().as("s"))), "binding")().asNondeterministicSubqueryBinding

  test("relocation preserves binding identity and reaches a fixed point") {
    val value = binding
    val projected = Project(component.output :+ value, component)
    val condition = And(EqualTo(a, c), EqualTo(Add(b, KllSketchGetNBigint(value.toAttribute)), c))
    val original = Join(projected, LocalRelation(c), Inner, Some(condition), JoinHint.NONE)
    val actual = PushDownKllSubqueryBindings(original)
    val aliases = actual.collect { case project: Project =>
      project.projectList.collect { case alias: Alias if alias.exprId == value.exprId => alias }
    }.flatten
    assert(aliases.size == 1)
    assert(aliases.head.child eq value.child)
    assert(actual.exists {
      case Project(fields, child: LocalRelation) =>
        fields.exists(_.exprId == value.exprId) && child.output == Seq(b)
      case _ => false
    })
    actual.foreach(p => assert(p.missingInput.isEmpty, p))
    comparePlans(PushDownKllSubqueryBindings(actual), actual)
  }

  test("live random and throwing projections and non-inner hinted joins remain boundaries") {
    val inner = component
    val hinted = inner.copy(hint = JoinHint(Some(HintInfo(strategy = Some(BROADCAST))), None))
    val random = Project(inner.output :+ Rand(0).as("live"), inner)
    val throwing = Project(inner.output :+
      Cast(RaiseError(Literal("live")), LongType).as("live"), inner)
    for (barrier <- Seq(random, throwing, hinted, inner.copy(joinType = LeftOuter))) {
      val value = binding
      val child = Project(barrier.output :+ value, barrier)
      val original = Join(child, LocalRelation(c), Inner,
        Some(EqualTo(Add(b, KllSketchGetNBigint(value.toAttribute)), c)), JoinHint.NONE)
      comparePlans(PushDownKllSubqueryBindings(original), original)
      val destination = new KllSubqueryBindingPlacement().destination(barrier, AttributeSet(Seq(b)))
      assert(destination eq barrier)
    }
  }
}
