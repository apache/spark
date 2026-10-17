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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.DataType

class DeduplicateRelationsSuite extends PlanTest {
  import DeduplicateRelationsSuite._

  test("SPARK-60135: skip expression mapping for plans without subqueries") {
    val relation = LocalRelation($"a".int)
    val plan = new Filter(EqualTo(relation.output.head, Literal(1)), relation) {
      override def mapExpressions(f: Expression => Expression): this.type = {
        fail("Expressions should not be mapped when there are no subqueries")
      }
    }

    assert(DeduplicateRelations(plan) eq plan)
  }

  test("SPARK-60135: prune expression branches without subqueries") {
    val relation = LocalRelation($"a".int)
    val unrelated = TraversalCountingExpression(Add(Literal(1), Literal(2)))
    val subquery = ScalarSubquery(relation)
    val alias = Alias(Add(unrelated, subquery), "s")()
    val plan = Project(Seq(alias), relation)

    val result = DeduplicateRelations(plan).asInstanceOf[Project]
    assert(unrelated.traversals == 0)
    assert(result.child eq relation)
    assert(result.output.head.exprId == alias.exprId)
    val renewed = result.projectList.head.collect { case s: ScalarSubquery => s }.head
    assert(renewed.exprId == subquery.exprId)
    assert(renewed.plan.outputSet.intersect(relation.outputSet).isEmpty)
  }

  test("SPARK-60135: renew children even when there are no subqueries") {
    val relation = LocalRelation($"a".int)
    val attr = relation.output.head
    val project = Project(Seq(Alias(Add(attr, Literal(1)), "b")()),
      Filter(EqualTo(attr, Literal(1)), relation))
    val plan = Join(project, project, Inner, None, JoinHint.NONE)

    val result = DeduplicateRelations(plan).asInstanceOf[Join]
    assert(result.left eq project)
    assert(result.duplicateResolved)
    assert(result.right.collect { case r: LocalRelation => r }.head.outputSet
      .intersect(relation.outputSet).isEmpty)
    assert(!result.right.exists(_.missingInput.nonEmpty))
  }

  test("SPARK-60135: renew shared nested subqueries in traversal order") {
    val inner = LocalRelation($"a".int)
    val middle = LocalRelation($"b".int)
    val outer = LocalRelation($"c".int)
    val nested = Exists(inner)
    val shared = Exists(Filter(Not(nested), middle))
    val plan = Filter(And(shared, shared), outer)

    // The first occurrence does not change, but the second must still be visited with the
    // updated set of relations. It must not be cached as an ineffective transformation.
    val result = DeduplicateRelations(plan).asInstanceOf[Filter]
    val subqueries = result.condition.collect { case s: Exists => s }
    assert(subqueries.size == 2)
    assert(subqueries.head eq shared)
    assert(subqueries.map(_.exprId) == Seq(shared.exprId, shared.exprId))
    val renewed = subqueries.last.plan.asInstanceOf[Filter]
    assert(renewed.outputSet.intersect(middle.outputSet).isEmpty)
    val renewedNested = renewed.condition.collect { case s: Exists => s }.head
    assert(renewedNested.exprId == nested.exprId)
    assert(renewedNested.plan.outputSet.intersect(inner.outputSet).isEmpty)
    assert(DeduplicateRelations(result) eq result)
  }

  test("SPARK-60135: preserve correlation when renewing a shared plan with a wrapped subquery") {
    val outer = LocalRelation($"a".int)
    val inner = LocalRelation($"b".int)
    val outerAttr = outer.output.head
    val innerAttr = inner.output.head
    val subquery = Exists(
      Filter(EqualTo(innerAttr, OuterReference(outerAttr)), inner), Seq(outerAttr))
    val filter = Filter(Not(subquery), outer)
    val plan = Join(filter, filter, Inner, None, JoinHint.NONE)

    val result = DeduplicateRelations(plan).asInstanceOf[Join]
    assert(result.left eq filter)
    assert(result.duplicateResolved)
    val renewed = result.right.asInstanceOf[Filter]
    val renewedSubquery = renewed.condition.collect { case s: Exists => s }.head
    val renewedInner = renewedSubquery.plan.asInstanceOf[Filter]
    assert(renewedSubquery.exprId == subquery.exprId)
    assert(renewedSubquery.outerAttrs == renewed.output)
    assert(renewedInner.condition ==
      EqualTo(renewedInner.child.output.head, OuterReference(renewed.output.head)))
    assert(renewedInner.outputSet.intersect(inner.outputSet).isEmpty)
    assert(!result.exists(_.missingInput.nonEmpty))
    assert(renewedInner.missingInput.isEmpty)
  }
}

object DeduplicateRelationsSuite {
  private case class TraversalCountingExpression(child: Expression)
    extends Expression with Unevaluable {
    var traversals: Int = 0

    override def children: Seq[Expression] = Seq(child)
    override def dataType: DataType = child.dataType
    override def nullable: Boolean = child.nullable

    override def mapChildren(f: Expression => Expression): Expression = {
      traversals += 1
      super.mapChildren(f)
    }

    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression = copy(child = newChildren.head)
  }
}
