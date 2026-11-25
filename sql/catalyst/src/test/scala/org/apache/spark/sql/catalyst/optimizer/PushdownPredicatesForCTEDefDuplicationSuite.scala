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

import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.{Union, WithCTE}
import org.apache.spark.sql.types.LongType

/**
 * Tests for distinct CTE definition occurrences that share an ID after inlining. Lexical scope
 * must keep their predicate-pushdown and column-pruning state independent, whether or not
 * attribute deduplication also gives the copies different expression IDs.
 */
class PushdownPredicatesForCTEDefDuplicationSuite extends PlanTest {

  test("SPARK-61957: duplicated CTE definitions track predicates independently") {
    // The two inner definitions share cteId 1 but have different attributes. Each reference's
    // predicate points at the outer r (cteId 0) and must be translated to its own definition.
    val rId = AttributeReference("id", LongType, nullable = false)()
    val rCteId = 0L
    val rDef = CTERelationDef(LocalRelation(rId), rCteId, materialized = Some(true))
    val plan = WithCTE(duplicatedDefsPlan(Seq(rId)) { ref =>
      val rRef = CTERelationRef(rCteId, true, Seq(rId.newInstance()), false)
      val maxR = Aggregate(
        Nil,
        Seq(Alias(Max(rRef.output.head).toAggregateExpression(), "max")()),
        rRef)
      Project(ref.output, Filter(GreaterThan(ref.output.head, ScalarSubquery(maxR)), ref))
    }, Seq(rDef))

    val optimized = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)
    SimpleAnalyzer.checkAnalysis(optimized)
    val innerDefs = optimized.collectWithSubqueries {
      case d: CTERelationDef if d.id != rCteId => d
    }
    assert(innerDefs.length == 2, s"Expected both inner definitions: $optimized")
    innerDefs.foreach { cteDef =>
      val filter = cteDef.child match {
        case f: Filter => f
        case other => fail(s"Expected an independently translated filter: $other")
      }
      assert(filter.condition.references.forall(cteDef.outputSet.contains))
    }
  }

  test("SPARK-61957: same-attribute copies track predicates by lexical scope") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val n = AttributeReference("n", LongType, nullable = false)()
    val aCteId = 0L
    val xCteId = 1L
    val nCteId = 2L

    def maxOf(ref: CTERelationRef): ScalarSubquery = ScalarSubquery(Aggregate(
      Nil, Seq(Alias(Max(ref.output.head).toAggregateExpression(), "max")()), ref))

    def nCopy(ref: CTERelationRef, predicate: ScalarSubquery): WithCTE = {
      val cteDef = CTERelationDef(LocalRelation(n), nCteId, materialized = Some(true))
      WithCTE(Project(ref.output, Filter(GreaterThan(ref.output.head, predicate), ref)),
        Seq(cteDef))
    }

    val aRef = CTERelationRef(aCteId, true, Seq(a.newInstance()), false)
    val firstNRef = CTERelationRef(nCteId, true, Seq(n.newInstance()), false)
    val firstN = nCopy(firstNRef, maxOf(aRef))
    val xDef = CTERelationDef(Project(Seq(Alias(firstN.output.head, "x")()), firstN),
      xCteId, materialized = Some(true))
    val xRef = CTERelationRef(xCteId, true, xDef.output.map(_.newInstance()), false)
    val secondNRef = CTERelationRef(nCteId, true, Seq(n.newInstance()), false)

    // The first n copy is defined inside x and can see a but not x. The second is in the outer
    // body and can see x. Both definitions deliberately reuse nCteId and n's expression ID.
    val plan = WithCTE(nCopy(secondNRef, maxOf(xRef)),
      Seq(CTERelationDef(LocalRelation(a), aCteId, materialized = Some(true)), xDef))

    val optimized = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)
    SimpleAnalyzer.checkAnalysis(optimized)
    val nDefs = optimized.collectWithSubqueries {
      case d: CTERelationDef if d.output.map(_.exprId) == Seq(n.exprId) => d
    }
    assert(nDefs.length == 2, s"Expected both n copies: $optimized")
    val referencedCteIds = nDefs.map { cteDef =>
      val filter = cteDef.child match {
        case f: Filter => f
        case other => fail(s"Expected a pushed filter in each n copy: $other")
      }
      assert(filter.condition.references.forall(cteDef.outputSet.contains))
      filter.condition.collect {
        case s: ScalarSubquery => s.plan.collect { case r: CTERelationRef => r.cteId }
      }.flatten
    }
    assert(referencedCteIds.toSet == Set(Seq(aCteId), Seq(xCteId)))
    comparePlans(optimized, PushdownPredicatesAndPruneColumnsForCTEDef.apply(optimized))
  }

  test("SPARK-61957: duplicated CTE definitions track pruning independently") {
    // Each reference projects only d. Both definitions can be pruned, but only using attribute
    // requirements and mappings gathered from their own references.
    val id = AttributeReference("id", LongType, nullable = false)()
    val k = AttributeReference("k", LongType, nullable = false)()
    val d = AttributeReference("d", LongType, nullable = false)()
    val plan = duplicatedDefsPlan(Seq(id, k, d)) { ref =>
      Project(Seq(ref.output(2)), ref)
    }

    val optimized = PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan)
    SimpleAnalyzer.checkAnalysis(optimized)
    val cteDefs = optimized.collectWithSubqueries { case d: CTERelationDef => d }
    assert(cteDefs.length == 2, s"Expected both definitions: $optimized")
    cteDefs.foreach { cteDef =>
      assert(cteDef.child.isInstanceOf[Project], s"Expected independent pruning: $cteDef")
      assert(cteDef.child.output.map(_.name) == Seq("d"))
    }
    val cteRefs = optimized.collectWithSubqueries { case r: CTERelationRef => r }
    assert(cteRefs.nonEmpty && cteRefs.forall(_.output.map(_.name) == Seq("d")))
  }

  /**
   * Builds the post-InlineCTE shape with two materialized definitions sharing `cteId` but owning
   * different attributes. Each reference in turn has fresh attributes, as after self-join
   * deduplication. The callback builds the query above each reference.
   */
  private def duplicatedDefsPlan(
      output: Seq[Attribute])(query: CTERelationRef => LogicalPlan): LogicalPlan = {
    val cteId = 1L
    val branches = Seq(output.map(_.newInstance()), output.map(_.newInstance())).map { defOutput =>
      val cteDef = CTERelationDef(LocalRelation(defOutput), cteId, materialized = Some(true))
      val ref = CTERelationRef(cteId, true, defOutput.map(_.newInstance()), false)
      WithCTE(query(ref), Seq(cteDef))
    }
    Union(branches)
  }
}
