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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, OuterReference, OuterScopeReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Checks that a MATERIALIZED CTE does not reference the query enclosing it, as it is evaluated
 * once on its own. The CTEs it references, transitively, are checked against the same boundary,
 * since they are inlined into it. An outer reference crosses the boundary only if it targets an
 * attribute that is not produced within it: a CTE nested in a subquery of the definition may
 * legitimately be correlated to the definition's own relations. The check covers the given plan
 * and all its subqueries.
 */
object MaterializedCTECheck extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = {
    if (plan.containsPattern(CTE)) {
      // All CTE definitions, including those of nested subqueries, so that references from a
      // MATERIALIZED CTE can be followed across subquery boundaries.
      val cteDefs = mutable.LinkedHashMap.empty[Long, CTERelationDef]
      plan.foreachWithSubqueries {
        case cteDef: CTERelationDef => cteDefs(cteDef.id) = cteDef
        case _ =>
      }
      cteDefs.values.filter(_.materialized.contains(true)).foreach { cteDef =>
        checkMaterializedCTE(cteDef, cteDefs)
      }
    }
  }

  private def checkMaterializedCTE(
      cteDef: CTERelationDef,
      cteDefs: collection.Map[Long, CTERelationDef]): Unit = {
    val inlinedDefs = collectInlinedDefs(cteDef, cteDefs)
    // The attributes produced within the materialized boundary, including those of nested
    // subqueries, as a correlation to any of them does not cross the boundary. Unresolved
    // operators are skipped: they may not have an output, and are reported by the analysis
    // checks that follow.
    val internalAttrs = AttributeSet(inlinedDefs.flatMap(
      _.child.collectWithSubqueries { case p if p.resolved => p.output }.flatten))
    inlinedDefs.foreach { inlinedDef =>
      // The scan stays out of subquery plans on purpose: a correlation from a nested subquery to
      // the enclosing query fails resolution before this check, and a correlation targeting an
      // attribute within the boundary is legitimate.
      inlinedDef.child.foreach(_.expressions.foreach(_.foreach {
        case o: OuterReference if !internalAttrs.contains(o.toAttribute) =>
          throw QueryCompilationErrors.materializedCTEWithOuterReferenceError(o)
        case s: SubqueryExpression if s.outerScopeAttrs.nonEmpty =>
          // Defense in depth for a subquery whose outer-scope reference crosses the boundary:
          // resolution rejects that shape first, so this branch is only reached by hand-built
          // plans.
          s.outerScopeAttrs.flatMap(_.collect { case r: OuterScopeReference => r })
            .find(r => !internalAttrs.contains(r.toAttribute))
            .foreach(r => throw QueryCompilationErrors.materializedCTEWithOuterReferenceError(r))
        case _ =>
      }))
    }
  }

  /**
   * The given CTE definition and the definitions it references, transitively, which are inlined
   * into it.
   */
  private def collectInlinedDefs(
      cteDef: CTERelationDef,
      cteDefs: collection.Map[Long, CTERelationDef]): Seq[CTERelationDef] = {
    val visited = mutable.LinkedHashMap.empty[Long, CTERelationDef]
    def visit(cteDef: CTERelationDef): Unit = {
      if (!visited.contains(cteDef.id)) {
        visited(cteDef.id) = cteDef
        cteDef.child.foreachWithSubqueries {
          case ref: CTERelationRef => cteDefs.get(ref.cteId).foreach(visit)
          case _ =>
        }
      }
    }
    visit(cteDef)
    visited.values.toSeq
  }
}
