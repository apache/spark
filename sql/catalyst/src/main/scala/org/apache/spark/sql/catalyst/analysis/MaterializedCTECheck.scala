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

import org.apache.spark.sql.catalyst.expressions.{OuterReference, OuterScopeReference, SubExprUtils, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, LogicalPlan, SubqueryAlias, WithCTE}
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Checks that a MATERIALIZED CTE does not reference the query enclosing it, as it is evaluated
 * once on its own. The CTEs it references, transitively, are checked with it, since they are
 * inlined into it, unless they are MATERIALIZED themselves and form their own boundary. Each
 * definition is scanned at its own operator level and never inside its subquery plans: a
 * correlation a definition keeps inside its own subquery targets that definition, not the
 * enclosing query. A MATERIALIZED CTE is also rejected in a correlated subquery when the query
 * of its WITH clause references the outer query, as a `WithCTE` that is not inlined cannot be
 * decorrelated. The check covers the given plan and all its subqueries.
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
      val materializedDefs = cteDefs.values.filter(_.materialized.contains(true)).toSeq
      if (materializedDefs.nonEmpty) {
        materializedDefs.foreach { cteDef =>
          (cteDef +: collectReferencedDefs(cteDef, cteDefs)).foreach(checkDefinition)
        }
        // Decorrelation stops at a subtree without outer references, so only a `WithCTE` whose
        // own subtree is correlated is on its path. A correlation above the WITH clause, e.g. on
        // a derived table holding it, is fine.
        plan.subqueriesAll.foreach(_.foreach {
          case withCTE: WithCTE if SubExprUtils.hasOuterReferences(withCTE) =>
            withCTE.cteDefs.find(_.materialized.contains(true)).foreach { cteDef =>
              val cteName = cteDef.child match {
                case alias: SubqueryAlias => alias.alias
                case _ => cteDef.id.toString
              }
              throw QueryCompilationErrors.materializedCTEInCorrelatedSubqueryError(
                cteName, cteDef.child.origin)
            }
          case _ =>
        })
      }
    }
  }

  private def checkDefinition(cteDef: CTERelationDef): Unit = {
    cteDef.child.foreach(_.expressions.foreach(_.foreach {
      case o: OuterReference =>
        throw QueryCompilationErrors.materializedCTEWithOuterReferenceError(o)
      case s: SubqueryExpression if s.outerScopeAttrs.nonEmpty =>
        // Defense in depth: resolution rejects a subquery whose outer-scope reference crosses the
        // boundary before this check, so this branch is only reached by hand-built plans.
        s.outerScopeAttrs.flatMap(_.collect { case r: OuterScopeReference => r }).headOption
          .foreach(r => throw QueryCompilationErrors.materializedCTEWithOuterReferenceError(r))
      case _ =>
    }))
  }

  /**
   * The definitions referenced by the given definition, transitively, that are inlined into it.
   * A definition nested in a definition is not collected, as its outer references target the
   * definition it is nested in. A referenced MATERIALIZED definition forms its own boundary and
   * is not followed.
   */
  private def collectReferencedDefs(
      cteDef: CTERelationDef,
      cteDefs: collection.Map[Long, CTERelationDef]): Seq[CTERelationDef] = {
    val referenced = mutable.LinkedHashMap.empty[Long, CTERelationDef]
    def visit(cteDef: CTERelationDef): Unit = {
      val nested = mutable.HashSet.empty[Long]
      cteDef.child.foreachWithSubqueries {
        case nestedDef: CTERelationDef => nested += nestedDef.id
        case _ =>
      }
      cteDef.child.foreachWithSubqueries {
        case ref: CTERelationRef
            if !nested.contains(ref.cteId) && !referenced.contains(ref.cteId) =>
          cteDefs.get(ref.cteId).filterNot(_.materialized.contains(true)).foreach { referencedDef =>
            referenced(referencedDef.id) = referencedDef
            visit(referencedDef)
          }
        case _ =>
      }
    }
    visit(cteDef)
    referenced.values.toSeq
  }
}
