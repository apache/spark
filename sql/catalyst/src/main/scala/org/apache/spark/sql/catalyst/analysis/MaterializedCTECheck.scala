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

import org.apache.spark.sql.catalyst.expressions.{OuterReference, OuterScopeReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Checks that a MATERIALIZED CTE does not reference the query enclosing it, neither directly nor
 * through a subquery with an outer-scope reference, as it is evaluated once on its own. The CTEs
 * it references are checked as well, since they are inlined into it unless they are MATERIALIZED
 * themselves. The check covers the given plan and all its subqueries.
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
      val checked = mutable.HashSet.empty[Long]
      cteDefs.values.filter(_.materialized.contains(true)).foreach { cteDef =>
        checkCTERelationDef(cteDef, cteDefs, checked)
      }
    }
  }

  private def checkCTERelationDef(
      cteDef: CTERelationDef,
      cteDefs: collection.Map[Long, CTERelationDef],
      checked: mutable.Set[Long]): Unit = {
    if (checked.add(cteDef.id)) {
      cteDef.child.foreach(_.expressions.foreach(_.foreach {
        case o: OuterReference =>
          throw QueryCompilationErrors.materializedCTEWithOuterReferenceError(o)
        case s: SubqueryExpression if s.outerScopeAttrs.nonEmpty =>
          val outerScopeRef = s.outerScopeAttrs
            .flatMap(_.collect { case r: OuterScopeReference => r }).head
          throw QueryCompilationErrors.materializedCTEWithOuterReferenceError(outerScopeRef)
        case _ =>
      }))
      cteDef.child.foreachWithSubqueries {
        case ref: CTERelationRef =>
          cteDefs.get(ref.cteId).foreach(checkCTERelationDef(_, cteDefs, checked))
        case _ =>
      }
    }
  }
}
