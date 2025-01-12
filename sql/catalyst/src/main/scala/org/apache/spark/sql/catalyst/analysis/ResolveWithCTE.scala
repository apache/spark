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

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, Distinct, Except, LogicalPlan,
  SubqueryAlias, Union, UnionLoop, UnionLoopRef, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Updates CTE references with the resolve output attributes of corresponding CTE definitions.
 */
object ResolveWithCTE extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.containsAllPatterns(CTE)) {
      val cteDefMap = mutable.HashMap.empty[Long, CTERelationDef]
      resolveWithCTE(plan, cteDefMap)
    } else {
      plan
    }
  }

  // Substitute CTERelationRef with UnionLoopRef.
  private def transformRefs(plan: LogicalPlan) = {
    plan.transformWithPruning(_.containsPattern(CTE)) {
      case r: CTERelationRef if r.recursive =>
        UnionLoopRef(r.cteId, r.output, false)
    }
  }

  // Update the definition's recursiveAnchor if the anchor is resolved.
  private def recursiveAnchorResolved(cteDef: CTERelationDef): Option[LogicalPlan] = {
    cteDef.child match {
      case SubqueryAlias(_, ul: UnionLoop) =>
        if (ul.anchor.resolved) {
          Some(ul.anchor)
        } else {
          None
        }
      case SubqueryAlias(_, Distinct(ul: UnionLoop)) =>
        if (ul.anchor.resolved) {
          Some(ul.anchor)
        } else {
          None
        }
      case SubqueryAlias(_, UnresolvedSubqueryColumnAliases(_, ul: UnionLoop)) =>
        if (ul.anchor.resolved) {
          Some(ul.anchor)
        } else {
          None
        }
      case SubqueryAlias(_, UnresolvedSubqueryColumnAliases(_, Distinct(ul: UnionLoop))) =>
        if (ul.anchor.resolved) {
          Some(ul.anchor)
        } else {
          None
        }
      case _ =>
        cteDef.failAnalysis(
          errorClass = "INVALID_RECURSIVE_CTE",
          messageParameters = Map.empty)
        throw QueryCompilationErrors.recursiveCteError()
    }
  }

  private def resolveWithCTE(
      plan: LogicalPlan,
      cteDefMap: mutable.HashMap[Long, CTERelationDef]): LogicalPlan = {
    plan.resolveOperatorsDownWithPruning(_.containsAllPatterns(CTE)) {
      case withCTE @ WithCTE(_, cteDefs) =>
        val newCTEDefs = cteDefs.map { cteDef =>
          val newCTEDef = if (cteDef.recursive) {
            cteDef.child match {
              // Substitutions to UnionLoop and UnionLoopRef.
              case alias @ SubqueryAlias(_, Union(Seq(anchor, recursion), false, false)) =>
                cteDef.copy(child =
                  alias.copy(child =
                    UnionLoop(cteDef.id, anchor, transformRefs(recursion))))
              case alias as@ SubqueryAlias(_,
              columnAlias @ UnresolvedSubqueryColumnAliases(_,
              Union(Seq(anchor, recursion), false, false))) =>
                cteDef.copy(child =
                  alias.copy(child =
                    columnAlias.copy(child =
                      UnionLoop(cteDef.id, anchor, transformRefs(recursion)))))
              // If the recursion is described with an UNION (deduplicating) clause then the
              // recursive term should not return those rows that have been calculated previously,
              // and we exclude those rows from the current iteration result.
              case alias @ SubqueryAlias(_,
                  Distinct(Union(Seq(anchor, recursion), false, false))) =>
                cteDef.copy(child =
                  alias.copy(child =
                    UnionLoop(cteDef.id,
                      Distinct(anchor),
                      Except(
                        transformRefs(recursion),
                        UnionLoopRef(cteDef.id, cteDef.output, true),
                        false))))
              case alias @ SubqueryAlias(_,
              columnAlias @ UnresolvedSubqueryColumnAliases(_,
                  Distinct(Union(Seq(anchor, recursion),
              false, false)))) =>
                cteDef.copy(child =
                  alias.copy(child =
                    columnAlias.copy(child =
                      UnionLoop(cteDef.id,
                        Distinct(anchor),
                        Except(
                          transformRefs(recursion),
                          UnionLoopRef(cteDef.id, cteDef.output, true),
                          false)))))
              case _ =>
                // We do not support cases of sole Union (needs a SubqueryAlias above it), nor
                // Project (as UnresolvedSubqueryColumnAliases have not been substituted with the
                // Project yet), leaving us with cases of SubqueryAlias->Union and SubqueryAlias->
                // UnresolvedSubqueryColumnAliases->Union. The same applies to Distinct Union.
                cteDef.failAnalysis(
                    errorClass = "INVALID_RECURSIVE_CTE",
                    messageParameters = Map.empty)
                throw QueryCompilationErrors.recursiveCteError()
            }
          } else {
            cteDef
          }

          if (newCTEDef.recursive) {
            // cteDefMap holds "partially" resolved (only via anchor) CTE definitions in the
            // recursive case.
            if (newCTEDef.resolved) {
              newCTEDef.failAnalysis(
                errorClass = "INVALID_RECURSIVE_CTE",
                messageParameters = Map.empty)
              throw QueryCompilationErrors.recursiveCteError()
            }
            if (recursiveAnchorResolved(newCTEDef).isDefined) {
              cteDefMap.put(newCTEDef.id, newCTEDef)
            }
          } else {
            if (newCTEDef.resolved) {
              cteDefMap.put(newCTEDef.id, newCTEDef)
            }
          }

          newCTEDef
        }
        withCTE.copy(cteDefs = newCTEDefs)

      case ref: CTERelationRef if !ref.resolved =>
        cteDefMap.get(ref.cteId) match {
          case Some(cteDef) if !ref.recursive =>
            if (!cteDef.resolved) {
              // In the non-recursive case, cteDefMap contains resolved Definitions.
              cteDef.failAnalysis(
                errorClass = "INVALID_RECURSIVE_CTE",
                messageParameters = Map.empty)
              throw QueryCompilationErrors.recursiveCteError()
            }
            ref.copy(_resolved = true, output = cteDef.output, isStreaming = cteDef.isStreaming)
          case Some(cteDef) =>
            // Recursive references can be resolved from the anchor term. Non-resolved ref implies
            // non-resolved definition. Since the definition was present in the map of resolved and
            // "partially" resolved definitions, the only explanation is that definition was
            // "partially" resolved.
            val anchorResolved = recursiveAnchorResolved(cteDef)
            if (anchorResolved.isDefined) {
              ref.copy(_resolved = true, output = anchorResolved.get.output,
                isStreaming = cteDef.isStreaming)
            } else {
              cteDef.failAnalysis(
                errorClass = "INVALID_RECURSIVE_CTE",
                messageParameters = Map.empty)
              throw QueryCompilationErrors.recursiveCteError()
            }
          case None =>
            ref
        }

      case other =>
        other.transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression => e.withNewPlan(resolveWithCTE(e.plan, cteDefMap))
        }
    }
  }
}
