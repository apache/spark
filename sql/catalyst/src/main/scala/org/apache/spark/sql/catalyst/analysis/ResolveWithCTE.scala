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
  private def maybeUpdateRecursiveAnchor(cteDef: CTERelationDef): CTERelationDef = {
    cteDef.child match {
      case SubqueryAlias(_, ul: UnionLoop) =>
        if (ul.anchor.resolved) {
          cteDef.copy(recursionAnchor = Some(ul.anchor))
        } else {
          cteDef
        }
      case SubqueryAlias(_, d @ Distinct(ul: UnionLoop)) =>
        if (ul.anchor.resolved) {
          cteDef.copy(recursionAnchor = Some(d.copy(child = ul.anchor)))
        } else {
          cteDef
        }
      case SubqueryAlias(_, a @ UnresolvedSubqueryColumnAliases(_, ul: UnionLoop)) =>
        if (ul.anchor.resolved) {
          cteDef.copy(recursionAnchor = Some(a.copy(child = ul.anchor)))
        } else {
          cteDef
        }
      case SubqueryAlias(_, a @ UnresolvedSubqueryColumnAliases(_, d @ Distinct(ul: UnionLoop))) =>
        if (ul.anchor.resolved) {
          cteDef.copy(recursionAnchor = Some(a.copy(child = d.copy(child = ul.anchor))))
        } else {
          cteDef
        }
      case _ =>
        cteDef.failAnalysis(
          errorClass = "INVALID_RECURSIVE_CTE",
          messageParameters = Map.empty)
    }
  }

  private def resolveWithCTE(
      plan: LogicalPlan,
      cteDefMap: mutable.HashMap[Long, CTERelationDef]): LogicalPlan = {
    plan.resolveOperatorsDownWithPruning(_.containsAllPatterns(CTE)) {
      case w @ WithCTE(_, cteDefs) =>
        val newCTEDefs = cteDefs.map { cteDef =>
          val newCTEDef = if (cteDef.recursive) {
            cteDef.child match {
              // Substitutions to UnionLoop and UnionLoopRef.
              //
              // We do not support cases of Union (needs a SubqueryAlias above it), nor Project (as
              // UnresolvedSubqueryColumnAliases have not been substituted with the Project yet),
              // leaving us with cases of SubqueryAlias->Union and SubqueryAlias->
              // UnresolvedSubqueryColumnAliases->Union. The same applies to Distinct Union.
              case a @ SubqueryAlias(_, Union(Seq(anchor, recursion), false, false)) =>
                cteDef.copy(child =
                  a.copy(child =
                    UnionLoop(cteDef.id, anchor, transformRefs(recursion))))
              case a @ SubqueryAlias(_,
              ca @ UnresolvedSubqueryColumnAliases(_,
              Union(Seq(anchor, recursion), false, false))) =>
                cteDef.copy(child =
                  a.copy(child =
                    ca.copy(child =
                      UnionLoop(cteDef.id, anchor, transformRefs(recursion)))))
              // If the recursion is described with an UNION (deduplicating) clause then the
              // recursive term should not return those rows that have been calculated previously,
              // and we exclude those rows from the current iteration result.
              case a @ SubqueryAlias(_, Distinct(Union(Seq(anchor, recursion), false, false))) =>
                cteDef.copy(child =
                  a.copy(child =
                    UnionLoop(cteDef.id,
                      Distinct(anchor),
                      Except(
                        transformRefs(recursion),
                        UnionLoopRef(cteDef.id, cteDef.output, true),
                        false))))
              case a @ SubqueryAlias(_,
              ca @ UnresolvedSubqueryColumnAliases(_, Distinct(Union(Seq(anchor, recursion),
              false, false)))) =>
                cteDef.copy(child =
                  a.copy(child =
                    ca.copy(child =
                      UnionLoop(cteDef.id,
                        Distinct(anchor),
                        Except(
                          transformRefs(recursion),
                          UnionLoopRef(cteDef.id, cteDef.output, true),
                          false)))))
              case _ =>
                cteDef.failAnalysis(
                  errorClass = "INVALID_RECURSIVE_CTE",
                  messageParameters = Map.empty)
            }

            // If a recursive CTE definition is not yet resolved, store the anchor term (if it is
            // resolved) to the definition.
            if (!cteDef.resolved) {
              if (cteDef.recursionAnchor.isEmpty) {
                maybeUpdateRecursiveAnchor(cteDef)
              } else {
                cteDef
              }
            } else {
              cteDef
            }
          } else {
            cteDef
          }

          // cteDefMap holds resolved and "partially" resolved (only via anchor) CTE definitions.
          if (newCTEDef.resolved || newCTEDef.recursionAnchorResolved) {
            cteDefMap.put(newCTEDef.id, newCTEDef)
          }

          newCTEDef
        }
        w.copy(cteDefs = newCTEDefs)

      case ref: CTERelationRef if !ref.resolved =>
        cteDefMap.get(ref.cteId).map { cteDef =>
          if (ref.recursive) {
            // Recursive references can be resolved from the anchor term and non-resolved ref
            // implies non-resolved definition. Since the definition was present in the map of
            // resolved and "partially" resolved definitions, the only explanation is that
            // definition was "partially" resolved.
            if (cteDef.recursionAnchorResolved) {
              ref.copy(_resolved = true, output = cteDef.recursionAnchor.get.output,
                isStreaming = cteDef.isStreaming)
            } else {
              cteDef.failAnalysis(
                errorClass = "INVALID_RECURSIVE_CTE",
                messageParameters = Map.empty)
            }
          } else if (cteDef.resolved) {
            ref.copy(_resolved = true, output = cteDef.output, isStreaming = cteDef.isStreaming)
          } else {
            // In the non-recursive case, cteDefMap contains only resolved Definitions.
            cteDef.failAnalysis(
              errorClass = "INVALID_RECURSIVE_CTE",
              messageParameters = Map.empty)
          }
        }.getOrElse {
          ref
        }

      case other =>
        other.transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression => e.withNewPlan(resolveWithCTE(e.plan, cteDefMap))
        }
    }
  }
}
