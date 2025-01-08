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
  Project, SubqueryAlias, Union, UnionLoop, UnionLoopRef, WithCTE}
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

  private def transformRefs(plan: LogicalPlan) = {
    plan.transformWithPruning(_.containsPattern(CTE)) {
      case r: CTERelationRef if r.recursive =>
        UnionLoopRef(r.cteId, r.output, false)
    }
  }

  private def updateRecursiveAnchor(cteDef: CTERelationDef): CTERelationDef = {
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
          // If a recursive CTE definition is not yet resolved then extract the anchor term to the
          // definition, but if it is resolved then the extracted anchor term is no longer needed
          // and can be removed.
          val newCTEDef = if (cteDef.recursive) {
            cteDef.child match {
              case Union(Seq(anchor, recursion), false, false) =>
                cteDef.copy(child = UnionLoop(cteDef.id, anchor, transformRefs(recursion)))
              case a @ SubqueryAlias(_, Union(Seq(anchor, recursion), false, false)) =>
                cteDef.copy(child =
                  a.copy(child =
                    UnionLoop(cteDef.id, anchor, transformRefs(recursion))))
              case p @ Project(_, Union(Seq(anchor, recursion), false, false)) =>
                cteDef.copy(child =
                  p.copy(child =
                    UnionLoop(cteDef.id, anchor, transformRefs(recursion))))
              case a @ SubqueryAlias(_,
              p @ Project(_, Union(Seq(anchor, recursion), false, false))) =>
                cteDef.copy(child =
                  a.copy(child =
                    p.copy(child =
                      UnionLoop(cteDef.id, anchor, transformRefs(recursion)))))
              // If the recursion is described with an UNION (deduplicating) clause then the
              // recursive term should not return those rows that have been calculated previously,
              // and we exclude those rows from the current iteration result.
              case p @ Project(_, Distinct(Union(Seq(anchor, recursion), false, false))) =>
                cteDef.copy(child =
                  p.copy(child =
                    UnionLoop(cteDef.id,
                      Distinct(anchor),
                      Except(
                        transformRefs(recursion),
                        UnionLoopRef(cteDef.id, cteDef.output, true),
                        false))))
              case a @ SubqueryAlias(_,
              p @ Project(_, Distinct(Union(Seq(anchor, recursion), false, false)))) =>
                cteDef.copy(child =
                  a.copy(child =
                    p.copy(child =
                      UnionLoop(cteDef.id,
                        Distinct(anchor),
                        Except(
                          transformRefs(recursion),
                          UnionLoopRef(cteDef.id, cteDef.output, true),
                          false)))))
              case _ => cteDef
            }

            if (!cteDef.resolved) {
              if (cteDef.recursionAnchor.isEmpty) {
                updateRecursiveAnchor(cteDef)
              } else {
                cteDef
              }
            } else {
              if (cteDef.recursionAnchor.nonEmpty) {
                cteDef.copy(recursionAnchor = None)
              } else {
                cteDef
              }
            }
          } else {
            cteDef
          }

          if (newCTEDef.resolved || newCTEDef.recursionAnchorResolved) {
            cteDefMap.put(newCTEDef.id, newCTEDef)
          }

          newCTEDef
        }
        w.copy(cteDefs = newCTEDefs)

      case ref: CTERelationRef if !ref.resolved =>
        cteDefMap.get(ref.cteId).map { cteDef =>
          // Recursive references can be resolved from the anchor term.
          if (ref.recursive) {
            if (cteDef.recursionAnchorResolved) {
              ref.copy(_resolved = true, output = cteDef.recursionAnchor.get.output,
                isStreaming = cteDef.isStreaming)
            } else {
              ref
            }
          } else if (cteDef.resolved) {
            ref.copy(_resolved = true, output = cteDef.output, isStreaming = cteDef.isStreaming)
          } else {
            ref
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
