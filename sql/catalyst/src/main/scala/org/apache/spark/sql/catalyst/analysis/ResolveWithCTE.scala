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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}

/**
 * Updates CTE references with the resolve output attributes of corresponding CTE definitions.
 */
object ResolveWithCTE extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.containsAllPatterns(CTE)) {
      // A helper map definitionID->Definition. Used for non-recursive CTE definitions only, either
      // inherently non-recursive or that became non-recursive due to recursive CTERelationRef->
      // UnionLoopRef substitution. Bridges the gap between two CTE resolutions (one for WithCTE,
      // another for reference) by materializing the resolved definitions in first pass and
      // consuming them in the second.
      val cteDefMap = mutable.HashMap.empty[Long, CTERelationDef]
      resolveWithCTE(plan, cteDefMap)
    } else {
      plan
    }
  }

  private def resolveWithCTE(
      plan: LogicalPlan,
      cteDefMap: mutable.HashMap[Long, CTERelationDef]): LogicalPlan = {
    plan.resolveOperatorsDownWithPruning(_.containsAllPatterns(CTE)) {
      case withCTE @ WithCTE(_, cteDefs) =>
        val newCTEDefs = cteDefs.map {
          // `cteDef.recursive` means "presence of a recursive CTERelationRef under cteDef". The
          // side effect of node substitution below is that after CTERelationRef substitution
          // its cteDef is no more considered `recursive`. This code path is common for `cteDef`
          // that were non-recursive from the get go, as well as those that are no more recursive
          // due to node substitution.
          case cteDef if !cteDef.recursive =>
            if (cteDef.resolved) {
              cteDefMap.put(cteDef.id, cteDef)
            }
            cteDef
          case cteDef =>
            cteDef.child match {
              // If it is a supported recursive CTE query pattern (4 so far), extract the anchor and
              // recursive plans from the Union and rewrite Union with UnionLoop. The recursive CTE
              // references inside UnionLoop's recursive plan will be rewritten as UnionLoopRef,
              // using the output of the resolved anchor plan. The side effect of recursive
              // CTERelationRef->UnionLoopRef substitution is that `cteDef` that was originally
              // considered `recursive` is no more in the context of `cteDef.recursive` method
              // definition.
              //
              // Simple case of duplicating (UNION ALL) clause.
              case alias @ SubqueryAlias(_, Union(Seq(anchor, recursion), false, false)) =>
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val loop = UnionLoop(
                    cteDef.id,
                    anchor,
                    rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, None))
                  cteDef.copy(child = alias.copy(child = loop))
                }

              // The case of CTE name followed by a parenthesized list of column name(s), eg.
              // WITH RECURSIVE t(n).
              case alias @ SubqueryAlias(_,
                  columnAlias @ UnresolvedSubqueryColumnAliases(
                  colNames,
                  Union(Seq(anchor, recursion), false, false)
                )) =>
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val loop = UnionLoop(
                    cteDef.id,
                    anchor,
                    rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, Some(colNames)))
                  cteDef.copy(child = alias.copy(child = columnAlias.copy(child = loop)))
                }

              // If the recursion is described with an UNION (deduplicating) clause then the
              // recursive term should not return those rows that have been calculated previously,
              // and we exclude those rows from the current iteration result.
              case alias @ SubqueryAlias(_,
                  Distinct(Union(Seq(anchor, recursion), false, false))) =>
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val loop = UnionLoop(
                    cteDef.id,
                    Distinct(anchor),
                    Except(
                      rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, None),
                      UnionLoopRef(cteDef.id, anchor.output, true),
                      isAll = false
                    )
                  )
                  cteDef.copy(child = alias.copy(child = loop))
                }

              // The case of CTE name followed by a parenthesized list of column name(s).
              case alias @ SubqueryAlias(_,
                  columnAlias@UnresolvedSubqueryColumnAliases(
                  colNames,
                  Distinct(Union(Seq(anchor, recursion), false, false))
                )) =>
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val loop = UnionLoop(
                    cteDef.id,
                    Distinct(anchor),
                    Except(
                      rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, Some(colNames)),
                      UnionLoopRef(cteDef.id, anchor.output, true),
                      isAll = false
                    )
                  )
                  cteDef.copy(child = alias.copy(child = columnAlias.copy(child = loop)))
                }

              case other =>
                // We do not support cases of sole Union (needs a SubqueryAlias above it), nor
                // Project (as UnresolvedSubqueryColumnAliases have not been substituted with the
                // Project yet), leaving us with cases of SubqueryAlias->Union and SubqueryAlias->
                // UnresolvedSubqueryColumnAliases->Union. The same applies to Distinct Union.
                throw new AnalysisException(
                  errorClass = "INVALID_RECURSIVE_CTE",
                  messageParameters = Map.empty)
            }
        }
        withCTE.copy(cteDefs = newCTEDefs)

      // This is a non-recursive reference to a definition.
      case ref: CTERelationRef if !ref.resolved =>
        cteDefMap.get(ref.cteId).map { cteDef =>
          // cteDef is certainly resolved, otherwise it would not have been in the map.
          CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
        }.getOrElse {
          ref
        }

      case other =>
        other.transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression => e.withNewPlan(resolveWithCTE(e.plan, cteDefMap))
        }
    }
  }

  // Substitutes a recursive CTERelationRef with UnionLoopRef if CTERelationRef refers to cteDefId.
  // Assumes that `anchor` is already resolved. If `columnNames` is set (which happens if an
  // UnresolvedSubqueryColumnAliases is atop a Union) also places an UnresolvedSubqueryColumnAliases
  // node above UnionLoopRef. At some later stage UnresolvedSubqueryColumnAliases gets resolved to
  // a Project node.
  private def rewriteRecursiveCTERefs(
      recursion: LogicalPlan,
      anchor: LogicalPlan,
      cteDefId: Long,
      columnNames: Option[Seq[String]]) = {
    recursion.transformWithPruning(_.containsPattern(CTE)) {
      case r: CTERelationRef if r.recursive && r.cteId == cteDefId =>
        val ref = UnionLoopRef(r.cteId, anchor.output, false)
        columnNames.map(UnresolvedSubqueryColumnAliases(_, ref)).getOrElse(ref)
    }
  }
}
