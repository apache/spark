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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.plans.logical.{
  CTERelationDef,
  Distinct,
  LogicalPlan,
  Project,
  SubqueryAlias,
  UnionLoop,
  UnresolvedWith,
  WithCTE
}

/**
 * Resolves and registers WITH definitions in declaration order with isolated scopes.
 *
 * {{{
 * WITH t AS (SELECT 1 AS n) SELECT n FROM t
 * }}}
 *
 * Registers `CTERelationDef(Project [1 AS n])` so the child can resolve to
 * `Project [n] -> CTERelationRef`.
 */
private[resolver] class CteDefinitionResolver(
    resolver: Resolver,
    expressionResolver: ExpressionResolver) {
  private val scopes = resolver.getNameScopes
  private val queryBlockScopes = resolver.getQueryBlockScopes
  private val cteRegistry = resolver.getCteRegistry
  private val expressionIdAssigner = expressionResolver.getExpressionIdAssigner

  /**
   * Resolves and registers the definitions of a WITH clause before its child query.
   *
   * For each CTE definition, a new [[CteScope]] is pushed with:
   *  - `cteName` and `cteId` to identify this specific CTE
   *  - `allowRecursion` flag indicating if this is a recursive CTE
   *  - `maxDepth` specifying iteration limit for recursive CTEs
   *  - `isInsideRecursiveCteScope` set to true for recursive CTEs to force immediate inlining of
   *  CTE definitions and prevent unnecessary stack traversals.
   *
   * The order in which CTE ids are allocated (via [[CTERelationDef.newId]]) can matter to
   * downstream consumers that key off of [[CTERelationDef.id]] (for example, lineage serializes
   * CTEs using the numeric id as the "table name").
   *
   * For non-recursive CTEs, we preserve the historical depth-first allocation behavior by
   * allocating ids *after* resolving the CTE plan. This ensures nested WITH clauses inside the
   * definition consume ids first (inner CTEs get smaller ids than their enclosing CTEs).
   *
   * For recursive CTEs, we must allocate the id up-front so that self-references can be
   * represented during resolution (via `UnionLoopRef` / [[UnionLoop]]).
   *
   * During CTE resolution, the scope tracks whether the CTE references itself. If self-referenced,
   * the CTE is validated as a properly structured recursive CTE.
   *
   */
  def resolve(unresolvedWith: UnresolvedWith): Unit = {
    for (cteRelation <- unresolvedWith.cteRelations) {
      val cteName = cteRelation.name
      val ctePlan = cteRelation.plan
      val maxDepth = cteRelation.maxDepth
      val preallocatedCteIdForRecursive: Option[Long] =
        if (unresolvedWith.allowRecursion) Some(CTERelationDef.newId) else None

      expressionIdAssigner.pushMapping()
      queryBlockScopes.push()
      scopes.pushScope()

      val recursiveCteParams = if (unresolvedWith.allowRecursion) {
        Some(
          RecursiveCteParameters(
            cteId = preallocatedCteIdForRecursive.get,
            cteName = cteName,
            maxDepth = maxDepth
          )
        )
      } else {
        None
      }

      cteRegistry.pushScope(
        recursiveCteParameters = recursiveCteParams,
        isInsideRecursiveCteScope = unresolvedWith.allowRecursion
      )

      var referencedRecursively = false
      var recursiveReferenceIds = Set.empty[Long]

      val resolvedCtePlan = try {
        val plan = resolver.resolve(ctePlan)
        referencedRecursively = cteRegistry.currentScope.referencedRecursively
        recursiveReferenceIds = cteRegistry.currentScope.enclosingRecursiveReferenceIds
        plan
      } finally {
        cteRegistry.popScope()
        scopes.popScope()
        queryBlockScopes.pop()
        expressionIdAssigner.popMapping()
      }

      val cteDefinition = if (referencedRecursively) {
        checkValidRecursiveCTE(resolvedCtePlan)
        CTERelationDef(
          resolvedCtePlan,
          id = preallocatedCteIdForRecursive.get,
          maxDepth = maxDepth,
          materialized = cteRelation.materialized
        )
      } else {
        CTERelationDef(resolvedCtePlan, materialized = cteRelation.materialized)
      }
      cteRegistry.registerCte(
        name = cteName,
        cteDefinition = cteDefinition,
        recursiveReferenceIds = recursiveReferenceIds
      )
    }
  }

  /**
   * Validates that a recursive CTE has the correct structure. Valid structures are:
   * - [[SubqueryAlias]] -> [[UnionLoop]] (optionally wrapped with [[Project]] or [[WithCTE]])
   *
   * Invalid structures:
   * - [[SubqueryAlias]] -> [[Distinct]] -> [[UnionLoop]]: UNION without ALL is not supported
   *   (rejects UNION, requires UNION ALL)
   * - Any other structure: The CTE must have a proper anchor and recursion branch
   *
   * @param plan The resolved CTE plan to validate
   */
  private def checkValidRecursiveCTE(plan: LogicalPlan): Unit = {
    plan match {
      case SubqueryAlias(_, UnionLoop(_, _, _, _, _, _)) =>
      case SubqueryAlias(_, WithCTE(UnionLoop(_, _, _, _, _, _), _)) =>
      case SubqueryAlias(_, Project(_, UnionLoop(_, _, _, _, _, _))) =>
      case SubqueryAlias(_, Project(_, WithCTE(UnionLoop(_, _, _, _, _, _), _))) =>
      case SubqueryAlias(_, Distinct(UnionLoop(_, _, _, _, _, _))) =>
        plan.failAnalysis(
          errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
      case SubqueryAlias(_, WithCTE(Distinct(UnionLoop(_, _, _, _, _, _)), _)) =>
        plan.failAnalysis(
          errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
      case SubqueryAlias(_, Project(_, Distinct(UnionLoop(_, _, _, _, _, _)))) =>
        plan.failAnalysis(
          errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
      case SubqueryAlias(_, Project(_, WithCTE(Distinct(UnionLoop(_, _, _, _, _, _)), _))) =>
        plan.failAnalysis(
          errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
      case _ =>
        throw new AnalysisException(
          errorClass = "INVALID_RECURSIVE_CTE",
          messageParameters = Map.empty
        )
    }
  }
}
