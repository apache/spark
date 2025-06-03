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

import java.util.{ArrayDeque, ArrayList}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.plans.logical.{
  CTERelationDef,
  LogicalPlan,
  UnresolvedWith,
  WithCTE
}
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_WITH

/**
 * The [[CteScope]] is responsible for keeping track of visible and known CTE definitions at a given
 * stage of a SQL query/DataFrame program resolution. These scopes are stacked and the stack is
 * managed by the [[CteRegistry]]. The scope is created per single WITH clause.
 *
 * The CTE operators are:
 *  - [[UnresolvedWith]]. This is a `host` operator that contains a list of unresolved CTE
 *    definitions from the WITH clause and a single child operator, which is the actual unresolved
 *    SELECT query.
 *  - [[UnresolvedRelation]]. This is a generic unresolved relation operator that will sometimes
 *    be resolved to a CTE definition and later replaced with a [[CTERelationRef]]. The CTE takes
 *    precedence over a regular table or a view when resolving this identifier.
 *  - [[CTERelationDef]]. This is a reusable logical plan, which will later be referenced by the
 *    lower CTE definitions and [[UnresolvedWith]] child.
 *  - [[CTERelationRef]]. This is a leaf node similar to a relation operator that references a
 *    certain [[CTERelationDef]] by its ID. It has a name (unique locally for a WITH clause list)
 *    and an ID (unique for all the CTEs in a query).
 *  - [[WithCTE]]. This is a `host` operator that contains a list of resolved CTE definitions from
 *    the WITH clause and a single child operator, which is the actual resolved SELECT query.
 *
 * The task of the [[Resolver]] is to correctly place [[WithCTE]] with [[CTERelationDef]]s inside
 * and make sure that [[CTERelationRef]]s correctly reference [[CTERelationDef]]s with their IDs.
 * The decision whether to inline those CTE subtrees or not is made by the [[Optimizer]], unlike
 * what Spark does for the [[View]]s (always inline during the analysis).
 *
 * There are some caveats in how Spark places those operators and resolves their names:
 *  - Ambiguous CTE definition names are disallowed only within a single WITH clause, and this is
 *    validated by the Parser in [[AstBuilder]]
 *    using [[QueryParsingErrors.duplicateCteDefinitionNamesError]]:
 *
 *    {{{
 *    -- This is disallowed.
 *    WITH cte AS (SELECT 1),
 *    cte AS (SELECT 2)
 *    SELECT * FROM cte;
 *    }}}
 *
 *  - When [[UnresolvedRelation]] identifier is resolved to a [[CTERelationDef]] and there is a
 *    name conflict on several layers of CTE definitions, the lower definitions take precedence:
 *
 *    {{{
 *    -- The result is `3`, lower [[CTERelationDef]] takes precedence.
 *    WITH cte AS (
 *      SELECT 1
 *    )
 *    SELECT * FROM (
 *      WITH cte AS (
 *        SELECT 2
 *      )
 *      SELECT * FROM (
 *        WITH cte AS (
 *          SELECT 3
 *        )
 *        SELECT * FROM cte
 *      )
 *    )
 *    }}}
 *
 *  - Any subquery can contain [[UnresolvedWith]] on top of it, but [[WithCTE]] is not gonna be
 *    1 to 1 to its unresolved counterpart. For example, if we are dealing with simple subqueries,
 *    [[CTERelationDef]]s will be merged together under a single [[WithCTE]]. The previous example
 *    would produce the following resolved plan:
 *
 *    {{{
 *    WithCTE
 *    :- CTERelationDef 18, false
 *    :  +- ...
 *    :- CTERelationDef 19, false
 *    :  +- ...
 *    :- CTERelationDef 20, false
 *    :  +- ...
 *    +- Project [3#1203]
 *    :  +- ...
 *    }}}
 *
 *  - The [[WithCTE]] operator is placed on top of the resolved operator if one of the following
 *    conditions are met:
 *      1. We just resolved an [[UnresolvedWith]], which is the topmost [[UnresolvedWith]] of this
 *         root query, view or an expression subquery.
 *      2. In case there is no single topmost [[UnresolvedWith]], we pick the least common ancestor
 *         of those branches. This is going to be a multi-child operator - [[Union]], [[Join]], etc.
 *
 *    Here's an example for the second case:
 *
 *    {{{
 *    SELECT * FROM (
 *      WITH cte AS (
 *        SELECT 1
 *      )
 *      SELECT * FROM cte
 *      UNION ALL
 *      (
 *        WITH cte AS (
 *          SELECT 2
 *        )
 *        SELECT * FROM cte
 *      )
 *    )
 *    }}}
 *
 *    ->
 *
 *    {{{
 *    Project [1#60]
 *    +- SubqueryAlias __auto_generated_subquery_name
 *       +- WithCTE
 *          :- CTERelationDef 30, false
 *          :  +- ...
 *          :- CTERelationDef 31, false
 *          :  +- ...
 *          +- Union false, false
 *             :- Project [1#60]
 *             :  +- ...
 *             +- Project [2#61]
 *                +- ...
 *    }}}
 *
 *    Consider a different example though:
 *
 *    {{{
 *    SELECT * FROM (
 *      SELECT 1
 *      UNION ALL
 *      (
 *        WITH cte AS (
 *          SELECT 2
 *        )
 *        SELECT * FROM cte
 *      )
 *    )
 *    }}}
 *
 *    The [[Union]] operator is not the least common ancestor of the [[UnresolvedWith]]s in the
 *    query. In fact, there's just a single [[UnresolvedWith]], which is a proper place where we
 *    need to place a [[WithCTE]].
 *
 *  - However, if we have any expression subquery (scalar/IN/EXISTS...), the top
 *    [[CTERelationDef]]s and subquery's [[CTERelationDef]] won't be merged together (as they are
 *    separated by an expression tree):
 *
 *    {{{
 *    WITH cte AS (
 *      SELECT 1 AS col1
 *    )
 *    SELECT * FROM cte WHERE col1 IN (
 *      WITH cte AS (
 *        SELECT 2
 *      )
 *      SELECT * FROM cte
 *    )
 *    }}}
 *
 *    ->
 *
 *    {{{
 *    WithCTE
 *    :- CTERelationDef 21, false
 *    :  +- ...
 *    +- Project [col1#1223]
 *       +- Filter col1#1223 IN (list#1222 [])
 *          :  +- WithCTE
 *          :     :- CTERelationDef 22, false
 *          :     :  +- ...
 *          :     +- Project [2#1241]
 *          :        +- ...
 *          +- ...
 *    }}}
 *
 *  - Upper CTEs are visible through subqueries and can be referenced by lower operators, but not
 *    through the [[View]] boundary:
 *
 *    {{{
 *    CREATE VIEW v1 AS SELECT 1;
 *    CREATE VIEW v2 AS SELECT * FROM v1;
 *
 *    -- The result is 1.
 *    -- The `v2` body will be inlined in the main query tree during the analysis, but upper `v1`
 *    -- CTE definition _won't_ take precedence over the lower `v1` view.
 *    WITH v1 AS (
 *      SELECT 2
 *    )
 *    SELECT * FROM v2;
 *    }}}
 *
 * @param isRoot This marks the place where [[WithCTE]] has to be placed with all the merged
 *   [[CTERelationDef]] that were collected under it. It will be true for root query, [[View]]s
 *   and expression subqueries.
 * @param isOpaque This flag makes this [[CteScope]] opaque for [[CTERelationDef]] lookups. It will
 *   be true for root query and [[View]]s.
 */
class CteScope(val isRoot: Boolean, val isOpaque: Boolean) {

  /**
   * Known [[CTERelationDef]]s that were already resolved in this scope or in child scopes. This is
   * used to merge CTE definitions together in a single [[WithCTE]].
   */
  private val knownCtes = new ArrayList[CTERelationDef]

  /**
   * Visible [[CTERelationDef]]s that were already resolved in this scope. Child scope definitions
   * are _not_ visible. Upper definitions _are_ visible, but this is handled by
   * [[CteRegistry.resolveCteName]] to avoid cascadingly growing [[IdentifierMap]]s.
   */
  private val visibleCtes = new IdentifierMap[CTERelationDef]

  /**
   * Optionally put [[WithCTE]] on top of the `resolvedOperator`. This is done just for the root
   * scopes in the context of a correct `unresolvedOperator`. Return the `resolvedOperator`
   * otherwise.
   */
  def tryPutWithCTE(unresolvedOperator: LogicalPlan, resolvedOperator: LogicalPlan): LogicalPlan = {
    if (!knownCtes.isEmpty && isRoot && isSuitableOperatorForWithCTE(unresolvedOperator)) {
      WithCTE(resolvedOperator, knownCtes.asScala.toSeq)
    } else {
      resolvedOperator
    }
  }

  /**
   * Register a new CTE definition in this scope. Since the scope is created per single WITH clause,
   * there can be no name conflicts, but this is validated by the Parser in [[AstBuilder]]
   * using [[QueryParsingErrors.duplicateCteDefinitionNamesError]]. This definition will be both
   * known and visible.
   */
  def registerCte(name: String, cteDef: CTERelationDef): Unit = {
    knownCtes.add(cteDef)
    visibleCtes.put(name, cteDef)
  }

  /**
   * Get a visible CTE definition by its name.
   */
  def getCte(name: String): Option[CTERelationDef] = {
    visibleCtes.get(name)
  }

  /**
   * Merge the state from a child scope. We transfer all the known CTE definitions to later merge
   * them in one [[WithCTE]]. Root scopes terminate this chain, since they have their own
   * [[WithCTE]].
   */
  def mergeChildScope(childScope: CteScope): Unit = {
    if (!childScope.isRoot) {
      knownCtes.addAll(childScope.knownCtes)
    }
  }

  /**
   * This predicate returns `true` if the `unresolvedOperator` is suitable to place a [[WithCTE]]
   * on top of its resolved counterpart. This is the case for:
   *  - [[UnresolvedWith]];
   *  - Multi-child operators with [[UnresolvedWith]]s in multiple subtrees.
   */
  private def isSuitableOperatorForWithCTE(unresolvedOperator: LogicalPlan): Boolean = {
    unresolvedOperator match {
      case _: UnresolvedWith => true
      case _ =>
        CteRegistry.isSuitableMultiChildOperatorForWithCTE(unresolvedOperator)
    }
  }
}

/**
 * The [[CteRegistry]] is responsible for managing the stack of [[CteScope]]s and resolving visible
 * [[CTERelationDef]] names.
 */
class CteRegistry {
  private val stack = new ArrayDeque[CteScope]
  stack.push(new CteScope(isRoot = true, isOpaque = true))

  def currentScope: CteScope = stack.peek()

  /**
   * This is a [[withNewScope]] variant specifically designed to be called above multi-child
   * operator children resolution (e.g. for children of a [[Join]] or [[Union]]).
   *
   * The `isRoot` flag has to be propagated from the parent scope if all of the following
   * conditions are met:
   *  - The current scope is a root scope
   *  - The multi-child `unresolvedOperator` IS NOT suitable to place a [[WithCTE]]
   *  - Some operator in `unresolvedChild` subtree IS suitable to place a [[WithCTE]].
   */
  def withNewScopeUnderMultiChildOperator[R](
      unresolvedOperator: LogicalPlan,
      unresolvedChild: LogicalPlan
  )(body: => R): R = {
    withNewScope(
      isRoot = currentScope.isRoot &&
        !CteRegistry.isSuitableMultiChildOperatorForWithCTE(unresolvedOperator) &&
        CteRegistry.hasSuitableOperatorForWithCTEInSubtree(unresolvedChild)
    ) {
      body
    }
  }

  /**
   * A RAII-wrapper for pushing/popping scopes. This is used by the [[Resolver]] to create a new
   * scope for each WITH clause.
   */
  def withNewScope[R](isRoot: Boolean = false, isOpaque: Boolean = false)(body: => R): R = {
    stack.push(new CteScope(isRoot = isRoot, isOpaque = isOpaque))

    try {
      body
    } finally {
      val childScope = stack.pop()
      currentScope.mergeChildScope(childScope)
    }
  }

  /**
   * Resolve `name` to a visible [[CTERelationDef]]. The upper definitions are also visible, and
   * the lowest of them takes precedence. Opaque scopes terminate the lookup (e.g. [[View]]
   * boundary).
   */
  def resolveCteName(name: String): Option[CTERelationDef] = {
    val iter = stack.iterator
    var done = false
    var result: Option[CTERelationDef] = None
    while (iter.hasNext() && !done) {
      val scope = iter.next()

      done = scope.isOpaque

      scope.getCte(name) match {
        case Some(cte) =>
          result = Some(cte)
          done = true
        case None =>
      }
    }

    result
  }
}

object CteRegistry {

  /**
   * This predicate returns `true` if the `unresolvedOperator` is a multi-child operator that
   * contains multiple [[UnresolvedWith]] operators in its subtrees. This way we determine if
   * this operator is suitable to place a [[WithCTE]] on top of its resolved counterpart.
   */
  def isSuitableMultiChildOperatorForWithCTE(unresolvedOperator: LogicalPlan): Boolean = {
    unresolvedOperator.children.count(hasSuitableOperatorForWithCTEInSubtree(_)) > 1
  }

  def hasSuitableOperatorForWithCTEInSubtree(unresolvedOperator: LogicalPlan): Boolean = {
    unresolvedOperator.containsPattern(UNRESOLVED_WITH)
  }
}
