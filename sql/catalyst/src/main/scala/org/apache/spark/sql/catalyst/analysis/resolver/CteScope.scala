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

import org.apache.spark.sql.catalyst.analysis.UnresolvedSubqueryColumnAliases
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{
  CTERelationDef,
  LogicalPlan,
  UnionLoop,
  UnionLoopRef,
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
 *  - Any subquery can contain [[UnresolvedWith]] on top of it, but [[WithCTE]] is not going to be
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
 * == Recursive CTEs ==
 *
 * For recursive CTEs (marked with [[recursiveCteParameters]] being defined), additional operators
 * are used:
 *  - [[UnionLoop]]. This operator replaces a [[Union]] when a recursive CTE is detected. It has
 *    an anchor (non-recursive) branch and a recursion branch, with fixed output expression IDs
 *    that remain stable across all iterations. The [[UnionLoop]] is created during set operation
 *    resolution when self-references are detected.
 *  - [[UnionLoopRef]]. This is a leaf node representing a self-reference within a recursive CTE.
 *    During the first resolution pass, it has placeholder output. During final resolution, it
 *    references the anchor output schema registered in the [[CteScope]].
 *
 * === Recursive CTE Resolution ===
 *
 * Recursive CTEs allow self-references within their definition. Consider this example:
 * {{{
 * WITH RECURSIVE rcte(n) AS (
 *   SELECT 1
 *   UNION ALL
 *   SELECT n + 1 FROM rcte WHERE n < 5
 * )
 * SELECT * FROM rcte;
 * }}}
 *
 * The [[CteScope]] is created with [[recursiveCteParameters]] containing a unique ID used by
 * [[UnionLoopRef]] and [[UnionLoop]], the CTE name for matching self-references, and optionally
 * a maximum recursion depth.
 *
 * In the above case, 'n' is given as a [[UnresolvedSubqueryColumnAliases]], which is registered for
 * the current scope if it allows recursion. This is done because 'n' appears as a self reference
 * and thus must be defined in the output of the [[UnionLoopRef]].
 *
 * When a self-reference is first encountered during CTE substitution, a [[UnionLoopRef]] with empty
 * output is returned since the anchor schema is not yet known. In the resolution, after the anchor
 * branch is resolved, its output schema is registered on the CTE scope in the mutable state. Since
 * at the time of resolution of the first child of the [[Union]], it is unknown whether the CTE is
 * indeed recursive, we register the anchor output for every CTE that allows recursion following the
 * pattern of a [[Union]] with two children. When the recursion branch is resolved and a self
 * reference is found, the [[UnionLoopRef]] uses the registered anchor output as its output format,
 * and, in the case column names are defined by alternative names from
 * [[UnresolvedSubqueryColumnAliases]], a copy of this node is placed above the self reference to
 * ensure the names exist in the appropriate context.
 *
 * After resolution completes, if a self-reference occurred, a [[UnionLoop]] is created instead
 * of a regular [[Union]]. It is then checked that this follows one of the predefined allowed
 * schemas for an rCTE, which is a [[UnionLoop]] node with optionally a [[WithCTE]] and an
 * [[UnresolvedSubqueryColumnAliases]] node.
 *
 * [[RecursiveCteState]] holds the mutable resolution state: the registered anchor output schema,
 * column aliases from the CTE definition, and a flag tracking whether a self-reference occurred.
 *
 * Recursive CTEs require [[WithCTE]] nodes to be placed immediately rather than merged. This is
 * for plan compatibility with the fixed-point Analyzer, which expects [[WithCTE]] nodes at
 * specific positions in the plan tree.
 * For example:
 * {{{
 * WITH RECURSIVE t1 AS (
 *   SELECT 1 AS level
 *   UNION ALL (
 *     WITH t2 AS (SELECT level + 1 FROM t1 WHERE level < 10)
 *     SELECT * FROM t2
 *   )
 * )
 * SELECT * FROM t1;
 * }}}
 * The immediate placement of [[WithCTE]] for t2 ensures the resolved plan structure matches what
 * the fixed-point Analyzer produces.
 *
 * During resolution of set operations, the anchor output must be registered for recursive CTEs.
 * However, the anchor branch may itself contain nested UNION ALLs, which creates a challenge:
 * {{{
 * WITH RECURSIVE t(n, m) AS (
 *   SELECT n, m
 *   FROM (SELECT 1 AS n UNION ALL SELECT 2) a
 *        CROSS JOIN (SELECT 1 AS m) b
 *   UNION ALL
 *   SELECT n + 1, 2 * m FROM t WHERE n < 5
 * )
 * SELECT * FROM t;
 * }}}
 *
 * In this query, there are two UNION ALLs:
 *  1. The outer UNION ALL between the anchor and recursion branches (this is the recursive union)
 *  2. The inner UNION ALL within the anchor branch (SELECT 1 UNION ALL SELECT 2)
 *
 * When [[SetOperationLikeResolver]] resolves unions, it attempts to register anchor output for
 * each union's first child. Without depth tracking, the inner UNION ALL would register its output
 * as the anchor schema, when we actually need the outer anchor's schema (the CROSS JOIN result).
 *
 * To handle this, the resolution uses [[RecursiveCteState.expectedUnionDepth]] to track the
 * canonical depth at which operations should occur:
 *  1. [[CteRegistry.trySetExpectedUnionDepth]] is called when starting to resolve each [[Union]].
 *     The first [[Union]] to resolve (the outermost) sets the expected depth.
 *  2. [[CteRegistry.tryRegisterAnchorOutput]] only registers if the current depth matches the
 *     expected depth (accounting for a child scope offset, since anchor registration happens
 *     during child resolution).
 *  3. [[CteRegistry.tryPlaceUnionLoop]] only places a [[UnionLoop]] if the current depth matches
 *     the expected depth.
 *
 * This ensures that nested [[Union]] nodes cannot interfere with anchor registration or
 * [[UnionLoop]] placement. The depth is measured as the number of stack iterations to reach the
 * recursive CTE scope.
 *
 * The full stack traversal is necessary because the recursive CTE scope's position in the stack
 * varies depending on query structure. In particular, [[insideRecursiveCteScope]] causes nested
 * [[WithCTE]] nodes to be placed immediately, creating additional scopes between the current
 * resolution position and the recursive CTE scope.
 *
 * @param isRoot This marks the place where [[WithCTE]] has to be placed with all the merged
 *   [[CTERelationDef]] that were collected under it. It will be true for root query, [[View]]s
 *   and expression subqueries.
 * @param isOpaque This flag makes this [[CteScope]] opaque for [[CTERelationDef]] lookups. It will
 *   be true for root query and [[View]]s.
 * @param recursiveCteParameters Parameters specific to recursive CTEs. When defined, this scope
 *   corresponds to a recursive CTE with the given id, name, and optional max depth.
 * @param isInsideRecursiveCteScope Keeps track whether this scope is inside a recursive CTE. Used
 *   for stopping traversing the stack for rCTE related operations when not resolving an rCTE. Also
 *   used to force place [[WithCTE]] nodes while in this scope, as opposed to collecting them into
 *   one [[WithCTE]] as the root of the plan.
 */
class CteScope(
    val isRoot: Boolean,
    val isOpaque: Boolean,
    val recursiveCteParameters: Option[RecursiveCteParameters] = None,
    val isInsideRecursiveCteScope: Boolean = false) {

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
   * Mutable state for recursive CTEs. Only present when this scope corresponds to a CTE that allows
   * recursion.
   */
  private val recursiveCteState: Option[RecursiveCteState] =
    recursiveCteParameters.map(_ => new RecursiveCteState)

  /**
   * Flag to track whether recursive CTE references are allowed in this scope.
   */
  private var hasRecursiveSelfReference: Boolean = false

  /**
   * Mark CTE Scope as having a self-reference inside it for legality checks of self-reference
   * placement. Recursive CTE self-references may not be inside [[Aggregate]] nodes as well as on
   * some sides in [[Join]] nodes. Also, whether they are allowed inside [[Window]] and [[Sort]]
   * nodes isn't clearly defined behavior and the legality of this is determined by a flag.
   */
  def setRecursiveSelfReference(): Unit = {
    hasRecursiveSelfReference = true
  }

  /**
   * Check if scope has recursive CTE self-reference.
   */
  def failIfScopeHasSelfReference(): Unit = {
    if (hasRecursiveSelfReference) {
      throw new org.apache.spark.sql.AnalysisException(
        errorClass = "INVALID_RECURSIVE_REFERENCE.PLACE",
        messageParameters = Map.empty
      )
    }
  }

  def referencedRecursively: Boolean = recursiveCteState.exists(_.referencedRecursively)

  /**
   * Checks if the given depth matches the expected union depth for this recursive CTE scope.
   */
  def isExpectedUnionDepth(depth: Int): Boolean = {
    recursiveCteState.exists(_.expectedUnionDepth.contains(depth))
  }

  /**
   * Checks if the given depth is appropriate for anchor registration. Anchor registration happens
   * during child resolution with an extra scope pushed, so we check against expectedUnionDepth - 1.
   */
  def isExpectedAnchorDepth(depth: Int): Boolean = {
    recursiveCteState.exists(_.expectedUnionDepth.contains(depth - 1))
  }

  /**
   * Optionally put [[WithCTE]] on top of the `resolvedOperator`. This is done just for the root
   * scopes and for forced inlining of CTEs inside rCTEs in the context of a correct
   * `unresolvedOperator`. Return the `resolvedOperator` otherwise.
   */
  def tryPutWithCTE(unresolvedOperator: LogicalPlan, resolvedOperator: LogicalPlan): LogicalPlan = {
    if (!knownCtes.isEmpty && (isRoot || isInsideRecursiveCteScope) &&
      isSuitableOperatorForWithCTE(unresolvedOperator)) {
      WithCTE(resolvedOperator, knownCtes.asScala.toSeq)
    } else {
      resolvedOperator
    }
  }

  /**
   * Sets the expected union depth for a recursive CTE. This should be called when first
   * encountering a UNION in a recursive CTE context to establish the canonical depth at which
   * operations should occur. Only sets the depth if it hasn't been set yet.
   */
  def trySetExpectedUnionDepth(depth: Int): Unit = {
    recursiveCteState.foreach { state =>
      if (state.expectedUnionDepth.isEmpty) {
        state.expectedUnionDepth = Some(depth)
      }
    }
  }

  /**
   * Register the anchor output schema for a recursive CTE. This establishes the schema that the
   * recursion branch must conform to. Only registered if this scope allows recursion, the anchor
   * output hasn't been registered yet, and the depth is appropriate for anchor registration.
   */
  def tryRegisterAnchorOutput(anchorOutput: Seq[Attribute], depth: Int): Unit = {
    recursiveCteState.foreach { state =>
      if (state.anchorOutput.isEmpty && isExpectedAnchorDepth(depth)) {
        state.anchorOutput = Some(anchorOutput)
      }
    }
  }

  /**
   * Register column names for a recursive CTE. These are used when creating a [[Project]] node
   * around [[UnionLoopRef]] to rename columns to the specified aliases.
   *
   * Example query using column aliases:
   * {{{
   * WITH RECURSIVE t(n) AS (
   *   SELECT 1
   *   UNION ALL
   *   SELECT n + 1 FROM t WHERE n < 10
   * )
   * SELECT * FROM t;
   * }}}
   *
   * Here, "n" is registered as a column name, so self-references to `t` can resolve the column `n`
   * even though the anchor output might have a different column name (e.g., "1" as a literal).
   */
  def tryRegisterColumnNames(columnNames: Seq[String]): Unit = {
    recursiveCteState.foreach { state =>
      if (state.columnNames.isEmpty) {
        state.columnNames = Some(columnNames)
      }
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
   * Checks if a CTE with the given name exists in this scope. This is a simplified existence check
   * used by [[IdentifierAndCteSubstitutor]] to determine if an [[UnresolvedRelation]] should be
   * replaced with an [[UnresolvedCteRelationRef]].
   */
  def cteExists(name: String): Boolean = {
    visibleCtes.get(name).isDefined || {
      recursiveCteParameters.exists(params => params.cteName == name)
    }
  }

  /**
   * Gets a visible CTE definition by its name. In case of Recursive CTEs, attempts to see whether
   * the CTE the function gets is supposed to be self reference. For self-references, this returns
   * a [[UnionLoopRef]] with the anchor output that must have been registered previously.
   */
  def getCte(name: String): Option[LogicalPlan] = {
    val cte = visibleCtes.get(name)
    cte.orElse {
      recursiveCteParameters match {
        case Some(params) if params.cteName == name =>
          recursiveCteState.foreach(_.referencedRecursively = true)
          val ref = UnionLoopRef(
            params.cteId,
            recursiveCteState
              .flatMap(_.anchorOutput)
              .get
              .map(_.newInstance().withNullability(true)),
            accumulated = false
          )
          Some(
            recursiveCteState
              .flatMap(_.columnNames)
              .map(UnresolvedSubqueryColumnAliases(_, ref))
              .getOrElse(ref)
          )
        case _ =>
          None
      }
    }
  }

  /**
   * Merge the state from a child scope. We transfer all the known CTE definitions to later merge
   * them in one [[WithCTE]]. Root or forcibly inlined scopes terminate this chain, since they have
   * their own [[WithCTE]].
   */
  def mergeChildScope(childScope: CteScope): Unit = {
    if (!childScope.isRoot && !childScope.isInsideRecursiveCteScope) {
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
   * This is a [[pushScope]] variant specifically designed to be called above multi-child
   * operator children resolution (e.g. for children of a [[Join]] or [[Union]]).
   *
   * The `isRoot` flag has to be propagated from the parent scope if all of the following
   * conditions are met:
   *  - The current scope is a root scope
   *  - The multi-child `unresolvedOperator` IS NOT suitable to place a [[WithCTE]]
   *  - Some operator in `unresolvedChild` subtree IS suitable to place a [[WithCTE]].
   */
  def pushScopeForMultiChildOperator[R](
      unresolvedOperator: LogicalPlan,
      unresolvedChild: LogicalPlan
  ): Unit = {
    pushScope(
      isRoot = currentScope.isRoot &&
        !CteRegistry.isSuitableMultiChildOperatorForWithCTE(unresolvedOperator) &&
        CteRegistry.hasSuitableOperatorForWithCTEInSubtree(unresolvedChild)
    )
  }

  /**
   * Push new scope to the stack. This is used by the [[Resolver]] to create a new scope for
   * each WITH clause.
   */
  def pushScope(
      isRoot: Boolean = false,
      isOpaque: Boolean = false,
      recursiveCteParameters: Option[RecursiveCteParameters] = None,
      isInsideRecursiveCteScope: Boolean = currentScope.isInsideRecursiveCteScope): Unit = {
    stack.push(
      new CteScope(
        isRoot = isRoot,
        isOpaque = isOpaque,
        recursiveCteParameters = recursiveCteParameters,
        isInsideRecursiveCteScope = isInsideRecursiveCteScope
      )
    )
  }

  /**
   * Pop current scope from the stack.
   */
  def popScope(): Unit = {
    val childScope = stack.pop()
    currentScope.mergeChildScope(childScope)
  }

  /**
   * Check if a CTE with the given name exists in the scope stack. Used by
   * [[IdentifierAndCteSubstitutor]] to determine if an [[UnresolvedRelation]] should be replaced
   * with an [[UnresolvedCteRelationRef]].
   */
  def cteExists(name: String): Boolean = {
    traverseStack(scope => if (scope.cteExists(name)) Some(true) else None).isDefined
  }

  /**
   * Resolve `name` to a visible [[CTERelationDef]] or [[UnionLoopRef]]. The upper definitions are
   * also visible, and the lowest of them takes precedence. Opaque scopes terminate the lookup
   * (e.g. [[View]] boundary).
   *
   * For recursive CTEs with column aliases (e.g., `WITH RECURSIVE t(n) AS (...)`), may return
   * [[UnresolvedSubqueryColumnAliases]] wrapping a [[UnionLoopRef]], which will be resolved
   * later by [[Resolver.resolveSubqueryColumnAliases]].
   *
   * If a recursive self-reference is found (returns a [[UnionLoopRef]]), marks all traversed
   * scopes with [[setRecursiveSelfReference]] for later validation by operators that disallow
   * recursive references (e.g., [[Aggregate]], [[Sort]], [[Window]], [[Join]]).
   */
  def resolveCteName(name: String): Option[LogicalPlan] = {
    val result = traverseStack { scope =>
      scope.getCte(name)
    }

    result match {
      case Some(_: UnionLoopRef) | Some(UnresolvedSubqueryColumnAliases(_, _: UnionLoopRef)) =>
        traverseStack { scope =>
          scope.setRecursiveSelfReference()
          scope.getCte(name)
        }
      case _ =>
    }

    result
  }

  /**
   * Sets the expected union depth for a recursive CTE. This should be called when first
   * encountering a UNION in a recursive CTE context to establish the canonical depth at which
   * operations should occur. This prevents nested UNIONs from interfering with anchor registration
   * and UnionLoop placement.
   */
  def trySetExpectedUnionDepth(): Unit = {
    if (currentScope.isInsideRecursiveCteScope) {
      traverseStackWithDepth { (scope, depth) =>
        if (scope.recursiveCteParameters.isDefined) {
          scope.trySetExpectedUnionDepth(depth)
          Some(())
        } else {
          None
        }
      }
    }
  }

  /**
   * Walks up the scope stack to find the first CTE scope (meaning it has a CteId). Registers the
   * output of the anchor of a recursive CTE in this scope. This output will be registered, but
   * unused if it's not a recursive CTE. Only registers if the current depth matches the expected
   * union depth.
   */
  def tryRegisterAnchorOutput(anchorOutput: Seq[Attribute]): Unit = {
    if (currentScope.isInsideRecursiveCteScope) {
      traverseStackWithDepth { (scope, depth) =>
        if (scope.recursiveCteParameters.isDefined) {
          scope.tryRegisterAnchorOutput(anchorOutput, depth)
          Some(())
        } else {
          None
        }
      }
    }
  }

  /**
   * Attempts to create a UnionLoop operator for a recursive CTE. Walks up the scope stack to find
   * a scope with a CTE ID that was referenced recursively. If found, creates a [[UnionLoop]] with
   * the provided children (anchor and recursion branches). The output ExprIds are derived from the
   * anchor output to ensure consistent IDs across iterations. Only places UnionLoop if the current
   * depth matches the expected union depth.
   */
  def tryPlaceUnionLoop(
      anchor: LogicalPlan,
      anchorOutput: Seq[Attribute],
      recursion: LogicalPlan,
      expressionIdAssigner: ExpressionIdAssigner): Option[UnionLoop] = {
    if (currentScope.isInsideRecursiveCteScope) {
      traverseStackWithDepth { (scope, depth) =>
        scope.recursiveCteParameters match {
          case Some(params) if scope.referencedRecursively && scope.isExpectedUnionDepth(depth) =>
            val outputAttrs = anchorOutput.map { attr =>
              val newAttr = attr.newInstance()
              expressionIdAssigner.mapExpression(newAttr, allowUpdatesForAttributeReferences = true)
            }

            Some(
              UnionLoop(
                id = params.cteId,
                anchor = anchor,
                recursion = recursion,
                outputAttrIds = outputAttrs.map(_.exprId),
                maxDepth = params.maxDepth
              )
            )
          case _ => None
        }
      }
    } else {
      None
    }
  }

  /**
   * Traverses the scope stack from bottom (current) to top (root), applying a function to each
   * scope. Stops when reaching an opaque scope (e.g., view boundary) or when the function returns
   * Some value.
   *
   * This is a simplified version of [[traverseStackWithDepth]] for operations that don't need
   * depth tracking.
   */
  private def traverseStack[T](fn: CteScope => Option[T]): Option[T] = {
    traverseStackWithDepth { (scope, _) =>
      fn(scope)
    }
  }

  /**
   * Traverses the scope stack from bottom (current) to top (root), applying a function to each
   * scope along with its depth. Stops when reaching an opaque scope (e.g., view boundary) or when
   * the function returns Some value.
   *
   * The depth parameter represents the number of stack iterations from the current scope (depth 0)
   * upward. This is used for depth-aware operations in recursive CTE resolution.
   */
  private def traverseStackWithDepth[T](fn: (CteScope, Int) => Option[T]): Option[T] = {
    val iter = stack.iterator
    var done = false
    var result: Option[T] = None
    var currentDepth = 0
    while (iter.hasNext() && !done) {
      val scope = iter.next()
      done = scope.isOpaque

      fn(scope, currentDepth) match {
        case Some(value) =>
          result = Some(value)
          done = true
        case None =>
      }
      currentDepth += 1
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
