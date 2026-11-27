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

import org.apache.spark.sql.catalyst.analysis.{
  ExpressionWithUnresolvedIdentifier,
  IdentifierResolution,
  NameParameterizedQuery,
  PlanWithUnresolvedIdentifier,
  UnresolvedRelation,
  UnresolvedStarExceptOrReplace
}
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  NamedExpression,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{
  CTERelationDef,
  LogicalPlan,
  OneRowRelation,
  SubqueryAlias,
  UnresolvedWith
}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.{
  PLAN_EXPRESSION,
  PLAN_WITH_UNRESOLVED_IDENTIFIER,
  UNRESOLVED_IDENTIFIER,
  UNRESOLVED_RELATION,
  UNRESOLVED_STAR_EXCEPT_OR_REPLACE,
  UNRESOLVED_WITH
}
import org.apache.spark.sql.errors.QueryErrorsBase

/**
 * The [[PlanPreprocessor]] is responsible for substituting the IDENTIFIERs and CTE references
 * in the unresolved logical plan before the actual resolution starts (specifically before
 * metadata resolution). This is important for SQL features like WITH (that could confuse
 * [[MetadataResolver]] with extra [[UnresolvedRelation]]s) or IDENTIFIER (that "hides" the
 * actual [[UnresolvedRelation]]s).
 *
 * We only recurse into the plan if [[PlanPreprocessor.NODES_OF_INTEREST]] are present. This is
 * done so that [[PlanPreprocessor]] is fast and not invasive.
 *
 * With `resolveExpressionIdentifiersInRootScope`, an expression IDENTIFIER whose name is not
 * resolved yet is also materialized from a name resolved in an isolated root scope, so the
 * expression it builds is visible to plan checks (e.g. guards):
 * {{{
 * DECLARE function_name = 'my_udf';
 * SELECT IDENTIFIER(function_name)(col1) FROM t;
 * -- 'Project [unresolvedalias('my_udf('col1))] instead of an opaque IDENTIFIER placeholder
 * }}}
 * Columns are not visible in that scope, while resolution prefers a same-named column over a SQL
 * variable, so this mode only predicts what resolution builds and must not feed resolution.
 */
class PlanPreprocessor(resolver: Resolver, resolveExpressionIdentifiersInRootScope: Boolean)
    extends QueryErrorsBase
    with ResolverMetricTracker {
  private var cteRegistry = new CteRegistry

  /**
   * This is the main entry point to the substitution process. It takes `unresolvedPlan` and
   * substitutes CTEs and IDENTIFIERs that would otherwise confuse the metadata resolution process.
   *
   * [[CteRegistry]] has to be reset for each new invocation, because CTEs in views are analyzed
   * in isolation.
   */
  def substitutePlan(unresolvedPlan: LogicalPlan): LogicalPlan = {
    recordProfile("substitutePlan") {
      cteRegistry = new CteRegistry

      substitute(unresolvedPlan)
    }
  }

  private def substitute(unresolvedPlan: LogicalPlan): LogicalPlan = {
    unresolvedPlan match {
      case unresolvedWith: UnresolvedWith =>
        handleWith(unresolvedWith)
      case unresolvedRelation: UnresolvedRelation =>
        handleUnresolvedRelation(unresolvedRelation)
      case nameParameterizedQuery: NameParameterizedQuery
          if nameParameterizedQuery.child.containsAnyPattern(
            PlanPreprocessor.NODES_OF_INTEREST: _*) =>
        handleNameParameterizedQuery(nameParameterizedQuery)
      case planWithUnresolvedIdentifier: PlanWithUnresolvedIdentifier =>
        handlePlanWithUnresolvedIdentifier(planWithUnresolvedIdentifier)
      case _ =>
        handleOperator(unresolvedPlan)
    }
  }

  /**
   * Handle [[UnresolvedWith]] operator. WITH clause produces unresolved relations in the plan,
   * that are not actual tables or views, but are just potential [[CTERelationRef]]s. To correctly
   * detect those CTE references we use the [[CteRegistry]] framework. The actual CTE resolution
   * is left to the main algebraic pass in the [[Resolver]] - here we replace the
   * [[UnresolvedRelation]]s with [[UnresolvedCteRelationRef]] to avoid issuing useless catalog
   * RPCs later on in the [[MetadataResolver]].
   *
   * We need to use the [[CteRegistry]] framework, because whether the relation is a CTE reference
   * or not depends on its position in the logical plan tree:
   * {{{
   * CREATE TABLE rel2 (col1 INT);
   *
   * WITH rel1 AS (
   *   WITH rel2 AS (
   *     SELECT 1
   *   )
   *   SELECT * FROM rel2 -- `rel2` is a CTE reference
   * )
   * SELECT * FROM rel2 -- `rel2` is a table reference
   * }}}
   *
   * For each CTE definition, a scope is pushed with the CTE name to enable self-reference
   * detection for recursive CTEs. During substitution, we use dummy values for cteId (0) since
   * the actual ID is assigned later during resolution.
   */
  private def handleWith(unresolvedWith: UnresolvedWith): LogicalPlan = {
    val cteRelationsAfterSubstitution = unresolvedWith.cteRelations.map { cteRelation =>
      val cteName = cteRelation.name

      val recursiveCteParams = if (unresolvedWith.allowRecursion) {
        Some(RecursiveCteParameters(cteId = 0, cteName = cteName, maxDepth = None))
      } else {
        None
      }

      cteRegistry.pushScope(recursiveCteParameters = recursiveCteParams)

      val ctePlanAfter = try {
        substitute(cteRelation.plan).asInstanceOf[SubqueryAlias]
      } finally {
        cteRegistry.popScope()
      }

      cteRegistry.currentScope.registerCte(cteName, CTERelationDef(ctePlanAfter))

      cteRelation.copy(plan = ctePlanAfter)
    }

    cteRegistry.pushScope()

    val childAfterSubstitution = try {
      substitute(unresolvedWith.child)
    } finally {
      cteRegistry.popScope()
    }

    val result = withOrigin(unresolvedWith.origin) {
      unresolvedWith.copy(
        child = childAfterSubstitution,
        cteRelations = cteRelationsAfterSubstitution
      )
    }
    result.copyTagsFrom(unresolvedWith)
    result
  }

  /**
   * Preprocesses a parameterized child with its named bindings in scope. This node survives parser
   * substitution only in legacy constants-only mode.
   */
  private def handleNameParameterizedQuery(
      nameParameterizedQuery: NameParameterizedQuery): LogicalPlan = {
    resolver.withNameParameterizedQueryParameters(nameParameterizedQuery) {
      handleOperator(nameParameterizedQuery)
    }
  }

  /**
   * Handle [[UnresolvedRelation]] operator, which could be a CTE reference. If that's the case, we
   * replace it with [[UnresolvedCteRelationRef]].
   */
  private def handleUnresolvedRelation(unresolvedRelation: UnresolvedRelation): LogicalPlan = {
    if (unresolvedRelation.multipartIdentifier.size == 1 &&
      cteRegistry.cteExists(unresolvedRelation.multipartIdentifier.head)) {
      val result = withOrigin(unresolvedRelation.origin) {
        UnresolvedCteRelationRef(unresolvedRelation.multipartIdentifier.head)
      }
      result.copyTagsFrom(unresolvedRelation)
      result
    } else {
      unresolvedRelation
    }
  }

  /**
   * Handles [[PlanWithUnresolvedIdentifier]] by resolving its name expression in an isolated root
   * context, evaluating it, and building the named plan. The expression may contain functions
   * supported by the resolver and SQL variables, but cannot reference columns.
   */
  private def handlePlanWithUnresolvedIdentifier(
      planWithUnresolvedIdentifier: PlanWithUnresolvedIdentifier): LogicalPlan = {
    val identifierParts = evaluateIdentifierInRootScope(
      identifierExpression = planWithUnresolvedIdentifier.identifierExpr,
      parentOperator = planWithUnresolvedIdentifier
    )

    val builtPlan = withOrigin(planWithUnresolvedIdentifier.origin) {
      planWithUnresolvedIdentifier.planBuilder(
        identifierParts,
        planWithUnresolvedIdentifier.children
      )
    }

    val substitutedPlan = substitute(builtPlan)
    substitutedPlan.copyTagsFrom(planWithUnresolvedIdentifier)
    substitutedPlan
  }

  /**
   * Evaluates an IDENTIFIER name expression into name parts after substituting its subqueries and
   * resolving it in an isolated root scope under `parentOperator`.
   */
  private def evaluateIdentifierInRootScope(
      identifierExpression: Expression,
      parentOperator: LogicalPlan): Seq[String] = {
    val identifierExpressionAfterSubstitution =
      identifierExpression.transformUpWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
        case subqueryExpression: SubqueryExpression =>
          substituteIdentifierSubqueryPlan(subqueryExpression)
      }
    val resolvedIdentifierExpression = resolver.resolveIdentifierExpressionRoot(
      identifierExpression = identifierExpressionAfterSubstitution,
      parentOperator = parentOperator
    )
    IdentifierResolution.evalIdentifierExpr(resolvedIdentifierExpression)
  }

  /**
   * Handle `unresolvedOperator` generically. We use the [[CteRegistry]] framework to recurse into
   * its children and subquery expressions.
   */
  private def handleOperator(unresolvedOperator: LogicalPlan): LogicalPlan = {
    val operatorAfterSubstitution = unresolvedOperator match {
      case operator
          if !operator.containsAnyPattern(PlanPreprocessor.NODES_OF_INTEREST: _*) =>
        operator

      case operator if operator.children.size > 1 =>
        val newChildren = operator.children.map { child =>
          cteRegistry.pushScopeForMultiChildOperator(operator, child)

          try {
            substitute(child)
          } finally {
            cteRegistry.popScope()
          }
        }
        operator.withNewChildren(newChildren)

      case operator if operator.children.size == 1 =>
        val newChildren = Seq(substitute(operator.children.head))
        operator.withNewChildren(newChildren)

      case operator =>
        operator
    }

    substituteExpressions(operatorAfterSubstitution)
  }

  /** Materializes expression IDENTIFIERs so metadata lookup and plan checks can see their names. */
  private def substituteExpressions(operator: LogicalPlan): LogicalPlan = {
    operator.transformExpressionsWithPruning(
      _.containsAnyPattern(
        PLAN_EXPRESSION,
        UNRESOLVED_IDENTIFIER,
        UNRESOLVED_STAR_EXCEPT_OR_REPLACE
      )
    )(substituteExpression)
  }

  /**
   * Substitution rule applied to an operator's expression trees. It is also applied explicitly to
   * the replacement expressions of an [[UnresolvedStarExceptOrReplace]] (see
   * [[substituteStarExceptOrReplaceReplacements]]), which are not exposed as children because the
   * star is a leaf expression.
   *
   * An expression IDENTIFIER is materialized when its name is already resolved, or, with
   * `resolveExpressionIdentifiersInRootScope`, from its name resolved under a [[OneRowRelation]]
   * placeholder so the enclosing operator cannot change the name lookup.
   */
  private def substituteExpression: PartialFunction[Expression, Expression] = {
    case subqueryExpression: SubqueryExpression =>
      substituteIdentifierSubqueryPlan(subqueryExpression)
    case expressionWithUnresolvedIdentifier: ExpressionWithUnresolvedIdentifier
        if expressionWithUnresolvedIdentifier.identifierExpr.resolved =>
      val identifierParts = IdentifierResolution.evalIdentifierExpr(
        expressionWithUnresolvedIdentifier.identifierExpr
      )
      IdentifierResolution.buildExpressionFromIdentifierParts(
        expressionWithUnresolvedIdentifier = expressionWithUnresolvedIdentifier,
        identifierParts = identifierParts
      )
    case expressionWithUnresolvedIdentifier: ExpressionWithUnresolvedIdentifier
        if resolveExpressionIdentifiersInRootScope =>
      val identifierParts = evaluateIdentifierInRootScope(
        identifierExpression = expressionWithUnresolvedIdentifier.identifierExpr,
        parentOperator = OneRowRelation()
      )
      IdentifierResolution.buildExpressionFromIdentifierParts(
        expressionWithUnresolvedIdentifier = expressionWithUnresolvedIdentifier,
        identifierParts = identifierParts
      )
    case starExceptOrReplace: UnresolvedStarExceptOrReplace
        if starExceptOrReplace.replacements.isDefined =>
      substituteStarExceptOrReplaceReplacements(starExceptOrReplace)
  }

  /**
   * Substitutes IDENTIFIERs and CTE references nested in the replacement expressions of an
   * [[UnresolvedStarExceptOrReplace]] (e.g. a scalar subquery selecting from `IDENTIFIER(...)` or a
   * CTE in a pipe `|> SET` target). These expressions are not children of the leaf star, so the
   * operator's expression traversal does not reach them; they are transformed here explicitly with
   * the same [[substituteExpression]] rule.
   *
   * The replacements see the enclosing CTE scopes, so a CTE reference nested in a replacement
   * subquery is rewritten to an [[UnresolvedCteRelationRef]]: an outer CTE takes precedence over
   * a same-named table, per standard scoping. This intentionally diverges from the fixed-point
   * analyzer, which does not make an enclosing WITH visible inside a star replacement (its CTE
   * substitution runs before the star is expanded) and instead resolves such a name to the
   * same-named table -- a known legacy limitation left unfixed here.
   */
  private def substituteStarExceptOrReplaceReplacements(
      starExceptOrReplace: UnresolvedStarExceptOrReplace): UnresolvedStarExceptOrReplace = {
    val substitutedReplacements = starExceptOrReplace.replacements.get.map { replacement =>
      replacement
        .transformDownWithPruning(
          _.containsAnyPattern(PLAN_EXPRESSION, UNRESOLVED_IDENTIFIER)
        )(substituteExpression)
        .asInstanceOf[NamedExpression]
    }
    val result = withOrigin(starExceptOrReplace.origin) {
      starExceptOrReplace.copy(replacements = Some(substitutedReplacements))
    }
    result.copyTagsFrom(starExceptOrReplace)
    result
  }

  private def substituteIdentifierSubqueryPlan(
      subqueryExpression: SubqueryExpression): SubqueryExpression = {
    cteRegistry.pushScope(isRoot = true)

    val newPlan = try {
      substitute(subqueryExpression.plan)
    } finally {
      cteRegistry.popScope()
    }

    val result = withOrigin(subqueryExpression.origin) {
      subqueryExpression.withNewPlan(newPlan)
    }
    result.copyTagsFrom(subqueryExpression)
    result
  }
}

object PlanPreprocessor {
  val NODES_OF_INTEREST = Seq(
    UNRESOLVED_RELATION,
    UNRESOLVED_WITH,
    PLAN_WITH_UNRESOLVED_IDENTIFIER,
    UNRESOLVED_IDENTIFIER,
    UNRESOLVED_STAR_EXCEPT_OR_REPLACE
  )
}
