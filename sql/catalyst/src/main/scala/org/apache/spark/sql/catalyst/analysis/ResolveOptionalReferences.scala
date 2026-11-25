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

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  LambdaFunction,
  LateralColumnAliasReference,
  NamedExpression,
  SubqueryExpression,
  UnaryExpression,
  Unevaluable
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  Filter,
  LateralJoin,
  LogicalPlan,
  RepartitionByExpression,
  Sort,
  UnaryNode
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.{PARAMETERIZED_QUERY, PLAN_EXPRESSION}
import org.apache.spark.sql.types.DataType

/**
 * Replaces the optional references that the fixed-point "Resolution" batch could not resolve
 * with a static NULL, and then resolves the plan again with `resolveAgain`.
 *
 * {{{
 *   -- `t` has no column `name`:
 *   SELECT id, name? FROM t;
 *   -- Project [id#1, 'name] becomes Project [id#1, null AS name#2]
 * }}}
 *
 * Only the expressions of operators with resolved children are replaced, because a reference in
 * an operator with unresolved children can still resolve once the children are resolved. The
 * rule is meant to run in a fixed-point batch: every application replaces the references of the
 * lowest operators that cannot be resolved, which can make the operators above them resolvable.
 * References that a later resolution step is still waiting for, such as the body of an unbound
 * lambda function or a lateral column alias reference, are left in place.
 *
 * The analyzer resolves the plan of a subquery expression on its own, while the operator of the
 * subquery expression is still being resolved. A reference of the subquery that is unresolved at
 * the end of that can still resolve later, e.g. once the outer query binds its parameter markers
 * or resolves the alias of a generator. The rule therefore only replaces the references of a
 * subquery plan after it could not resolve the operator of the subquery expression, which it
 * signals by wrapping the subquery plan in [[ResolveOptionalReferencesInSubquery]].
 *
 * The rule never changes the plan that it is given, which can be analyzed again later, e.g. from a
 * plan cache: its decisions are nodes of the plan that it returns.
 */
class ResolveOptionalReferences(
    resolveAgain: LogicalPlan => LogicalPlan,
    variableResolution: VariableResolution)
    extends Rule[LogicalPlan] {
  import ResolveOptionalReferences._

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case subquery: ResolveOptionalReferencesInSubquery =>
      val resolvedPlan = replaceAndResolveAgain(subquery.child)
      if (resolvedPlan.fastEquals(subquery.child)) subquery.child else subquery.copy(resolvedPlan)
    case _ if plan.resolved || isPlanOfSubquery || plan.containsPattern(PARAMETERIZED_QUERY) =>
      plan
    case _ if hasUnresolvedOptionalReferences(plan) =>
      replaceAndResolveAgain(plan)
    case _ =>
      plan
  }

  private def replaceAndResolveAgain(plan: LogicalPlan): LogicalPlan = {
    val replacedPlan = replaceUnresolvableOptionalReferences(plan)
    val resolvedPlan = if (replacedPlan.fastEquals(plan)) {
      plan
    } else {
      unwrapSubqueryPlans(resolveAgain(replacedPlan))
    }
    if (!resolvedPlan.fastEquals(plan)) {
      resolvedPlan
    } else {
      val wrappedPlan = wrapOptionalFieldsAsLastResort(plan)
      if (wrappedPlan.fastEquals(plan)) {
        plan
      } else {
        unwrapLastResortExtractValues(resolveAgain(wrappedPlan))
      }
    }
  }

  /**
   * Replaces the optional references of the lowest operators of `plan` that are not resolved, and
   * wraps the unresolved plans of their subquery expressions for the analysis of these plans.
   *
   * The traversal is top-down so that an operator is only considered while its children are the
   * ones that the "Resolution" batch resolved its references against. An operator above a
   * replaced one waits for `resolveAgain`, which can still resolve its references in other ways,
   * for example as lateral column aliases or as variables.
   */
  private def replaceUnresolvableOptionalReferences(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsDown {
      case aggregate: Aggregate
          if aggregate.childrenResolved && !aggregate.aggregateExpressions.forall(_.resolved) =>
        replaceInAggregateExpressions(aggregate)
      case lateralJoin: LateralJoin
          if lateralJoin.left.resolved && !lateralJoin.right.plan.resolved =>
        // The join condition can reference the output of the subquery, which is not known yet.
        if (hasUnresolvedOptionalReferences(lateralJoin.right.plan)) {
          lateralJoin.copy(right = wrapSubqueryPlan(lateralJoin.right))
        } else {
          lateralJoin
        }
      case operator if operator.childrenResolved && !operator.resolved =>
        lazy val attributeGroups = visibleAttributeGroups(operator)
        operator.mapExpressions(replace(_, attributeGroups, isTopLevel = true))
    }
  }

  /**
   * Replaces the optional references in the aggregate expressions of `aggregate` only, because
   * its grouping expressions can reference aliases of the aggregate expressions, which can only
   * be resolved once all the aggregate expressions are resolved.
   *
   * {{{
   *   -- `archived` in GROUP BY resolves to the alias `archived` of the SELECT list.
   *   SELECT coalesce(archived?, false) AS archived FROM t GROUP BY archived?;
   * }}}
   */
  private def replaceInAggregateExpressions(aggregate: Aggregate): Aggregate = {
    lazy val attributeGroups = visibleAttributeGroups(aggregate)
    aggregate.copy(aggregateExpressions = aggregate.aggregateExpressions.map { expression =>
      replace(expression, attributeGroups, isTopLevel = true).asInstanceOf[NamedExpression]
    })
  }

  /**
   * Replaces the optional references of `expression` that resolve to NULL against
   * `attributeGroups`.
   * A top-level reference keeps its name through an [[Alias]] of its last name part.
   */
  private def replace(
      expression: Expression,
      attributeGroups: => Seq[Seq[Attribute]],
      isTopLevel: Boolean): Expression = expression match {
    case unresolvedAttribute: UnresolvedAttribute
        if OptionalReferences.optionalNamePartIndices(unresolvedAttribute).nonEmpty &&
        OptionalReferences.resolvesToNull(
          nameParts = unresolvedAttribute.nameParts,
          optionalNamePartIndices = OptionalReferences.optionalNamePartIndices(unresolvedAttribute),
          attributeGroups = attributeGroups,
          resolver = conf.resolver,
          isVariableQualifier = variableResolution.isVariableQualifier(
            _,
            includeScopeLabels = canResolveScopeLabels)
        ) =>
      // The NULL keeps the position of the reference, e.g. for `CheckOptionalReferences`.
      CurrentOrigin.withOrigin(unresolvedAttribute.origin) {
        val nullReference = OptionalReferences.nullReference()
        if (isTopLevel) {
          OptionalReferences.implicitAlias(nullReference, unresolvedAttribute.nameParts.last)
        } else {
          nullReference
        }
      }
    case namedParameter: NamedParameter if OptionalReferences.isOptional(namedParameter) =>
      CurrentOrigin.withOrigin(namedParameter.origin)(OptionalReferences.nullReference())
    case lambdaFunction: LambdaFunction if !lambdaFunction.bound =>
      lambdaFunction
    case lateralColumnAliasReference: LateralColumnAliasReference =>
      lateralColumnAliasReference
    case subquery: SubqueryExpression if hasUnresolvedOptionalReferences(subquery.plan) =>
      wrapSubqueryPlan(subquery)
    case other if !other.resolved =>
      other.mapChildren(replace(_, attributeGroups, isTopLevel = false))
    case other =>
      other
  }

  private def wrapSubqueryPlan[T <: SubqueryExpression](subquery: T): T = subquery.plan match {
    case _: ResolveOptionalReferencesInSubquery => subquery
    case plan => subquery.withNewPlan(ResolveOptionalReferencesInSubquery(plan)).asInstanceOf[T]
  }

  /**
   * Unwraps the subquery plans of `plan` that are still wrapped because the analyzer did not
   * resolve them again.
   */
  private def unwrapSubqueryPlans(plan: LogicalPlan): LogicalPlan = {
    plan.resolveExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
      case subquery: SubqueryExpression =>
        subquery.plan match {
          case wrapped: ResolveOptionalReferencesInSubquery => subquery.withNewPlan(wrapped.child)
          case _ => subquery
        }
    }
  }

  /**
   * Wraps the optional field extractions of the lowest operators of `plan` that are not resolved
   * in [[LastResortExtractValue]].
   */
  private def wrapOptionalFieldsAsLastResort(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsDown {
      case operator if operator.childrenResolved && !operator.resolved =>
        operator.mapExpressions(wrapOptionalFields)
    }
  }

  private def wrapOptionalFields(expression: Expression): Expression = expression match {
    case lastResort: LastResortExtractValue =>
      lastResort
    case extractValue: UnresolvedExtractValue if OptionalReferences.isOptional(extractValue) =>
      // The children are wrapped first, so the wrapped extraction is still an extraction.
      LastResortExtractValue(
        extractValue.mapChildren(wrapOptionalFields).asInstanceOf[UnresolvedExtractValue])
    case other if !other.resolved =>
      other.mapChildren(wrapOptionalFields)
    case other =>
      other
  }

  /** Unwraps the extractions of `plan` that the analyzer did not resolve. */
  private def unwrapLastResortExtractValues(plan: LogicalPlan): LogicalPlan = {
    plan.resolveExpressions {
      case LastResortExtractValue(extractValue) => extractValue
    }
  }

  /**
   * Returns the groups of attributes that the name resolution of `operator` can match a multipart
   * name against, see `OptionalReferences.firstUnmatchedNamePartIndex`: the output of its children,
   * with the output of the subquery of a lateral join, then the output of each operator below the
   * child of a `Sort`, a `Filter`, DISTRIBUTE BY, or QUALIFY, or the input of the aggregate of
   * HAVING, and then the attributes of the outer query of a subquery.
   */
  private def visibleAttributeGroups(operator: LogicalPlan): Seq[Seq[Attribute]] = {
    val localAttributes = operator.children.flatMap(namedAttributes) ++ (operator match {
      case lateralJoin: LateralJoin => namedAttributes(lateralJoin.right.plan)
      case _ => Seq.empty
    })
    val attributeGroupsFromBelow = operator match {
      case sort: Sort => attributeGroupsForMissingReferences(sort.child)
      case filter: Filter => attributeGroupsForMissingReferences(filter.child)
      case repartition: RepartitionByExpression =>
        attributeGroupsForMissingReferences(repartition.child)
      case qualify: UnresolvedQualify => attributeGroupsForMissingReferences(qualify.child)
      case UnresolvedHaving(_, aggregate: Aggregate) => Seq(namedAttributes(aggregate.child))
      case _ => Seq.empty
    }
    val outerAttributeGroups = AnalysisContext.get.outerPlan.toSeq
      .flatMap(ColumnResolutionHelper.outerReferenceSources)
      .map(_.children.flatMap(namedAttributes))
    localAttributes +: (attributeGroupsFromBelow ++ outerAttributeGroups)
  }

  private def namedAttributes(plan: LogicalPlan): Seq[Attribute] =
    plan.output ++ plan.metadataOutput

  /**
   * Returns the output of each operator below `plan` that a `Sort`, a `Filter`, DISTRIBUTE BY, or
   * QUALIFY on top of `plan` can reference as missing attributes: those that
   * `ColumnResolutionHelper.resolveExprsAndAddMissingAttrs` resolves one operator after the other
   * and adds to the output of `plan`.
   */
  private def attributeGroupsForMissingReferences(plan: LogicalPlan): Seq[Seq[Attribute]] =
    plan match {
      case unary: UnaryNode if ColumnResolutionHelper.canAddMissingAttrsThrough(unary) =>
        namedAttributes(unary.child) +: attributeGroupsForMissingReferences(unary.child)
      case _ =>
        Seq.empty
    }
}

object ResolveOptionalReferences {

  /**
   * Returns true if `plan` or the plan of one of its subquery expressions has an optional
   * reference that is not resolved and that the rule can still replace or wrap: an attribute, a
   * named parameter marker, or a field extraction. Other unresolved nodes with an optional marker,
   * such as a [[TempResolvedColumn]] that HAVING or ORDER BY tried, hold a reference that was
   * found, and the rule leaves them to the rules that report their errors. A resolved operator,
   * expression, or subquery plan has no unresolved reference, so the search does not enter it.
   */
  private def hasUnresolvedOptionalReferences(plan: LogicalPlan): Boolean = {
    !plan.resolved &&
    (plan.expressions.exists(isOrHasUnresolvedOptionalReference) ||
    plan.children.exists(hasUnresolvedOptionalReferences))
  }

  private def isOrHasUnresolvedOptionalReference(expression: Expression): Boolean = {
    !expression.resolved && (expression match {
      case attribute: UnresolvedAttribute =>
        OptionalReferences.optionalNamePartIndices(attribute).nonEmpty
      case namedParameter: NamedParameter =>
        OptionalReferences.isOptional(namedParameter)
      case extractValue: UnresolvedExtractValue if OptionalReferences.isOptional(extractValue) =>
        true
      case subquery: SubqueryExpression =>
        hasUnresolvedOptionalReferences(subquery.plan) ||
        subquery.children.exists(isOrHasUnresolvedOptionalReference)
      case other =>
        other.children.exists(isOrHasUnresolvedOptionalReference)
    })
  }

  /**
   * Whether a label of a SQL script can qualify a name. It cannot in the query of a view that is
   * read or defined, or in the body of a persistent SQL function that is called or created: they
   * cannot see the variables of a script, so a definition that depends on a label fails when it
   * is created rather than when it is read or called.
   */
  private def canResolveScopeLabels: Boolean = {
    val context = AnalysisContext.get
    context.catalogAndNamespace.isEmpty && !context.isViewDefinition &&
      !context.isPersistentSQLFunctionBody
  }

  private def isPlanOfSubquery: Boolean = AnalysisContext.get.isSubqueryPlan

  /**
   * Returns true if `plan` is the plan of a subquery expression that the analyzer is resolving on
   * its own and that still has unresolved optional references. [[ResolveOptionalReferences]] only
   * replaces these when it handles the operator of the subquery expression, so the rules that
   * finish the analysis of a plan must leave its pending state alone until then.
   */
  def isUnfinishedSubqueryPlan(plan: LogicalPlan): Boolean =
    isPlanOfSubquery && !plan.resolved && hasUnresolvedOptionalReferences(plan)
}

/**
 * Wraps an optional [[UnresolvedExtractValue]] that no name resolution step of its operator could
 * resolve, so that its missing field is NULL in the next step that resolves its child. Until then,
 * a step whose errors are not final leaves the extraction unresolved, like the same extraction
 * without `?`, because a later step can still resolve it. For example, ORDER BY resolves names
 * against the SELECT list before the columns of the FROM clause:
 *
 * {{{
 *   -- `t.arr` is an ARRAY<STRUCT<a INT, b INT>>, so ORDER BY sorts by its field `b`.
 *   SELECT array(named_struct('a', 1)) AS arr FROM t ORDER BY arr[0].b?;
 * }}}
 *
 * [[ResolveOptionalReferences]] adds the wrapper to the plan that it returns and removes the ones
 * that are left after the analyzer resolved that plan again. A tag on the extraction would outlive
 * the analysis instead, and make the field NULL in a later analysis of the same unresolved plan,
 * e.g. from a plan cache, even once a column has the field.
 */
case class LastResortExtractValue(child: UnresolvedExtractValue)
    extends UnaryExpression
    with Unevaluable {
  override lazy val resolved: Boolean = false
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override def sql: String = child.sql
  override def toString: String = child.toString
  override protected def withNewChildInternal(newChild: Expression): Expression = newChild match {
    case extractValue: UnresolvedExtractValue => copy(child = extractValue)
    // The extraction is resolved, with or without the field.
    case resolved => resolved
  }
}

/**
 * Wraps the plan of a subquery expression of an operator that [[ResolveOptionalReferences]] could
 * not resolve. It lets the rule replace the optional references of the subquery plan when the
 * analyzer resolves that plan the next time, and it is removed by then. Until then, it keeps the
 * subquery plan unresolved, so that the analyzer does resolve it again.
 */
case class ResolveOptionalReferencesInSubquery(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override lazy val resolved: Boolean = false
  override protected def withNewChildInternal(
      newChild: LogicalPlan): ResolveOptionalReferencesInSubquery = copy(child = newChild)
}
