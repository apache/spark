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
  PipeOperator,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  Distinct,
  Filter,
  LateralJoin,
  LogicalPlan,
  RepartitionByExpression,
  Sort,
  SubqueryAlias,
  UnaryNode
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{PARAMETERIZED_QUERY, PLAN_EXPRESSION}

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
 */
class ResolveOptionalReferences(resolveAgain: LogicalPlan => LogicalPlan)
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
    } else if (markOptionalFieldsAsLastResort(plan)) {
      resolveAgain(plan)
    } else {
      plan
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
        lazy val attributes = visibleAttributes(operator)
        operator.mapExpressions(replace(_, attributes, isTopLevel = true))
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
    lazy val attributes = visibleAttributes(aggregate)
    aggregate.copy(aggregateExpressions = aggregate.aggregateExpressions.map { expression =>
      replace(expression, attributes, isTopLevel = true).asInstanceOf[NamedExpression]
    })
  }

  /**
   * Replaces the optional references of `expression` that resolve to NULL against `attributes`.
   * A top-level reference keeps its name through an [[Alias]] of its last name part.
   */
  private def replace(
      expression: Expression,
      attributes: => Seq[Attribute],
      isTopLevel: Boolean): Expression = expression match {
    case unresolvedAttribute: UnresolvedAttribute
        if OptionalReferences.optionalNamePartIndices(unresolvedAttribute).nonEmpty &&
        OptionalReferences.resolvesToNull(
          nameParts = unresolvedAttribute.nameParts,
          optionalNamePartIndices = OptionalReferences.optionalNamePartIndices(unresolvedAttribute),
          attributes = attributes,
          resolver = conf.resolver
        ) =>
      val nullReference = OptionalReferences.nullReference()
      if (isTopLevel) {
        OptionalReferences.implicitAlias(nullReference, unresolvedAttribute.nameParts.last)
      } else {
        nullReference
      }
    case namedParameter: NamedParameter if OptionalReferences.isOptional(namedParameter) =>
      OptionalReferences.nullReference()
    case lambdaFunction: LambdaFunction if !lambdaFunction.bound =>
      lambdaFunction
    case lateralColumnAliasReference: LateralColumnAliasReference =>
      lateralColumnAliasReference
    case subquery: SubqueryExpression if hasUnresolvedOptionalReferences(subquery.plan) =>
      wrapSubqueryPlan(subquery)
    case other if !other.resolved =>
      other.mapChildren(replace(_, attributes, isTopLevel = false))
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
   * Marks the optional field extractions of the lowest operators of `plan` that are not resolved
   * as a last resort, and returns whether it marked any. See
   * [[OptionalReferences.markLastResort]].
   */
  private def markOptionalFieldsAsLastResort(plan: LogicalPlan): Boolean = {
    var marked = false
    plan.foreach { operator =>
      if (operator.childrenResolved && !operator.resolved) {
        operator.expressions.foreach(_.foreach {
          case extractValue: UnresolvedExtractValue
              if OptionalReferences.isOptional(extractValue) &&
              !OptionalReferences.isLastResort(extractValue) =>
            OptionalReferences.markLastResort(extractValue)
            marked = true
          case _ =>
        })
      }
    }
    marked
  }

  /**
   * Returns the attributes that the name resolution of `operator` can match a multipart name
   * against: the output of its children, the attributes that `Sort`, `Filter`, HAVING, and
   * QUALIFY can reference from below their child, the output of the subquery of a lateral join,
   * and the attributes of the outer query of a subquery.
   */
  private def visibleAttributes(operator: LogicalPlan): Seq[Attribute] = {
    val localAttributes = operator.children.flatMap(namedAttributes)
    val attributesFromBelow = operator match {
      case sort: Sort => attributesForMissingReferences(sort.child)
      case filter: Filter => attributesForMissingReferences(filter.child)
      case repartition: RepartitionByExpression =>
        attributesForMissingReferences(repartition.child)
      case qualify: UnresolvedQualify => attributesForMissingReferences(qualify.child)
      case UnresolvedHaving(_, aggregate: Aggregate) => namedAttributes(aggregate.child)
      case lateralJoin: LateralJoin => namedAttributes(lateralJoin.right.plan)
      case _ => Seq.empty
    }
    val outerAttributes = AnalysisContext.get.outerPlan.toSeq.flatMap {
      case having @ UnresolvedHaving(_, aggregate: Aggregate) =>
        having.children.flatMap(namedAttributes) ++ namedAttributes(aggregate.child)
      case outerPlan =>
        outerPlan.children.flatMap(namedAttributes)
    }
    localAttributes ++ attributesFromBelow ++ outerAttributes
  }

  private def namedAttributes(plan: LogicalPlan): Seq[Attribute] =
    plan.output ++ plan.metadataOutput

  /**
   * Returns the attributes below `plan` that a `Sort` or a `Filter` on top of `plan` can reference
   * as missing attributes, mirroring `ColumnResolutionHelper.resolveExprsAndAddMissingAttrs`.
   */
  private def attributesForMissingReferences(plan: LogicalPlan): Seq[Attribute] = plan match {
    case unary: UnaryNode
        if !unary.isInstanceOf[Distinct] && !unary.isInstanceOf[SubqueryAlias] &&
        !unary.isInstanceOf[PipeOperator] =>
      namedAttributes(unary.child) ++ attributesForMissingReferences(unary.child)
    case _ =>
      Seq.empty
  }
}

object ResolveOptionalReferences {

  /**
   * Returns true if `plan` or the plan of one of its subquery expressions has an optional
   * reference that is not resolved.
   */
  private def hasUnresolvedOptionalReferences(plan: LogicalPlan): Boolean = plan.exists {
    _.expressions.exists(_.exists {
      case subquery: SubqueryExpression => hasUnresolvedOptionalReferences(subquery.plan)
      case expression => !expression.resolved && OptionalReferences.hasOptionalMarker(expression)
    })
  }

  private def isPlanOfSubquery: Boolean = AnalysisContext.get.outerPlan.isDefined

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
