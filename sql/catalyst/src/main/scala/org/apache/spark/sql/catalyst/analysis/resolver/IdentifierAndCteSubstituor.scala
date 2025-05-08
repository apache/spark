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

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{
  CTERelationDef,
  LogicalPlan,
  SubqueryAlias,
  UnresolvedWith
}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION, UNRESOLVED_RELATION}

/**
 * The [[IdentifierAndCteSubstitutor]] is responsible for substituting the IDENTIFIERs (not yet
 * implemented) and CTE references in the unresolved logical plan before the actual resolution
 * starts (specifically before metadata resolution). This is important for SQL features like WITH
 * (that could confuse [[MetadataResolver]] with extra [[UnresolvedRelation]]s)
 * or IDENTIFIER (that "hides" the actual [[UnresolvedRelation]]s).
 *
 * We only recurse into the plan if [[IdentifierAndCteSubstitutor.NODES_OF_INTEREST]] are present.
 * This is done so that [[IdentifierAndCteSubstitutor]] is fast and not invasive.
 */
class IdentifierAndCteSubstitutor {
  private var cteRegistry = new CteRegistry

  /**
   * This is the main entry point to the substitution process. It takes `unresolvedPlan` and
   * substitutes CTEs and IDENTIFIERs (not yet implemented) that would otherwise confuse the
   * metadata resolution process.
   *
   * [[CteRegistry]] has to be reset for each new invocation, because CTEs in views are analyzed
   * in isolation.
   */
  def substitutePlan(unresolvedPlan: LogicalPlan): LogicalPlan = {
    cteRegistry = new CteRegistry

    substitute(unresolvedPlan)
  }

  private def substitute(unresolvedPlan: LogicalPlan): LogicalPlan = {
    unresolvedPlan match {
      case unresolvedWith: UnresolvedWith =>
        handleWith(unresolvedWith)
      case unresolvedRelation: UnresolvedRelation =>
        handleUnresolvedRelation(unresolvedRelation)
      case _ =>
        handleOperator(unresolvedPlan)
    }
  }

  /**
   * Handle [[UnresolvedWith]] operator. WITH clause produces unresolved relations in the plan,
   * that are not actual tables or views, but are just potential [[CTERelationRef]]s. To correctly
   * detect those CTE references we use the [[CteRegistry]] framework. The actual CTE resolution
   * is left to the main algebraic pass in the [[Resolver]] - here we replace the
   * [[UnresolvedRelation]]s with [[UnresolvedCteRelationRef]] to avoid isuing useless catalog
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
   */
  private def handleWith(unresolvedWith: UnresolvedWith): LogicalPlan = {
    val cteRelationsAfterSubstitution = unresolvedWith.cteRelations.map { cteRelation =>
      val (cteName, ctePlan) = cteRelation

      val ctePlanAfter = cteRegistry.withNewScope() {
        substitute(ctePlan).asInstanceOf[SubqueryAlias]
      }

      cteRegistry.currentScope.registerCte(cteName, CTERelationDef(ctePlanAfter))

      (cteName, ctePlanAfter)
    }

    val childAfterSubstitution = cteRegistry.withNewScope() {
      substitute(unresolvedWith.child)
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
   * Handle [[UnresolvedRelation]] operator, which could be a CTE reference. If that's the case, we
   * replace it with [[UnresolvedCteRelationRef]].
   */
  private def handleUnresolvedRelation(unresolvedRelation: UnresolvedRelation): LogicalPlan = {
    if (unresolvedRelation.multipartIdentifier.size == 1) {
      cteRegistry.resolveCteName(unresolvedRelation.multipartIdentifier.head) match {
        case Some(_) =>
          val result = withOrigin(unresolvedRelation.origin) {
            UnresolvedCteRelationRef(unresolvedRelation.multipartIdentifier.head)
          }
          result.copyTagsFrom(unresolvedRelation)
          result
        case None =>
          unresolvedRelation
      }
    } else {
      unresolvedRelation
    }
  }

  /**
   * Handle `unresolvedOperator` generically. We use the [[CteRegistry]] framework to recurse into
   * its children and subquery expressions.
   */
  private def handleOperator(unresolvedOperator: LogicalPlan): LogicalPlan = {
    val operatorAfterSubstitution = unresolvedOperator match {
      case operator
          if !operator.containsAnyPattern(IdentifierAndCteSubstitutor.NODES_OF_INTEREST: _*) =>
        operator

      case operator if operator.children.size > 1 =>
        val newChildren = operator.children.map { child =>
          cteRegistry.withNewScopeUnderMultiChildOperator(operator, child) {
            substitute(child)
          }
        }
        operator.withNewChildren(newChildren)

      case operator if operator.children.size == 1 =>
        val newChildren = Seq(substitute(operator.children.head))
        operator.withNewChildren(newChildren)

      case operator =>
        operator
    }

    operatorAfterSubstitution.transformExpressionsWithPruning(
      _.containsPattern(PLAN_EXPRESSION)
    ) {
      case subqueryExpression: SubqueryExpression =>
        val newPlan = cteRegistry.withNewScope(isRoot = true) {
          substitute(subqueryExpression.plan)
        }

        val result = withOrigin(subqueryExpression.origin) {
          subqueryExpression.withNewPlan(newPlan)
        }
        result.copyTagsFrom(subqueryExpression)
        result
    }
  }
}

object IdentifierAndCteSubstitutor {
  val NODES_OF_INTEREST = Seq(CTE, UNRESOLVED_RELATION)
}
