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

import java.util.HashSet

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{withPosition, AnalysisErrorAt}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.{
  ExistenceJoin,
  FullOuter,
  JoinType,
  LeftAnti,
  LeftOuter,
  LeftSemi,
  LeftSingle,
  RightOuter
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.BooleanType

/**
 * Shared resolution mechanics for join-like binary operators, mixed into [[JoinResolver]] and
 * other join-like resolvers: resolving two children in isolated multi-child scopes, computing a
 * combined hidden output, filtering hidden metadata columns by join type, and resolving a boolean
 * join condition. These routines carry subtle scope, CTE, hidden-output, metadata-nullability and
 * [[ExprId]] behavior; keeping them here ensures join-like resolvers stay aligned and fixes cannot
 * drift between them.
 *
 * For example, in the following query:
 *
 * {{{
 * SELECT * FROM t1 JOIN t2 ON t1.key = t2.key;
 * }}}
 *
 * the plan is:
 *
 * {{{
 * Project [key#1, key#2]
 * +- Join Inner, (key#1 = key#2)
 *    :- SubqueryAlias t1
 *    :  +- Relation t1[key#1]
 *    +- SubqueryAlias t2
 *       +- Relation t2[key#2]
 * }}}
 *
 * `t1` and `t2` are each resolved by [[resolveJoinChild]] in their own [[NameScope]], and the
 * condition `key#1 = key#2` is resolved by [[resolveJoinCondition]] against the union of the two
 * child scopes.
 */
trait JoinLikeResolver extends SQLConfHelper with QueryErrorsBase {

  protected val resolver: Resolver
  protected val expressionResolver: ExpressionResolver

  protected def scopes: NameScopeStack = resolver.getNameScopes
  protected def cteRegistry: CteRegistry = resolver.getCteRegistry
  protected def operatorResolutionContextStack: OperatorResolutionContextStack =
    resolver.getOperatorResolutionContextStack
  protected def expressionIdAssigner: ExpressionIdAssigner =
    expressionResolver.getExpressionIdAssigner

  /**
   * Resolves a single join child in the context of a) new [[NameScope]] b) new
   * [[ExpressionIdAssigner]] mapping c) new [[CteScope]] for the multi-child operator. Returns the
   * resolved child together with its [[NameScope]], which the caller uses to compute the join
   * output.
   */
  protected def resolveJoinChild(
      unresolvedOperator: LogicalPlan,
      child: LogicalPlan): (LogicalPlan, NameScope) = {
    expressionIdAssigner.pushMapping()
    scopes.pushScope()
    cteRegistry.pushScopeForMultiChildOperator(
      unresolvedOperator = unresolvedOperator,
      unresolvedChild = child
    )

    try {
      val resolvedChild = resolver.resolve(child)
      (resolvedChild, scopes.current)
    } finally {
      cteRegistry.popScope()
      scopes.popScope()
      expressionIdAssigner.popMapping(collectChildMapping = true)
    }
  }

  /**
   * Resolves the join condition against __all__ attributes from child scopes. We overwrite the
   * current scope first to prepare for
   * [[ExpressionResolver.resolveExpressionTreeInOperator]]. The join will actually produce a
   * different output than the one set here, so an additional overwrite with the correct values is
   * needed afterwards. Two overwrites are necessary because the condition is resolved from
   * original children outputs, whereas the join output will either not contain all attributes or
   * their nullabilities will be different.
   *
   * `collectInvalidExpressions` controls whether unsupported expressions (aggregate / window /
   * generator, etc.) found in the just-resolved condition are thrown immediately as
   * `UNSUPPORTED_EXPR_FOR_OPERATOR`. [[JoinResolver]] leaves this off and relies on the generic
   * post-resolution check in [[Resolver.validateResolvedOperatorGenerically]], which inspects
   * [[ExpressionResolver.getLastInvalidExpressionsInTheContextOfOperator]] once, after the last
   * expression tree of the operator is resolved. Callers that resolve further expression trees
   * after the condition must turn this on, or those later trees overwrite the "last invalid
   * expressions" snapshot before the generic check runs.
   */
  protected def resolveJoinCondition(
      unresolvedJoin: LogicalPlan,
      unresolvedCondition: Option[Expression],
      leftNameScope: NameScope,
      rightNameScope: NameScope,
      collectInvalidExpressions: Boolean = false): Option[Expression] = {
    scopes.overwriteCurrent(
      output = Some(leftNameScope.output ++ rightNameScope.output),
      hiddenOutput = Some(leftNameScope.hiddenOutput ++ rightNameScope.hiddenOutput)
    )

    val resolvedCondition = unresolvedCondition.map { condition =>
      expressionResolver.resolveExpressionTreeInOperator(
        condition,
        unresolvedJoin
      )
    }

    validateJoinConditionDataType(resolvedCondition, unresolvedJoin)

    if (collectInvalidExpressions) {
      val invalidExpressions =
        expressionResolver.getLastInvalidExpressionsInTheContextOfOperator
      if (invalidExpressions.nonEmpty) {
        withPosition(unresolvedJoin) {
          resolver.throwUnsupportedExprForOperator(
            operator = unresolvedJoin,
            invalidExpressions = invalidExpressions
          )
        }
      }
    }

    resolvedCondition
  }

  private def validateJoinConditionDataType(
      condition: Option[Expression],
      unresolvedJoin: LogicalPlan): Unit = {
    condition match {
      case Some(condition) =>
        if (condition.dataType != BooleanType) {
          unresolvedJoin.failAnalysis(
            errorClass = "JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE",
            messageParameters = Map(
              "joinCondition" -> toSQLExpr(condition),
              "conditionType" -> toSQLType(condition.dataType)
            )
          )
        }
      case None =>
    }
  }

  /**
   * Computes the new hidden output for a join. The result contains attributes from `mainOutput`,
   * followed by `extraHiddenOutput` (marked as qualified access only), followed by qualified access
   * only attributes from `oldHiddenOutput`. All attributes must be unique: `mainOutput` takes
   * precedence over hidden output, and `extraHiddenOutput` takes precedence over `oldHiddenOutput`.
   *
   * For regular joins, `extraHiddenOutput` is empty and the result is simply `mainOutput` plus the
   * qualified access only portion of `oldHiddenOutput`. For NATURAL / USING joins,
   * `extraHiddenOutput` contains additional attributes from
   * [[org.apache.spark.sql.catalyst.analysis.NaturalAndUsingJoinResolution]] that must be marked as
   * qualified access only and take precedence over `oldHiddenOutput` so that name resolution in
   * downstream operators (e.g. `Sort`, `Filter`) disambiguates correctly.
   */
  protected def computeHiddenOutputForJoin(
      mainOutput: Seq[Attribute],
      oldHiddenOutput: Seq[Attribute],
      extraHiddenOutput: Seq[Attribute] = Seq.empty): Seq[Attribute] = {
    val mainOutputLookup = new HashSet[ExprId](mainOutput.size)
    mainOutput.foreach { attribute =>
      mainOutputLookup.add(attribute.exprId)
    }

    val extraHiddenOutputLookup = new HashSet[ExprId](extraHiddenOutput.size)
    extraHiddenOutput.foreach { attribute =>
      extraHiddenOutputLookup.add(attribute.exprId)
    }

    val filteredExtraHiddenOutput = extraHiddenOutput.collect {
      case attribute if !mainOutputLookup.contains(attribute.exprId) =>
        attribute.markAsQualifiedAccessOnly()
    }

    val filteredOldHiddenOutput = oldHiddenOutput.filter { attribute =>
      !mainOutputLookup.contains(attribute.exprId) &&
      !extraHiddenOutputLookup.contains(attribute.exprId) &&
      (attribute.qualifiedAccessOnly || attribute.isMetadataCol)
    }

    mainOutput ++ filteredExtraHiddenOutput ++ filteredOldHiddenOutput
  }

  /**
   * Filters metadata columns from hidden output based on join type.
   *
   * For [[ExistenceJoin]] and left-existence joins ([[LeftSemi]], [[LeftAnti]]), right-side
   * metadata columns are removed, matching [[org.apache.spark.sql.catalyst.plans.logical.Join]]'s
   * `metadataOutput`, which propagates only the left side's metadata output for these join types.
   * For outer joins, metadata columns on the nullable side have their nullability set to `true`,
   * mirroring the adjustment applied to the main output -- metadata columns bypass it because they
   * live in hidden output instead. For all other join types, metadata columns from both sides are
   * kept as-is.
   */
  protected def filterHiddenOutputMetadataForJoin(
      joinType: JoinType,
      oldHiddenOutput: Seq[Attribute],
      rightHiddenOutput: Seq[Attribute]): Seq[Attribute] = {
    val rightMetadataIds = new HashSet[ExprId]()
    rightHiddenOutput.foreach { attribute =>
      if (attribute.isMetadataCol) {
        rightMetadataIds.add(attribute.exprId)
      }
    }

    joinType match {
      case _: ExistenceJoin | LeftSemi | LeftAnti =>
        oldHiddenOutput.filter(attribute =>
          !attribute.isMetadataCol || !rightMetadataIds.contains(attribute.exprId)
        )
      case LeftOuter | LeftSingle =>
        oldHiddenOutput.map { attribute =>
          if (attribute.isMetadataCol && rightMetadataIds.contains(attribute.exprId)) {
            attribute.withNullability(true)
          } else {
            attribute
          }
        }
      case RightOuter =>
        oldHiddenOutput.map { attribute =>
          if (attribute.isMetadataCol && !rightMetadataIds.contains(attribute.exprId)) {
            attribute.withNullability(true)
          } else {
            attribute
          }
        }
      case FullOuter =>
        oldHiddenOutput.map { attribute =>
          if (attribute.isMetadataCol) {
            attribute.withNullability(true)
          } else {
            attribute
          }
        }
      case _ =>
        oldHiddenOutput
    }
  }
}
