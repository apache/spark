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
import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.{
  Cross,
  ExistenceJoin,
  FullOuter,
  Inner,
  JoinType,
  LeftAnti,
  LeftOuter,
  LeftSemi,
  LeftSingle,
  NaturalJoin,
  RightOuter,
  UsingJoin
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
  protected def queryBlockScopes: QueryBlockScopeStack = resolver.getQueryBlockScopes
  protected def cteRegistry: CteRegistry = resolver.getCteRegistry
  protected def operatorResolutionContextStack: OperatorResolutionContextStack =
    resolver.getOperatorResolutionContextStack
  protected def expressionIdAssigner: ExpressionIdAssigner =
    expressionResolver.getExpressionIdAssigner

  /**
   * Determines whether recursive CTE self-references are allowed on the left and right sides of a
   * join. [[UsingJoin]] and [[NaturalJoin]] are classified by their base join type. Returns a
   * tuple (allowLeft, allowRight).
   *
   * Rules:
   *  - Inner/Cross: Both sides allow recursive references
   *  - LeftOuter/LeftSemi/LeftAnti: Left allows, right disallows
   *  - RightOuter: Left disallows, right allows
   *  - FullOuter/other: Both sides disallow
   */
  protected def getRecursiveCteAllowance(joinType: JoinType): (Boolean, Boolean) = {
    val baseJoinType = joinType match {
      case UsingJoin(baseJoinType, _) => baseJoinType
      case NaturalJoin(baseJoinType) => baseJoinType
      case baseJoinType => baseJoinType
    }

    baseJoinType match {
      case Inner | Cross => (true, true)
      case LeftOuter | LeftSemi | LeftAnti => (true, false)
      case RightOuter => (false, true)
      case FullOuter | _ => (false, false)
    }
  }

  /**
   * Resolves a single join child in the context of a) new [[NameScope]] b) new
   * [[ExpressionIdAssigner]] mapping c) new [[CteScope]] for the multi-child operator. Returns the
   * resolved child together with its [[NameScope]], which the caller uses to compute the join
   * output. If `shouldDisallowRecursiveCtes` is set, self references in the child's CTE scope are
   * rejected (see [[ResolveWithCTE.checkIfSelfReferenceIsPlacedCorrectly]]).
   */
  protected def resolveJoinChild(
      unresolvedOperator: LogicalPlan,
      child: LogicalPlan,
      shouldDisallowRecursiveCtes: Boolean): (LogicalPlan, NameScope) = {
    expressionIdAssigner.pushMapping()
    queryBlockScopes.push()
    scopes.pushScope()
    cteRegistry.pushScopeForMultiChildOperator(
      unresolvedOperator = unresolvedOperator,
      unresolvedChild = child
    )

    try {
      val resolvedChild = resolver.resolve(child)
      if (shouldDisallowRecursiveCtes) {
        cteRegistry.currentScope.failIfScopeHasSelfReference()
      }
      (resolvedChild, scopes.current)
    } finally {
      cteRegistry.popScope()
      scopes.popScope()
      queryBlockScopes.pop()
      expressionIdAssigner.popMapping(collectChildMapping = true)
    }
  }

  /**
   * Merges the ID mappings of both join sides. It also keeps the IDs of hidden columns like `d.k`,
   * so a copy of the same table on the other side can't remap them. Operators that map an already
   * resolved column again, like `d.*` or an ASOF JOIN `MATCH_CONDITION`, would otherwise read the
   * copy's column.
   */
  protected def mergeChildMappings(leftNameScope: NameScope, rightNameScope: NameScope): Unit = {
    val qualifiedAccessOnlyColumnIds =
      (leftNameScope.hiddenOutput ++ rightNameScope.hiddenOutput)
        .filter(_.qualifiedAccessOnly)
        .map(_.exprId)
    expressionIdAssigner.createMappingFromChildMappings(
      newOutputIds =
        leftNameScope.getOutputIds ++ rightNameScope.getOutputIds ++ qualifiedAccessOnlyColumnIds
    )
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
   * The merged [[PlanIdTargetRegistry]] is built fresh from the two child registries rather than
   * mutating either of them, so a caller that resolves more than one condition against the same
   * child scopes (e.g. [[AsOfJoinResolver]], which resolves the ON condition and then a USING
   * condition) does not merge the child targets twice, which would make a right-child plan-id
   * reference spuriously ambiguous.
   *
   * Example: `df1.joinAsOf(df2, leftAsOf = df1("t"), rightAsOf = df2("t"),
   * usingColumns = Seq("id"), ...)` resolves the ON condition and then the USING condition
   * against the same `df1`/`df2` scopes. Building fresh merges each child once, so the right-child
   * reference `df2("t")` stays unambiguous; mutating a child registry would merge `df2`'s targets
   * in twice and raise a spurious `AMBIGUOUS_COLUMN_REFERENCE`.
   */
  protected def resolveJoinCondition(
      unresolvedJoin: LogicalPlan,
      unresolvedCondition: Option[Expression],
      leftNameScope: NameScope,
      rightNameScope: NameScope): Option[Expression] = {
    val planIdTargetRegistry = new PlanIdTargetRegistry
    planIdTargetRegistry.merge(leftNameScope.planIdTargetRegistry)
    planIdTargetRegistry.merge(rightNameScope.planIdTargetRegistry)
    scopes.overwriteCurrent(
      output = Some(leftNameScope.output ++ rightNameScope.output),
      hiddenOutput = Some(leftNameScope.hiddenOutput ++ rightNameScope.hiddenOutput),
      planIdTargetRegistry = Some(planIdTargetRegistry)
    )

    val resolvedCondition = unresolvedCondition.map { condition =>
      expressionResolver.resolveExpressionTreeInOperator(
        condition,
        unresolvedJoin
      )
    }

    validateJoinConditionDataType(resolvedCondition, unresolvedJoin)

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
   * Returns the [[NameScope.qualifiedAccessOnlyOutputIds]] of the [[Project]] that a NATURAL or
   * USING join places on top of itself: the ids of the attributes returned by
   * [[computeQualifiedAccessOnlyColumnsForJoinProject]]. The set is empty for the top-level
   * operator, whose [[Project]] doesn't append qualified access only attributes.
   */
  protected def computeQualifiedAccessOnlyOutputIdsForJoinProject(
      unresolvedJoin: LogicalPlan,
      hiddenOutput: Seq[Attribute],
      joinType: JoinType,
      leftNameScope: NameScope,
      rightNameScope: NameScope): HashSet[ExprId] = {
    val qualifiedAccessOnlyOutputIds = new HashSet[ExprId]
    if (!unresolvedJoin.containsTag(ResolverTag.TOP_LEVEL_OPERATOR)) {
      computeQualifiedAccessOnlyColumnsForJoinProject(
        hiddenOutput = hiddenOutput,
        joinType = joinType,
        leftNameScope = leftNameScope,
        rightNameScope = rightNameScope
      ).foreach { attribute =>
        qualifiedAccessOnlyOutputIds.add(attribute.exprId)
      }
    }
    qualifiedAccessOnlyOutputIds
  }

  /**
   * Returns the qualified access only attributes from `hiddenOutput` that the join outputs, i.e.
   * that its left or right child outputs according to [[NameScope.isOperatorOutput]]. A left
   * existence join outputs only its left child. For example, `key#2` is dropped because
   * `|> SELECT key` keeps it only in its hidden output:
   *
   * {{{
   * FROM t1 JOIN t2 USING (key) |> SELECT key |> JOIN t3 USING (key) |> WHERE key IS NOT NULL;
   *
   * Filter isnotnull(key#1)
   * +- Project [key#1, key#3]
   *    +- Join Inner, (key#1 = key#3)
   *       :- Project [key#1]
   *       :  +- Project [key#1, key#2]
   *       :     +- Join Inner, (key#1 = key#2)
   *       :        :- Relation t1[key#1]
   *       :        +- Relation t2[key#2]
   *       +- Relation t3[key#3]
   * }}}
   */
  protected def computeQualifiedAccessOnlyColumnsForJoinProject(
      hiddenOutput: Seq[Attribute],
      joinType: JoinType,
      leftNameScope: NameScope,
      rightNameScope: NameScope): Seq[Attribute] = {
    val joinOutputsRightChild = outputsRightChild(joinType)
    hiddenOutput.filter { attribute =>
      attribute.qualifiedAccessOnly && (
        leftNameScope.isOperatorOutput(attribute.exprId) ||
        (joinOutputsRightChild && rightNameScope.isOperatorOutput(attribute.exprId))
      )
    }
  }

  /**
   * Returns the [[NameScope.qualifiedAccessOnlyOutputIds]] of a join that doesn't place a
   * [[Project]] on top of itself: the ones of both children, or only the ones of the left child
   * for a left existence join. The children sets are reused when possible, since they are never
   * mutated.
   */
  protected def computeQualifiedAccessOnlyOutputIdsForJoin(
      joinType: JoinType,
      leftNameScope: NameScope,
      rightNameScope: NameScope): HashSet[ExprId] = {
    val leftIds = leftNameScope.qualifiedAccessOnlyOutputIds
    val rightIds = rightNameScope.qualifiedAccessOnlyOutputIds
    if (!outputsRightChild(joinType) || rightIds.isEmpty) {
      leftIds
    } else if (leftIds.isEmpty) {
      rightIds
    } else {
      val qualifiedAccessOnlyOutputIds = new HashSet[ExprId](leftIds.size + rightIds.size)
      qualifiedAccessOnlyOutputIds.addAll(leftIds)
      qualifiedAccessOnlyOutputIds.addAll(rightIds)
      qualifiedAccessOnlyOutputIds
    }
  }

  /**
   * Returns whether a join of `joinType` outputs the attributes of its right child, like
   * [[org.apache.spark.sql.catalyst.plans.logical.Join.computeOutput]].
   */
  private def outputsRightChild(joinType: JoinType): Boolean = joinType match {
    case _: ExistenceJoin | LeftSemi | LeftAnti => false
    case _ => true
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
