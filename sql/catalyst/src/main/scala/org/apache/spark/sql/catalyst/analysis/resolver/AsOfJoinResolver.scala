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
  AnalysisErrorAt,
  AsOfJoinMatchConditionResolution,
  MaterializedMatchCondition,
  NaturalAndUsingJoinResolution
}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeSet,
  Expression,
  LambdaFunction,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util._

/**
 * Resolves [[AsOfJoin]] operators, including SQL `MATCH_CONDITION` and `USING` clauses.
 *
 * An ASOF JOIN matches each left-side row to the closest right-side row whose order key is
 * less than or equal to (or, for a reversed match condition, greater than or equal to) the
 * left row's order key, optionally within a tolerance and equality keys (`USING` / `ON`).
 * Unlike a regular join, at most one right row is chosen per left row based on temporal (or
 * other ordered) proximity rather than arbitrary key equality.
 *
 * {{{
 * SELECT * FROM trades ASOF JOIN quotes
 *   MATCH_CONDITION (trades.time >= quotes.time) USING (symbol)
 *
 * Project [symbol, time, ...]
 * +- AsOfJoin (trades.time >= quotes.time), (trades.symbol = quotes.symbol), Inner
 * }}}
 */
class AsOfJoinResolver(
    override val resolver: Resolver,
    override val expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[AsOfJoin, LogicalPlan]
    with JoinLikeResolver {

  override def resolve(unresolvedAsOfJoin: AsOfJoin): LogicalPlan = {
    val (resolvedLeft, leftNameScope) = resolveJoinChild(
      unresolvedOperator = unresolvedAsOfJoin,
      child = unresolvedAsOfJoin.left
    )

    val (resolvedRight, rightNameScope) = resolveJoinChild(
      unresolvedOperator = unresolvedAsOfJoin,
      child = unresolvedAsOfJoin.right
    )

    ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(
      Seq(leftNameScope.output, rightNameScope.output)
    )

    expressionIdAssigner.createMappingFromChildMappings(
      newOutputIds = leftNameScope.getOutputIds ++ rightNameScope.getOutputIds
    )

    val partiallyResolved = unresolvedAsOfJoin.copy(
      left = resolvedLeft,
      right = resolvedRight
    )

    val resolvedCondition = resolveJoinCondition(
      unresolvedJoin = unresolvedAsOfJoin,
      unresolvedCondition = partiallyResolved.condition,
      leftNameScope = leftNameScope,
      rightNameScope = rightNameScope,
      collectInvalidExpressions = true
    )

    val (usingOutput, conditionWithUsingColumns) = resolveUsingColumns(
      unresolvedAsOfJoin = unresolvedAsOfJoin,
      partiallyResolved = partiallyResolved,
      resolvedCondition = resolvedCondition,
      leftNameScope = leftNameScope,
      rightNameScope = rightNameScope
    )

    val resolvedJoin = resolveAsOfExpressions(
      unresolvedAsOfJoin = unresolvedAsOfJoin,
      partiallyResolved = partiallyResolved.copy(
        condition = conditionWithUsingColumns,
        usingColumns = None
      ),
      leftNameScope = leftNameScope,
      rightNameScope = rightNameScope
    )

    usingOutput match {
      case Some((outputList, hiddenList)) =>
        buildUsingProject(
          unresolvedAsOfJoin = unresolvedAsOfJoin,
          resolvedJoin = resolvedJoin,
          outputList = outputList,
          hiddenList = hiddenList,
          rightNameScope = rightNameScope
        )
      case None =>
        overwriteJoinOutputScope(
          joinType = unresolvedAsOfJoin.joinType,
          leftNameScope = leftNameScope,
          rightNameScope = rightNameScope
        )
        cteRegistry.currentScope.tryPutWithCTE(
          unresolvedOperator = unresolvedAsOfJoin,
          resolvedOperator = resolvedJoin
        )
    }
  }

  private def resolveUsingColumns(
      unresolvedAsOfJoin: AsOfJoin,
      partiallyResolved: AsOfJoin,
      resolvedCondition: Option[Expression],
      leftNameScope: NameScope,
      rightNameScope: NameScope)
      : (Option[(Seq[NamedExpression], Seq[Attribute])], Option[Expression]) = {
    partiallyResolved.usingColumns match {
      case Some(columns) if resolvedCondition.isEmpty =>
        val (outputList, hiddenList, newCondition) =
          NaturalAndUsingJoinResolution.computeJoinOutputsAndNewCondition(
            left = partiallyResolved.left,
            leftOutput = leftNameScope.output,
            right = partiallyResolved.right,
            rightOutput = rightNameScope.output,
            joinType = partiallyResolved.joinType,
            joinNames = columns,
            condition = None,
            resolveName = conf.resolver
          )
        val resolvedUsingCondition = resolveJoinCondition(
          unresolvedJoin = unresolvedAsOfJoin,
          unresolvedCondition = newCondition,
          leftNameScope = leftNameScope,
          rightNameScope = rightNameScope,
          collectInvalidExpressions = true
        )
        (Some((outputList, hiddenList)), resolvedUsingCondition)
      case _ =>
        (None, resolvedCondition)
    }
  }

  private def buildUsingProject(
      unresolvedAsOfJoin: AsOfJoin,
      resolvedJoin: AsOfJoin,
      outputList: Seq[NamedExpression],
      hiddenList: Seq[Attribute],
      rightNameScope: NameScope): Project = {
    val resolvedOutputList = outputList.map { expression =>
      resolveExpressionInJoin(unresolvedAsOfJoin, expression).asInstanceOf[NamedExpression]
    }
    val outputAttributes = resolvedOutputList.map(_.toAttribute)
    val filteredHiddenOutput = filterHiddenOutputMetadataForJoin(
      joinType = unresolvedAsOfJoin.joinType,
      oldHiddenOutput = scopes.current.hiddenOutput,
      rightHiddenOutput = rightNameScope.hiddenOutput
    )
    val newHiddenOutput = computeHiddenOutputForJoin(
      mainOutput = outputAttributes,
      oldHiddenOutput = filteredHiddenOutput,
      extraHiddenOutput = hiddenList
    )
    scopes.overwriteCurrent(
      output = Some(outputAttributes),
      hiddenOutput = Some(newHiddenOutput)
    )

    val qualifiedAccessOnlyColumns = newHiddenOutput.filter(_.qualifiedAccessOnly)
    val projectList =
      if (unresolvedAsOfJoin.containsTag(ResolverTag.TOP_LEVEL_OPERATOR)) {
        resolvedOutputList
      } else {
        resolvedOutputList ++ qualifiedAccessOnlyColumns
      }

    operatorResolutionContextStack.current.baseOperator = Some(resolvedJoin)
    val project = Project(projectList, resolvedJoin)
    project.setTagValue(Project.hiddenOutputTag, qualifiedAccessOnlyColumns)
    project
  }

  private def resolveAsOfExpressions(
      unresolvedAsOfJoin: AsOfJoin,
      partiallyResolved: AsOfJoin,
      leftNameScope: NameScope,
      rightNameScope: NameScope): AsOfJoin = {
    (
      partiallyResolved.matchLeftOperand,
      partiallyResolved.matchOperator,
      partiallyResolved.matchRightOperand
    ) match {
      case (Some(unresolvedLeftOperand), Some(operator), Some(unresolvedRightOperand)) =>
        val leftOperand = resolveExpressionInJoin(unresolvedAsOfJoin, unresolvedLeftOperand)
        val rightOperand = resolveExpressionInJoin(unresolvedAsOfJoin, unresolvedRightOperand)
        AsOfJoinMatchConditionResolution.materialize(
          join = partiallyResolved,
          leftSet = AttributeSet(leftNameScope.output),
          rightSet = AttributeSet(rightNameScope.output),
          leftOperand = leftOperand,
          operator = operator,
          rightOperand = rightOperand
        ) match {
          case Some(materialized) =>
            materializedMatchConditionToJoin(
              unresolvedAsOfJoin = unresolvedAsOfJoin,
              partiallyResolved = partiallyResolved,
              materialized = materialized
            )
          case None =>
            partiallyResolved
        }
      case (None, None, None) =>
        resolvePreMaterializedExpressions(unresolvedAsOfJoin, partiallyResolved)
      case _ =>
        partiallyResolved
    }
  }

  /**
   * Places the materialized `MATCH_CONDITION` expressions on the join. They are freshly
   * constructed trees, so each one still has to go through expression resolution before it can be
   * placed in the resolved plan.
   */
  private def materializedMatchConditionToJoin(
      unresolvedAsOfJoin: AsOfJoin,
      partiallyResolved: AsOfJoin,
      materialized: MaterializedMatchCondition): AsOfJoin = {
    throwIfLambdaBasedOrdering(materialized.orderExpression)

    val asOfCondition = resolveExpressionInJoin(unresolvedAsOfJoin, materialized.asOfCondition)
    val orderExpression =
      resolveExpressionInJoin(unresolvedAsOfJoin, materialized.orderExpression)
    val leftSortExpressions =
      materialized.leftSortExpressions.map(resolveExpressionInJoin(unresolvedAsOfJoin, _))
    val rightSortExpressions =
      materialized.rightSortExpressions.map(resolveExpressionInJoin(unresolvedAsOfJoin, _))

    partiallyResolved.copy(
      asOfCondition = asOfCondition,
      orderExpression = orderExpression,
      matchLeftOperand = None,
      matchOperator = None,
      matchRightOperand = None,
      leftSortExprs = leftSortExpressions,
      rightSortExprs = rightSortExpressions
    )
  }

  private def resolvePreMaterializedExpressions(
      unresolvedAsOfJoin: AsOfJoin,
      partiallyResolved: AsOfJoin): AsOfJoin = {
    throwIfLambdaBasedOrdering(partiallyResolved.orderExpression)

    val resolvedTolerance =
      partiallyResolved.toleranceAssertion.map(resolveExpressionInJoin(unresolvedAsOfJoin, _))
    validateTolerance(unresolvedAsOfJoin, resolvedTolerance)

    val asOfCondition =
      resolveExpressionInJoin(unresolvedAsOfJoin, partiallyResolved.asOfCondition)
    val orderExpression =
      resolveExpressionInJoin(unresolvedAsOfJoin, partiallyResolved.orderExpression)
    val leftSortExpressions =
      partiallyResolved.leftSortExprs.map(resolveExpressionInJoin(unresolvedAsOfJoin, _))
    val rightSortExpressions =
      partiallyResolved.rightSortExprs.map(resolveExpressionInJoin(unresolvedAsOfJoin, _))

    partiallyResolved.copy(
      asOfCondition = asOfCondition,
      orderExpression = orderExpression,
      toleranceAssertion = resolvedTolerance,
      leftSortExprs = leftSortExpressions,
      rightSortExprs = rightSortExpressions
    )
  }

  /**
   * `MATCH_CONDITION` operands that are ordered element-wise (`ARRAY` operands) materialize into
   * an ordering expression built on top of a [[LambdaFunction]]:
   *
   * {{{
   * -- MATCH_CONDITION (t.a >= r.a) with ARRAY<INT> operands
   * zip_with(t.a, r.a, lambdafunction((lambda left_elem - lambda right_elem), ...))
   * }}}
   *
   * The single-pass [[ExpressionResolver]] doesn't support lambda expressions, so bail out and
   * let the fixed-point analyzer resolve those queries.
   */
  private def throwIfLambdaBasedOrdering(orderExpression: Expression): Unit = {
    if (orderExpression.exists(_.isInstanceOf[LambdaFunction])) {
      throw new ExplicitlyUnsupportedResolverFeature(
        "MATCH_CONDITION with a lambda-based ordering expression"
      )
    }
  }

  private def resolveExpressionInJoin(
      unresolvedAsOfJoin: AsOfJoin,
      unresolvedExpression: Expression): Expression = {
    expressionResolver.resolveExpressionTreeInOperator(
      unresolvedExpression,
      unresolvedAsOfJoin
    )
  }

  private def validateTolerance(
      unresolvedAsOfJoin: AsOfJoin,
      toleranceAssertion: Option[Expression]): Unit = {
    toleranceAssertion.foreach { assertion =>
      if (!assertion.foldable) {
        unresolvedAsOfJoin.failAnalysis(
          errorClass = "AS_OF_JOIN.TOLERANCE_IS_UNFOLDABLE",
          messageParameters = Map.empty
        )
      }
      if (!assertion.eval().asInstanceOf[Boolean]) {
        unresolvedAsOfJoin.failAnalysis(
          errorClass = "AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE",
          messageParameters = Map.empty
        )
      }
    }
  }

  private def overwriteJoinOutputScope(
      joinType: JoinType,
      leftNameScope: NameScope,
      rightNameScope: NameScope): Unit = {
    val newOutput = AsOfJoin.computeOutput(
      joinType = joinType,
      leftOutput = leftNameScope.output,
      rightOutput = rightNameScope.output
    )

    val filteredHiddenOutput = filterHiddenOutputMetadataForJoin(
      joinType = joinType,
      oldHiddenOutput = scopes.current.hiddenOutput,
      rightHiddenOutput = rightNameScope.hiddenOutput
    )

    scopes.overwriteCurrent(
      output = Some(newOutput),
      hiddenOutput = Some(computeHiddenOutputForJoin(newOutput, filteredHiddenOutput))
    )
  }
}
