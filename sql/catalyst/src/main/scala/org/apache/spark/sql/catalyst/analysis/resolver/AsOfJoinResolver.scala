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
  AsOfJoinMatchConditionResolution,
  AsOfJoinValidation,
  NaturalAndUsingJoinResolution
}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeSet,
  Expression,
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

    mergeChildMappings(
      leftNameScope = leftNameScope,
      rightNameScope = rightNameScope
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

    val leftSet = AttributeSet(
      leftNameScope.output ++ leftNameScope.hiddenOutput.filter(_.qualifiedAccessOnly)
    )
    val rightSet = AttributeSet(
      rightNameScope.output ++ rightNameScope.hiddenOutput.filter(_.qualifiedAccessOnly)
    )
    val resolvedJoin = resolveAsOfExpressions(
      unresolvedAsOfJoin = unresolvedAsOfJoin,
      partiallyResolved = partiallyResolved.copy(
        condition = conditionWithUsingColumns,
        usingColumns = None
      ),
      leftSet = leftSet,
      rightSet = rightSet
    )

    val resolvedJoinWithOriginalOutput = retainOriginalOutputIfHiddenColumnIsReferenced(
      unresolvedAsOfJoin = unresolvedAsOfJoin,
      resolvedJoin = resolvedJoin,
      leftNameScope = leftNameScope,
      rightNameScope = rightNameScope
    )

    usingOutput match {
      case Some((outputList, hiddenList)) =>
        buildUsingProject(
          unresolvedAsOfJoin = unresolvedAsOfJoin,
          resolvedJoin = resolvedJoin,
          child = resolvedJoinWithOriginalOutput,
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
          resolvedOperator = resolvedJoinWithOriginalOutput
        )
    }
  }

  /**
   * If the join references a hidden column, wraps it in a [[Project]] that restores its original
   * output, plus the qualified-access-only columns unless the join is the top-level operator.
   */
  private def retainOriginalOutputIfHiddenColumnIsReferenced(
      unresolvedAsOfJoin: AsOfJoin,
      resolvedJoin: AsOfJoin,
      leftNameScope: NameScope,
      rightNameScope: NameScope): LogicalPlan = {
    val originalOutput = AsOfJoin.computeOutput(
      joinType = resolvedJoin.joinType,
      leftOutput = leftNameScope.output,
      rightOutput = rightNameScope.output
    )
    val originalOutputSet = AttributeSet(originalOutput)

    if (resolvedJoin.references.subsetOf(originalOutputSet)) {
      resolvedJoin
    } else {
      val qualifiedAccessOnlyColumns = AsOfJoin.computeOutput(
        joinType = resolvedJoin.joinType,
        leftOutput = leftNameScope.hiddenOutput.filter(_.qualifiedAccessOnly),
        rightOutput = rightNameScope.hiddenOutput.filter(_.qualifiedAccessOnly)
      )

      operatorResolutionContextStack.current.baseOperator = Some(resolvedJoin)
      Project(
        withQualifiedAccessOnlyColumns(
          unresolvedAsOfJoin = unresolvedAsOfJoin,
          output = originalOutput,
          qualifiedAccessOnlyColumns = qualifiedAccessOnlyColumns
        ),
        resolvedJoin
      )
    }
  }

  /**
   * Appends the qualified-access-only columns for the operators above, unless the join is the top
   * operator. [[PruneMetadataColumns]] later drops the ones they don't use.
   */
  private def withQualifiedAccessOnlyColumns(
      unresolvedAsOfJoin: AsOfJoin,
      output: Seq[NamedExpression],
      qualifiedAccessOnlyColumns: Seq[Attribute]): Seq[NamedExpression] = {
    if (unresolvedAsOfJoin.containsTag(ResolverTag.TOP_LEVEL_OPERATOR)) {
      output
    } else {
      output ++ qualifiedAccessOnlyColumns
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
      child: LogicalPlan,
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
    val projectList = withQualifiedAccessOnlyColumns(
      unresolvedAsOfJoin = unresolvedAsOfJoin,
      output = resolvedOutputList,
      qualifiedAccessOnlyColumns = qualifiedAccessOnlyColumns
    )

    operatorResolutionContextStack.current.baseOperator = Some(resolvedJoin)
    val project = Project(projectList, child)
    project.setTagValue(Project.hiddenOutputTag, qualifiedAccessOnlyColumns)
    project
  }

  private def resolveAsOfExpressions(
      unresolvedAsOfJoin: AsOfJoin,
      partiallyResolved: AsOfJoin,
      leftSet: AttributeSet,
      rightSet: AttributeSet): AsOfJoin = {
    val joinWithTolerance = partiallyResolved.copy(
      tolerance = partiallyResolved.tolerance.map(resolveExpressionInJoin(unresolvedAsOfJoin, _))
    )

    (
      joinWithTolerance.matchLeftOperand,
      joinWithTolerance.matchOperator,
      joinWithTolerance.matchRightOperand
    ) match {
      case (Some(unresolvedLeftOperand), matchOperator, Some(unresolvedRightOperand)) =>
        val leftOperand = resolveExpressionInJoin(unresolvedAsOfJoin, unresolvedLeftOperand)
        val rightOperand = resolveExpressionInJoin(unresolvedAsOfJoin, unresolvedRightOperand)

        val matchCondition = matchOperator match {
          case Some(operator) =>
            AsOfJoinMatchConditionResolution.materialize(
              join = joinWithTolerance,
              leftSet = leftSet,
              rightSet = rightSet,
              leftOperand = leftOperand,
              operator = operator,
              rightOperand = rightOperand
            )
          case None =>
            AsOfJoinMatchConditionResolution.materializeDataFrame(
              join = joinWithTolerance,
              leftSet = leftSet,
              rightSet = rightSet,
              leftOperand = leftOperand,
              rightOperand = rightOperand
            )
        }

        matchCondition match {
          case Some(materialized) =>
            resolvePreMaterializedExpressions(
              unresolvedAsOfJoin,
              materialized.toJoin(joinWithTolerance)
            )
          case None => joinWithTolerance
        }
      case (None, None, None) =>
        AsOfJoinValidation.validateTolerance(joinWithTolerance)
        resolvePreMaterializedExpressions(unresolvedAsOfJoin, joinWithTolerance)
      case _ => joinWithTolerance
    }
  }

  private def resolvePreMaterializedExpressions(
      unresolvedAsOfJoin: AsOfJoin,
      partiallyResolved: AsOfJoin): AsOfJoin = {
    val asOfCondition =
      resolveExpressionInJoin(unresolvedAsOfJoin, partiallyResolved.asOfCondition)
    val leftSortExpressions =
      partiallyResolved.leftSortExprs.map(resolveExpressionInJoin(unresolvedAsOfJoin, _))
    val rightSortExpressions =
      partiallyResolved.rightSortExprs.map(resolveExpressionInJoin(unresolvedAsOfJoin, _))

    partiallyResolved.copy(
      asOfCondition = asOfCondition,
      orderExpression = partiallyResolved.orderExpression.map(
        resolveExpressionInJoin(unresolvedAsOfJoin, _)),
      leftSortExprs = leftSortExpressions,
      rightSortExprs = rightSortExpressions
    )
  }

  private def resolveExpressionInJoin(
      unresolvedAsOfJoin: AsOfJoin,
      unresolvedExpression: Expression): Expression = {
    expressionResolver.resolveExpressionTreeInOperator(
      unresolvedExpression,
      unresolvedAsOfJoin
    )
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
