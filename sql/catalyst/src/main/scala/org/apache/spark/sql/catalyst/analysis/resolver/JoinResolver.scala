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

import org.apache.spark.sql.catalyst.analysis.NaturalAndUsingJoinResolution
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{JoinType, NaturalJoin, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util._

/**
 * Resolves [[Join]] operator by resolving its left and right children and its join condition. If
 * the unresolved join is [[NaturalJoin]] or [[UsingJoin]], the resulting operator will be
 * [[Project]], otherwise it will be [[Join]].
 */
class JoinResolver(resolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Join, LogicalPlan] {
  private val scopes = resolver.getNameScopes
  private val expressionIdAssigner = expressionResolver.getExpressionIdAssigner
  private val cteRegistry = resolver.getCteRegistry

  /**
   * Resolves [[Join]] operator:
   *  - Retrieve old output and child outputs if the operator is already resolved. This is relevant
   *    for partially resolved subtrees from DataFrame programs. Do not regenerate ExprIds if there
   *    are no conflicting ids.
   *  - Resolve each child in the context of a) New [[NameScope]] b) New [[ExpressionIdAssigner]]
   *    mapping. Collect children name scopes to use in [[Join]] output computation.
   *  - Based on the type of [[Join]] (natural, using or other) perform additional transformations
   *  and resolve join condition.
   *  - Return the resulting [[Project]] or [[Join]] with new children optionally wrapped in
   *  [[WithCTE]]. See [[CteScope]] scaladoc for more info.
   */
  override def resolve(unresolvedJoin: Join): LogicalPlan = {
    val (resolvedLeftOperator: LogicalPlan, leftNameScope: NameScope) = resolveJoinChild(
      unresolvedJoin = unresolvedJoin,
      child = unresolvedJoin.left
    )

    val (resolvedRightOperator: LogicalPlan, rightNameScope: NameScope) = resolveJoinChild(
      unresolvedJoin = unresolvedJoin,
      child = unresolvedJoin.right
    )

    ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(
      Seq(leftNameScope.output, rightNameScope.output)
    )

    expressionIdAssigner.createMappingFromChildMappings()

    val partiallyResolvedJoin = unresolvedJoin.copy(
      left = resolvedLeftOperator,
      right = resolvedRightOperator
    )

    handleDifferentTypesOfJoin(
      unresolvedJoin = unresolvedJoin,
      partiallyResolvedJoin = partiallyResolvedJoin,
      leftNameScope = leftNameScope,
      rightNameScope = rightNameScope
    )
  }

  private def resolveJoinChild(
      unresolvedJoin: Join,
      child: LogicalPlan): (LogicalPlan, NameScope) = {
    scopes.withNewScope() {
      expressionIdAssigner.withNewMapping(collectChildMapping = true) {
        cteRegistry.withNewScopeUnderMultiChildOperator(
          unresolvedOperator = unresolvedJoin,
          unresolvedChild = child
        ) {
          val resolvedLeftOperator = resolver.resolve(child)
          (resolvedLeftOperator, scopes.current)
        }
      }
    }
  }

  /**
   * If the type of join is [[NaturalJoin]] or [[UsingJoin]], perform additional transformations in
   * [[commonNaturalJoinProcessing]]. Otherwise, overwrite current name scope output with the
   * result of [[Join.computeOutput]].
   */
  private def handleDifferentTypesOfJoin(
      unresolvedJoin: Join,
      partiallyResolvedJoin: Join,
      leftNameScope: NameScope,
      rightNameScope: NameScope): LogicalPlan = partiallyResolvedJoin match {
    case Join(left, right, UsingJoin(joinType, usingCols), _, hint) =>
      commonNaturalJoinProcessing(
        unresolvedJoin = unresolvedJoin,
        left = left,
        leftNameScope = leftNameScope,
        right = right,
        rightNameScope = rightNameScope,
        joinType = joinType,
        joinNames = usingCols,
        condition = None,
        hint = hint
      )
    case Join(left, right, NaturalJoin(joinType), condition, hint) =>
      val joinNames = getJoinNamesForNaturalJoin(leftNameScope, rightNameScope)
      commonNaturalJoinProcessing(
        unresolvedJoin = unresolvedJoin,
        left = left,
        leftNameScope = leftNameScope,
        right = right,
        rightNameScope = rightNameScope,
        joinType = joinType,
        joinNames = joinNames,
        condition = condition,
        hint = hint
      )
    case partiallyResolvedJoin: Join =>
      handleRegularJoin(
        unresolvedJoin = unresolvedJoin,
        partiallyResolvedJoin = partiallyResolvedJoin,
        leftNameScope = leftNameScope,
        rightNameScope = rightNameScope
      )
  }

  /**
   * This method handles [[NaturalJoin]] and [[UsingJoin]] by computing their correct outputs and
   * placing a [[Project]] node on top of them.
   * The order of necessary operations is as follows:
   *  - Compute output list, hidden list and new condition with join pairs, if there are any.
   *    [[NaturalAndUsingJoinResolution.computeJoinOutputsAndNewCondition]] introduces new
   *    aliased expressions (e.g. [[Coalesce]] for keys), so we need to run the output list through
   *    [[ExpressionIdAssigner.mapExpression]].
   *  - Resolve the new condition.
   *  - Compute new `hiddenOutput` by appending elements from computed hidden list that are not
   *  already in current `hiddenOutput`. Hidden list must be qualified access only.
   *  - Overwrite current name scope with output list and newly computed hidden output.
   *  - Finally, put a [[Project]] node on top of the original [[Join]] by:
   *    - New project list becomes output list.
   *    - If [[Join]] was not a top level operator, append current hidden output to the project
   *    list.
   *    - Add new hidden output as a tag to project node in order to stay compatible with
   *    fixed-point. This should never be used in single-pass, but it can happen that fixed-point
   *    uses the single-pass result, therefore we need to set the tag.
   */
  private def commonNaturalJoinProcessing(
      unresolvedJoin: Join,
      left: LogicalPlan,
      leftNameScope: NameScope,
      right: LogicalPlan,
      rightNameScope: NameScope,
      joinType: JoinType,
      joinNames: Seq[String],
      condition: Option[Expression],
      hint: JoinHint): LogicalPlan = {
    val (outputList, hiddenList, newCondition) =
      NaturalAndUsingJoinResolution.computeJoinOutputsAndNewCondition(
        left = left,
        leftOutput = leftNameScope.output,
        right = right,
        rightOutput = rightNameScope.output,
        joinType = joinType,
        joinNames = joinNames,
        condition = condition,
        resolveName = conf.resolver
      )

    val newOutputList = outputList.map(expressionIdAssigner.mapExpression)

    val resolvedCondition =
      resolveJoinCondition(unresolvedJoin, newCondition, leftNameScope, rightNameScope)

    val hiddenListWithQualifiedAccess = hiddenList.map(_.markAsQualifiedAccessOnly())

    val newHiddenOutput = hiddenListWithQualifiedAccess ++ scopes.current.hiddenOutput

    scopes.overwriteCurrent(
      output = Some(newOutputList.map(_.toAttribute)),
      hiddenOutput = Some(newHiddenOutput)
    )

    val newProjectList =
      if (unresolvedJoin.getTagValue(Resolver.TOP_LEVEL_OPERATOR).isEmpty) {
        newOutputList ++ scopes.current.hiddenOutput
          .filter(attribute => attribute.qualifiedAccessOnly)
      } else {
        newOutputList
      }

    val project = Project(newProjectList, Join(left, right, joinType, resolvedCondition, hint))

    project.setTagValue(Project.hiddenOutputTag, newHiddenOutput)

    project
  }

  /**
   * Resolve a join that is not [[NaturalJoin]] or [[UsingJoin]]. In order to resolve the join we
   * do the following:
   *  - Resolve join condition.
   *  - Overwrite [[NameScope.output]] with join's output.
   *  - Wrap the [[Join]] in [[WithCTE]] if necessary.
   */
  private def handleRegularJoin(
      unresolvedJoin: Join,
      partiallyResolvedJoin: Join,
      leftNameScope: NameScope,
      rightNameScope: NameScope) = {
    val resolvedCondition = resolveJoinCondition(
      unresolvedJoin = unresolvedJoin,
      unresolvedCondition = partiallyResolvedJoin.condition,
      leftNameScope = leftNameScope,
      rightNameScope = rightNameScope
    )

    scopes.overwriteCurrent(
      output = Some(
        Join.computeOutput(
          partiallyResolvedJoin.joinType,
          leftNameScope.output,
          rightNameScope.output
        )
      ),
      hiddenOutput = Some(leftNameScope.hiddenOutput ++ rightNameScope.hiddenOutput)
    )

    val resolvedJoin = partiallyResolvedJoin.copy(condition = resolvedCondition)

    cteRegistry.currentScope.tryPutWithCTE(
      unresolvedOperator = unresolvedJoin,
      resolvedOperator = resolvedJoin
    )
  }

  /**
   * Computes the intersection of two child name scopes, by name.
   */
  private def getJoinNamesForNaturalJoin(
      leftNameScope: NameScope,
      rightNameScope: NameScope): Seq[String] = {
    leftNameScope.output
      .flatMap(attribute => rightNameScope.findAttributesByName(attribute.name))
      .map(_.name)
  }

  /**
   * Resolves join condition by __all__ attributes from child scopes. We need to overwrite current
   * scope first to prepare for [[resolveExpressionTreeInOperator]]. [[Join]] will actually produce
   * different output than the one we are setting here, so additional overwrite with correct values
   * will be needed. Two overwrites are necessary because condition is resolved from original
   * children outputs, whereas output of [[Join]] will either not contain all attributes or their
   * nullabilities will be different.
   */
  private def resolveJoinCondition(
      unresolvedJoin: Join,
      unresolvedCondition: Option[Expression],
      leftNameScope: NameScope,
      rightNameScope: NameScope) = {
    scopes.overwriteCurrent(
      output = Some(leftNameScope.output ++ rightNameScope.output),
      hiddenOutput = Some(leftNameScope.hiddenOutput ++ rightNameScope.hiddenOutput)
    )

    unresolvedCondition.map { condition =>
      expressionResolver.resolveExpressionTreeInOperator(
        condition,
        unresolvedJoin
      )
    }
  }
}
