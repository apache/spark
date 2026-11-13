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

import java.util.HashMap

import org.apache.spark.sql.catalyst.analysis.NaturalAndUsingJoinResolution
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{JoinType, NaturalJoin, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util._

/**
 * Resolves [[Join]] operator by resolving its left and right children and its join condition. If
 * the unresolved join is [[NaturalJoin]] or [[UsingJoin]], the resulting operator will be
 * [[Project]], otherwise it will be [[Join]], or a [[Project]] over it that keeps the join's output
 * (see `retainTopLevelJoinOutput`).
 */
class JoinResolver(
    override val resolver: Resolver,
    override val expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Join, LogicalPlan]
    with JoinLikeResolver {

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
   *    [[WithCTE]]. See [[CteScope]] scaladoc for more info.
   *  - Since joins interact with Recursive CTEs in such a way that they disallow self references if
   *    the self reference is on a specific side of the join operator, the appropriate [[CteScope]]
   *    is marked as disallowing self references according to the rules defined in
   *    [[ResolveWithCTE.checkIfSelfReferenceIsPlacedCorrectly]]
   */
  override def resolve(unresolvedJoin: Join): LogicalPlan = {
    val (allowLeftRecursiveCTE, allowRightRecursiveCTE) =
      getRecursiveCteAllowance(unresolvedJoin.joinType)

    val (resolvedLeftOperator: LogicalPlan, leftNameScope: NameScope) = resolveJoinChild(
      unresolvedOperator = unresolvedJoin,
      child = unresolvedJoin.left,
      shouldDisallowRecursiveCtes = !allowLeftRecursiveCTE
    )

    val (resolvedRightOperator: LogicalPlan, rightNameScope: NameScope) = resolveJoinChild(
      unresolvedOperator = unresolvedJoin,
      child = unresolvedJoin.right,
      shouldDisallowRecursiveCtes = !allowRightRecursiveCTE
    )

    ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(
      Seq(leftNameScope.output, rightNameScope.output)
    )

    mergeChildMappings(
      leftNameScope = leftNameScope,
      rightNameScope = rightNameScope
    )

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
   *  - Filter metadata columns in the current `hiddenOutput` based on the join type via
   *  [[filterHiddenOutputMetadataForJoin]] (e.g. right-side metadata columns are removed for
   *  left-existence joins and become nullable for left outer joins).
   *  - Compute new `hiddenOutput` by prepending the output attributes (so they remain available
   *  for resolution in downstream operators like `Sort` or `Filter` even when not directly in the
   *  `SELECT` list) and appending elements from computed hidden list that are not already in
   *  current `hiddenOutput`. Hidden list must be qualified access only. Output attributes take
   *  precedence over hidden attributes.
   *  - Overwrite current name scope with output list, newly computed hidden output and the
   *  [[NameScope.qualifiedAccessOnlyOutputIds]] of the [[Project]] (see
   *  [[computeQualifiedAccessOnlyOutputIdsForJoinProject]]). If [[Join]] was not a top level
   *  operator, these are the qualified access only attributes from the new hidden output that the
   *  [[Join]] outputs.
   *  - Finally, put a [[Project]] node on top of the original [[Join]] by:
   *    - New project list becomes output list, followed by
   *    [[NameScope.qualifiedAccessOnlyOutput]].
   *    - Store the resolved [[Join]] as the [[OperatorResolutionContextStack.baseOperator]].
   *    - Add qualified access only attributes from new hidden output as a tag to project node in
   *    order to stay compatible with fixed-point. This should never be used in single-pass, but it
   *    can happen that fixed-point uses the single-pass result, therefore we need to set the tag.
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

    val newOutputList = outputList.map { column =>
      expressionResolver
        .resolveExpressionTreeInOperator(column, unresolvedJoin)
        .asInstanceOf[NamedExpression]
    }

    val resolvedCondition =
      resolveJoinCondition(unresolvedJoin, newCondition, leftNameScope, rightNameScope)

    val newOutputAttributes = newOutputList.map(_.toAttribute)

    val filteredHiddenOutput = filterHiddenOutputMetadataForJoin(
      joinType = joinType,
      oldHiddenOutput = scopes.current.hiddenOutput,
      rightHiddenOutput = rightNameScope.hiddenOutput
    )

    val newHiddenOutput = computeHiddenOutputForJoin(
      mainOutput = newOutputAttributes,
      oldHiddenOutput = filteredHiddenOutput,
      extraHiddenOutput = hiddenList
    )

    scopes.overwriteCurrent(
      output = Some(newOutputAttributes),
      hiddenOutput = Some(newHiddenOutput),
      qualifiedAccessOnlyOutputIds = Some(
        computeQualifiedAccessOnlyOutputIdsForJoinProject(
          unresolvedJoin = unresolvedJoin,
          hiddenOutput = newHiddenOutput,
          joinType = joinType,
          leftNameScope = leftNameScope,
          rightNameScope = rightNameScope
        )
      )
    )

    val qualifiedAccessOnlyColumnsFromHiddenOutput =
      scopes.current.hiddenOutput.filter(_.qualifiedAccessOnly)

    val resolvedJoin = Join(left, right, joinType, resolvedCondition, hint)

    operatorResolutionContextStack.current.baseOperator = Some(resolvedJoin)

    val project = Project(newOutputList ++ scopes.current.qualifiedAccessOnlyOutput, resolvedJoin)

    project.setTagValue(Project.hiddenOutputTag, qualifiedAccessOnlyColumnsFromHiddenOutput)

    project
  }

  /**
   * Resolve a join that is not [[NaturalJoin]] or [[UsingJoin]]. In order to resolve the join we
   * do the following:
   *  - Resolve join condition.
   *  - Filter metadata columns in the current `hiddenOutput` based on the join type via
   *    [[filterHiddenOutputMetadataForJoin]].
   *  - Overwrite [[NameScope.output]] with join's output, overwrite [[NameScope.hiddenOutput]]
   *    using the result of `computeHiddenOutputForJoin` call and overwrite
   *    [[NameScope.qualifiedAccessOnlyOutputIds]] using the result of
   *    `computeQualifiedAccessOnlyOutputIdsForJoin` call.
   *  - Wrap a top-level [[Join]] in a [[Project]] with its output if necessary (see
   *    `retainTopLevelJoinOutput`).
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

    val conditionReferencedAttributes = if (partiallyResolvedJoin.condition.isDefined) {
      expressionResolver.getLastReferencedAttributes
    } else {
      new HashMap[ExprId, Attribute]
    }

    val newOutput = Join.computeOutput(
      joinType = partiallyResolvedJoin.joinType,
      leftOutput = leftNameScope.output,
      rightOutput = rightNameScope.output
    )

    val filteredHiddenOutput = filterHiddenOutputMetadataForJoin(
      joinType = partiallyResolvedJoin.joinType,
      oldHiddenOutput = scopes.current.hiddenOutput,
      rightHiddenOutput = rightNameScope.hiddenOutput
    )

    val newHiddenOutput = computeHiddenOutputForJoin(
      mainOutput = newOutput,
      oldHiddenOutput = filteredHiddenOutput
    )

    scopes.overwriteCurrent(
      output = Some(newOutput),
      hiddenOutput = Some(newHiddenOutput),
      qualifiedAccessOnlyOutputIds = Some(
        computeQualifiedAccessOnlyOutputIdsForJoin(
          joinType = partiallyResolvedJoin.joinType,
          leftNameScope = leftNameScope,
          rightNameScope = rightNameScope
        )
      )
    )

    val resolvedJoin = retainTopLevelJoinOutput(
      unresolvedJoin = unresolvedJoin,
      resolvedJoin = partiallyResolvedJoin.copy(condition = resolvedCondition),
      conditionReferencedAttributes = conditionReferencedAttributes
    )

    cteRegistry.currentScope.tryPutWithCTE(
      unresolvedOperator = unresolvedJoin,
      resolvedOperator = resolvedJoin
    )
  }

  /**
   * Wraps a top-level [[Join]] in a [[Project]] with the join's output when its condition
   * references a column found only in the hidden output, such as the hidden key of a USING join
   * below, which is added to a child's output, and the column is in the join's own output. A
   * LEFT SEMI or LEFT ANTI join outputs only its left side, so a hidden key of its right side does
   * not change its output, and fixed-point does not wrap it either. A join that is not top-level
   * is wrapped by the resolver of the operator above it instead (see
   * [[RetainsOriginalJoinOutput]]). For example:
   *
   * {{{
   * FROM nt1 JOIN nt2 USING (k) JOIN nt2 x ON nt2.k = x.k
   *
   * Project [k#1, v1#2, v2#3, k#5, v2#6]
   * +- Join Inner, (k#4 = k#5)
   *    :- Project [k#1, v1#2, v2#3, k#4]
   *    :  +- Join Inner, (k#1 = k#4)
   *    +- ...
   * }}}
   */
  private def retainTopLevelJoinOutput(
      unresolvedJoin: Join,
      resolvedJoin: Join,
      conditionReferencedAttributes: HashMap[ExprId, Attribute]): LogicalPlan = {
    def referencesHiddenOutput: Boolean = {
      val joinOutput = resolvedJoin.outputSet
      scopes.current
        .resolveMissingAttributesByHiddenOutput(conditionReferencedAttributes)
        .exists(joinOutput.contains)
    }
    if (unresolvedJoin.containsTag(ResolverTag.TOP_LEVEL_OPERATOR) && referencesHiddenOutput) {
      operatorResolutionContextStack.current.baseOperator = Some(resolvedJoin)
      Project(scopes.current.output, resolvedJoin)
    } else {
      resolvedJoin
    }
  }

  /**
   * Computes the intersection of two child name scopes, by name.
   */
  private def getJoinNamesForNaturalJoin(
      leftNameScope: NameScope,
      rightNameScope: NameScope): Seq[String] = {
    NaturalAndUsingJoinResolution.canonicalizedIntersect(
      leftNameScope.output.map(_.name),
      rightNameScope.output.map(_.name)
    )
  }
}
