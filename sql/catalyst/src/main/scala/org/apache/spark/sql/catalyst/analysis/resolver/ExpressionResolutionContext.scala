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

/**
 * The [[ExpressionResolutionContext]] is a state that is propagated between the nodes of the
 * expression tree during the bottom-up expression resolution process. This way we pass the results
 * of [[ExpressionResolver.resolve]] call, which are not the resolved child itself, from children
 * to parents.
 *
 * @param isRoot A flag indicating that we are resolving the root of the expression tree. It's
 *   used by the [[ExpressionResolver]] to correctly propagate top-level information like
 *   [[isTopOfProjectList]]. It's going to be set to `true` for the top-level context when we are
 *   entering expression resolution from a specific operator (this can be either a top-level query
 *   or a subquery).
 * @param hasLocalReferences A flag that highlights that a specific node corresponding to
 *   [[ExpressionResolutionContext]] has local [[AttributeReferences]] in its subtree.
 * @param hasOuterReferences A flag that highlights that a specific node corresponding to
 *   [[ExpressionResolutionContext]] has outer [[AttributeReferences]] in its subtree.
 * @param hasAggregateExpressions A flag that highlights that a specific node
 *   corresponding to [[ExpressionResolutionContext]] has aggregate expressions in its subtree.
 * @param hasAttributeOutsideOfAggregateExpressions A flag that highlights that a specific node
 *   corresponding to [[ExpressionResolutionContext]] has attributes in its subtree which are not
 *   under an [[AggregateExpression]].
 * @param hasLateralColumnAlias A flag that highlights that a specific node corresponding to
 *   [[ExpressionResolutionContext]] has LCA in its subtree.
 * @param isTopOfProjectList A flag indicating that we are resolving top of [[Project]] list.
 *   Otherwise, extra [[Alias]]es have to be stripped away.
 * @param resolvingGroupingExpressions A flag indicating whether an expression we are resolving is
 *   one of [[Aggregate.groupingExpressions]].
 * @param resolvingTreeUnderAggregateExpression A flag indicating whether an expression we are
 *   resolving a tree under [[AggregateExpression]].
 */
class ExpressionResolutionContext(
    val isRoot: Boolean = false,
    var hasLocalReferences: Boolean = false,
    var hasOuterReferences: Boolean = false,
    var hasAggregateExpressions: Boolean = false,
    var hasAttributeOutsideOfAggregateExpressions: Boolean = false,
    var hasLateralColumnAlias: Boolean = false,
    var isTopOfProjectList: Boolean = false,
    var resolvingGroupingExpressions: Boolean = false,
    var resolvingTreeUnderAggregateExpression: Boolean = false) {

  /**
   * Propagate generic information that is valid across the whole expression tree from the
   * [[child]] context.
   */
  def mergeChild(child: ExpressionResolutionContext): Unit = {
    hasLocalReferences |= child.hasLocalReferences
    hasOuterReferences |= child.hasOuterReferences
    hasAggregateExpressions |= child.hasAggregateExpressions
    hasAttributeOutsideOfAggregateExpressions |= child.hasAttributeOutsideOfAggregateExpressions
    hasLateralColumnAlias |= child.hasLateralColumnAlias
  }
}

object ExpressionResolutionContext {

  /**
   * Create a new child [[ExpressionResolutionContext]]. Propagates the relevant information
   * top-down.
   */
  def createChild(parent: ExpressionResolutionContext): ExpressionResolutionContext = {
    if (parent.isRoot) {
      new ExpressionResolutionContext(
        isTopOfProjectList = parent.isTopOfProjectList,
        resolvingGroupingExpressions = parent.resolvingGroupingExpressions
      )
    } else {
      new ExpressionResolutionContext(
        resolvingGroupingExpressions = parent.resolvingGroupingExpressions,
        resolvingTreeUnderAggregateExpression = parent.resolvingTreeUnderAggregateExpression
      )
    }
  }
}
