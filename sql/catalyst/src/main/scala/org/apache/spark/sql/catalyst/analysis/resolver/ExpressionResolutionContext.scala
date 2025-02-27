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
 * @param hasAggregateExpressionsInASubtree A flag that highlights that a specific node
 *                                    corresponding to [[ExpressionResolutionContext]] has
 *                                    aggregate expressions in its subtree.
 * @param hasAttributeInASubtree A flag that highlights that a specific node corresponding to
 *                         [[ExpressionResolutionContext]] has attributes in its subtree.
 * @param hasLateralColumnAlias A flag that highlights that a specific node corresponding to
 *                        [[ExpressionResolutionContext]] has LCA in its subtree.
 */
class ExpressionResolutionContext(
    var hasAggregateExpressionsInASubtree: Boolean = false,
    var hasAttributeInASubtree: Boolean = false,
    var hasLateralColumnAlias: Boolean = false) {
  def merge(other: ExpressionResolutionContext): Unit = {
    hasAggregateExpressionsInASubtree |= other.hasAggregateExpressionsInASubtree
    hasAttributeInASubtree |= other.hasAttributeInASubtree
    hasLateralColumnAlias |= other.hasLateralColumnAlias
  }
}
