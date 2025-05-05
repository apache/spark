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

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * A mixin trait for expression resolvers that as part of their resolution, replace single node
 * with a subtree of nodes. This step is necessary because the underlying legacy code that is being
 * called produces partially-unresolved subtrees. In order to resolve the subtree a callback
 * resolver is called recursively. This callback must ensure that no node is resolved twice in
 * order to not break the single-pass invariant. This is done by tagging the limits of this
 * traversal with [[ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY]] tag. This tag is applied to
 * the original expression's children, which are guaranteed to be resolved at the time of given
 * expression's resolution. When callback resolver encounters the node that is tagged, it should
 * return identity instead of trying to resolve it.
 */
trait ProducesUnresolvedSubtree extends ResolvesExpressionChildren {

  /**
   * Helper method used to resolve a subtree that is generated as part of the resolution of some
   * node. Method ensures that the downwards traversal never visits previously resolved nodes by
   * tracking the limits of the traversal with a tag. Invokes a resolver callback to resolve
   * children, but DOES NOT resolve the root of the subtree.
   *
   * If the result of the callback is the same object as the source `expression`, we don't perform
   * the downwards traversal. This is both more optimal and a fail-safe mechanism in case we
   * accidentally lose the [[ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY]] tag.
   */
  protected def withResolvedSubtree(
      expression: Expression,
      expressionResolver: Expression => Expression)(body: => Expression): Expression = {
    expression.children.foreach { child =>
      child.setTagValue(ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY, ())
    }

    val resultExpression = body

    if (resultExpression.eq(expression)) {
      expression.children.foreach { child =>
        child.unsetTagValue(ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY)
      }
      resultExpression
    } else {
      withResolvedChildren(resultExpression, expressionResolver)
    }
  }

  /**
   * Try to pop the tag that marks the boundary of the single-pass subtree resolution.
   * [[ExpressionResolver]] calls this method to check if the subtree traversal needs to be stopped
   * because lower subtree is already resolved.
   */
  protected def tryPopSinglePassSubtreeBoundary(unresolvedExpression: Expression): Boolean = {
    if (unresolvedExpression
        .getTagValue(ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY)
        .isDefined) {
      unresolvedExpression.unsetTagValue(ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY)
      true
    } else {
      false
    }
  }
}
