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

import java.util.{ArrayDeque, ArrayList, HashMap}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Properties of a current expression tree traversal.
 *
 * @param parentOperator The parent operator of the current expression tree.
 * @param invalidExpressionsInTheContextOfOperator The expressions that are invalid in the context
 *   of the current expression tree and its parent operator.
 * @param referencedAttributes All attributes that are referenced during the resolution of
 *   expression trees.
 * @param extractedAggregateExpressionAliases The aliases of the [[AggregateExpressions]] that are
 *   extracted during the resolution of expression trees.
 */
case class ExpressionTreeTraversal(
    parentOperator: LogicalPlan,
    invalidExpressionsInTheContextOfOperator: ArrayList[Expression] = new ArrayList[Expression],
    referencedAttributes: HashMap[ExprId, Attribute] = new HashMap[ExprId, Attribute],
    extractedAggregateExpressionAliases: ArrayList[Alias] = new ArrayList[Alias]
)

/**
 * The stack of expression tree traversal properties which are accumulated during the resolution
 * of a certain expression tree. This is filled by the
 * [[ExpressionResolver.resolveExpressionTreeInOperatorImpl]],
 * and will usually have size 1. However, in case of subquery expressions we would call
 * [[ExpressionResolver.resolveExpressionTreeInOperatorImpl]] several times recursively
 * for each expression tree in the operator tree -> expression tree -> operator tree ->
 * expression tree -> ... chain. Consider this example:
 *
 * {{{
 * SELECT
 *   col1
 * FROM
 *   VALUES (1) AS t1
 * WHERE EXISTS (
 *   SELECT
 *     *
 *   FROM
 *     VALUES (2) AS t2
 *   WHERE
 *     (SELECT col1 FROM VALUES (3) AS t3) == t1.col1
 * )
 * }}}
 *
 * We would have 3 nested stack entries for while resolving the lower scalar subquery (with the `t3`
 * table).
 */
class ExpressionTreeTraversalStack {
  private val stack = new ArrayDeque[ExpressionTreeTraversal]

  /**
   * Current expression tree traversal properties. Must exist when resolving an expression tree.
   */
  def current: ExpressionTreeTraversal = {
    if (stack.isEmpty) {
      throw SparkException.internalError("No current expression tree traversal")
    }
    stack.peek()
  }

  /**
   * Pushes a new [[ExpressionTreeTraversal]] object, executes the `body` and finally pops the
   * traversal from the stack.
   */
  def withNewTraversal[R](parentOperator: LogicalPlan)(body: => R): R = {
    stack.push(ExpressionTreeTraversal(parentOperator = parentOperator))
    try {
      body
    } finally {
      stack.pop()
    }
  }
}
