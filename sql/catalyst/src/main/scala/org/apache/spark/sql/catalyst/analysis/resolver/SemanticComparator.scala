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

import java.util.{ArrayList, HashMap}

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * [[SemanticComparator]] is a tool to compare expressions semantically to a predefined sequence
 * of `targetExpressions`. Semantic comparison is based on [[QueryPlan.canonicalized]] - for
 * example, `col1 + 1 + col2` is semantically equal to `1 + col2 + col1`. To speed up slow tree
 * traversals and expression node field comparisons, we cache the semantic hashes (which is
 * simply a hash of a canonicalized subtree) and use them for O(1) indexing. If the hashes don't
 * match, we perform an early return. Otherwise, we invoke the heavy [[Expression.semanticEquals]]
 * method to make sure that expression trees are indeed identical.
 */
class SemanticComparator(targetExpressions: Seq[Expression]) {
  private val targetExpressionsBySemanticHash =
    new HashMap[Int, ArrayList[Expression]](targetExpressions.size)

  for (targetExpression <- targetExpressions) {
    targetExpressionsBySemanticHash
      .computeIfAbsent(targetExpression.semanticHash(), _ => new ArrayList[Expression])
      .add(targetExpression)
  }

  /**
   * Returns the first expression in `targetExpressions` that is semantically equal to the given
   * `expression`. If no such expression is found, returns `None`.
   */
  def collectFirst(expression: Expression): Option[Expression] = {
    targetExpressionsBySemanticHash.get(expression.semanticHash()) match {
      case null =>
        None
      case targetExpressions =>
        val iter = targetExpressions.iterator
        var matchedExpression: Option[Expression] = None
        while (iter.hasNext && matchedExpression.isEmpty) {
          val element = iter.next
          if (element.semanticEquals(expression)) {
            matchedExpression = Some(element)
          }
        }
        matchedExpression
    }
  }

  /**
   * Use the previously constructed `targetExpressionsBySemanticHash` to check if the given
   * `expression` is semantically equal to any of the target expressions.
   */
  def exists(expression: Expression): Boolean = {
    collectFirst(expression).nonEmpty
  }
}
