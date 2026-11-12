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

package org.apache.spark.sql.catalyst.analysis

import java.util.LinkedHashMap

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXISTS_SUBQUERY, LIST_SUBQUERY}

object NondeterministicExpressionCollection {
  /**
   * Assign one attribute to each nondeterministic evaluation unit in encounter order.
   * Aggregate accepts predicate subqueries that same-output unary operators reject.
   */
  def getNondeterministicToAttributes(
      expressions: Seq[Expression],
      allowPredicateSubqueries: Boolean = false): LinkedHashMap[Expression, NamedExpression] = {
    val nonDeterministicToAttributes = new LinkedHashMap[Expression, NamedExpression]

    for (expr <- expressions) {
      if (!expr.deterministic) {
        val expressionsToCollect =
          collectNondeterministicFrontier(expr, allowPredicateSubqueries)._2

        for (nondeterministicExpr <- expressionsToCollect.distinct) {
          val namedExpression = nondeterministicExpr match {
            case namedExpression: NamedExpression => namedExpression
            case _ => Alias(nondeterministicExpr, "_nondeterministic")()
          }
          nonDeterministicToAttributes.put(nondeterministicExpr.canonicalized, namedExpression)
        }
      }
    }

    nonDeterministicToAttributes
  }

  /** Expressions extracted by the original collector, including inside conditional branches. */
  private def isLegacyNondeterministic(expression: Expression): Boolean = expression match {
    case _: Nondeterministic | _: UserDefinedExpression | _: ExternalUserDefinedFunction => true
    case _ => false
  }

  /**
   * Returns a copy used only to classify the parent, plus the minimal nondeterministic
   * evaluation frontier below `expression`.
   *
   * A selected expression is represented by a deterministic attribute while walking upward. If
   * its parent is still nondeterministic after that substitution, the parent is the evaluation
   * unit and replaces the selected descendants in the frontier.
   *
   * New units must retain conditional evaluation, lambda scope and error handling. Predicate
   * subqueries move with a unit only when the original operator accepts them and their plans
   * are deterministic. ListQuery and Exists themselves are never extraction units.
   */
  private def collectNondeterministicFrontier(
      expression: Expression,
      allowPredicateSubqueries: Boolean): (Expression, Seq[Expression]) = {
    if (expression.deterministic || expression.isInstanceOf[ListQuery] ||
        expression.isInstanceOf[Exists]) {
      (expression, Seq.empty)
    } else {
      val childResults = expression.children.map(
        collectNondeterministicFrontier(_, allowPredicateSubqueries))
      val childrenWithResults = expression.children.zip(childResults)
      val retainEvaluationContext = expression match {
        case h: HigherOrderFunction =>
          childrenWithResults.exists { case (child, (_, frontier)) =>
            h.functions.contains(child) && frontier.nonEmpty
          }
        case c: ConditionalExpression =>
          childrenWithResults.exists { case (child, (_, frontier)) =>
            !c.alwaysEvaluatedInputs.contains(child) &&
              frontier.exists(e => !isLegacyNondeterministic(e))
          }
        case _: And | _: Or =>
          childResults(1)._2.exists(e => !isLegacyNondeterministic(e))
        case _: TryEval =>
          childResults.head._2.exists(e => !isLegacyNondeterministic(e))
        case _ => false
      }
      val expressionWithDeterministicChildren =
        expression.withNewChildren(childResults.map(_._1))

      // Subquery plans propagate pattern bits, but only predicates in this operator constrain
      // extraction. A scalar subquery may contain valid IN/EXISTS predicates in its own plan.
      val containsUnsupportedPredicateSubquery =
        expression.containsAnyPattern(LIST_SUBQUERY, EXISTS_SUBQUERY) && expression.exists {
          case s: ListQuery => !allowPredicateSubqueries || !s.plan.deterministic
          case s: Exists => !allowPredicateSubqueries || !s.plan.deterministic
          case _ => false
        }
      if (!containsUnsupportedPredicateSubquery &&
          (retainEvaluationContext || !expressionWithDeterministicChildren.deterministic)) {
        val placeholder = AttributeReference(
          "_nondeterministic", expression.dataType, expression.nullable)()
        (placeholder, Seq(expression))
      } else {
        (expressionWithDeterministicChildren, childResults.flatMap(_._2))
      }
    }
  }
}
