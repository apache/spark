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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{And, Expression, Not, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Try converting join condition to conjunctive normal form expression so that more predicates may
 * be able to be pushed down.
 * To avoid expanding the join condition, the join condition will be kept in the original form even
 * when predicate pushdown happens.
 */
object PushCNFPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Convert an expression into conjunctive normal form.
   * Definition and algorithm: https://en.wikipedia.org/wiki/Conjunctive_normal_form
   * CNF can explode exponentially in the size of the input expression when converting Or clauses.
   * Use a configuration MAX_CNF_NODE_COUNT to prevent such cases.
   *
   * @param condition to be conversed into CNF.
   * @return If the number of expressions exceeds threshold on converting Or, return Seq.empty.
   *         If the conversion repeatedly expands nondeterministic expressions, return Seq.empty.
   *         Otherwise, return the converted result as sequence of disjunctive expressions.
   */
  protected def conjunctiveNormalForm(condition: Expression): Seq[Expression] = {
    val postOrderNodes = postOrderTraversal(condition)
    val resultStack = new scala.collection.mutable.Stack[Seq[Expression]]
    val maxCnfNodeCount = SQLConf.get.maxCnfNodeCount
    // Bottom up approach to get CNF of sub-expressions
    while (postOrderNodes.nonEmpty) {
      val cnf = postOrderNodes.pop() match {
        case _: And =>
          val right: Seq[Expression] = resultStack.pop()
          val left: Seq[Expression] = resultStack.pop()
          left ++ right
        case _: Or =>
          val right: Seq[Expression] = resultStack.pop()
          val left: Seq[Expression] = resultStack.pop()
          // Stop the loop whenever the result exceeds the `maxCnfNodeCount`
          if (left.size * right.size > maxCnfNodeCount) {
            Seq.empty
          } else {
            for {x <- left; y <- right} yield Or(x, y)
          }
        case other => other :: Nil
      }
      if (cnf.isEmpty) {
        return Seq.empty
      }
      resultStack.push(cnf)
    }
    assert(resultStack.length == 1,
      s"Fail to convert expression ${condition} to conjunctive normal form")
    resultStack.top
  }

  /**
   * Iterative post order traversal over a binary tree built by And/Or clauses.
   * @param condition to be traversed as binary tree
   * @return sub-expressions in post order traversal as an Array.
   *         The first element of result Array is the leftmost node.
   */
  private def postOrderTraversal(condition: Expression): mutable.Stack[Expression] = {
    val stack = new mutable.Stack[Expression]
    val result = new mutable.Stack[Expression]
    stack.push(condition)
    while (stack.nonEmpty) {
      val node = stack.pop()
      node match {
        case Not(a And b) => stack.push(Or(Not(a), Not(b)))
        case Not(a Or b) => stack.push(And(Not(a), Not(b)))
        case Not(Not(a)) => stack.push(a)
        case a And b =>
          result.push(node)
          stack.push(a)
          stack.push(b)
        case a Or b =>
          result.push(node)
          stack.push(a)
          stack.push(b)
        case _ =>
          result.push(node)
      }
    }
    result
  }


  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ Join(left, right, joinType, Some(joinCondition), hint) =>
      val predicates = conjunctiveNormalForm(joinCondition)
      if (predicates.isEmpty || predicates.size > SQLConf.get.maxCnfNodeCount) {
        j
      } else {
        val pushDownCandidates = predicates.filter(_.deterministic)
        val leftFilterConditions = pushDownCandidates.filter(_.references.subsetOf(left.outputSet))
        val rightFilterConditions =
          pushDownCandidates.filter(_.references.subsetOf(right.outputSet))

        val newLeft =
          leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
        val newRight =
          rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)

        joinType match {
          case _: InnerLike | LeftSemi =>
            Join(newLeft, newRight, joinType, Some(joinCondition), hint)
          case RightOuter =>
            Join(newLeft, right, RightOuter, Some(joinCondition), hint)
          case LeftOuter | LeftAnti | ExistenceJoin(_) =>
            Join(left, newRight, joinType, Some(joinCondition), hint)
          case FullOuter => j
          case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
          case UsingJoin(_, _) => sys.error("Untransformed Using join node")
        }
      }
  }
}