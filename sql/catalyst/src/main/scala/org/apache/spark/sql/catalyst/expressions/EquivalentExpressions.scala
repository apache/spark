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

package org.apache.spark.sql.catalyst.expressions

import java.util.Objects

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.TreePattern.{LAMBDA_VARIABLE, PLAN_EXPRESSION}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 */
class EquivalentExpressions(
    skipForShortcutEnable: Boolean = SQLConf.get.subexpressionEliminationSkipForShotcutExpr) {

  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */
  def addExpr(expr: Expression): Boolean = {
    if (supportedExpression(expr)) {
      updateExprInMap(expr, equivalenceMap)
    } else {
      false
    }
  }

  /**
   * Adds or removes an expression to/from the map and updates `useCount`.
   * Returns true
   * - if there was a matching expression in the map before add or
   * - if there remained a matching expression in the map after remove (`useCount` remained > 0)
   * to indicate there is no need to recurse in `updateExprTree`.
   */
  private def updateExprInMap(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats],
      useCount: Int = 1,
      conditional: Boolean = false): Boolean = {
    if (expr.deterministic) {
      val wrapper = ExpressionEquals(expr)
      map.get(wrapper) match {
        case Some(stats) =>
          val count = if (conditional) {
            stats.conditionalUseCount += useCount
            stats.conditionalUseCount
          } else {
            stats.useCount += useCount
            stats.useCount
          }
          if (count > 0) {
            true
          } else if (count == 0) {
            map -= wrapper
            false
          } else {
            // Should not happen
            throw SparkException.internalError(
              s"Cannot update expression: $expr in map: $map with use count: $useCount")
          }
        case _ =>
          if (useCount > 0) {
            val stats = if (conditional) {
              ExpressionStats(expr)(0, useCount)
            } else {
              ExpressionStats(expr)(useCount)
            }
            map.put(wrapper, stats)
          }
          false
      }
    } else {
      false
    }
  }

  /**
   * Adds or removes only expressions which are common in each of given expressions, in a recursive
   * way.
   * For example, given two expressions `(a + (b + (c + 1)))` and `(d + (e + (c + 1)))`, the common
   * expression `(c + 1)` will be added into `equivalenceMap`.
   *
   * Note that as we don't know in advance if any child node of an expression will be common across
   * all given expressions, we compute local equivalence maps for all given expressions and filter
   * only the common nodes.
   * Those common nodes are then removed from the local map and added to the final map of
   * expressions.
   *
   * Conditional expressions are not considered because we are simply looking for expressions
   * evaluated once in each parent expression.
   */
  private def updateCommonExprs(exprs: Seq[Expression]): Seq[ExpressionEquals] = {
    assert(exprs.length > 1)
    var localEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
    updateExprTree(exprs.head, localEquivalenceMap, conditionalsEnabled = false)

    exprs.tail.foreach { expr =>
      val otherLocalEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
      updateExprTree(expr, otherLocalEquivalenceMap, conditionalsEnabled = false)
      localEquivalenceMap = localEquivalenceMap.filter { case (key, _) =>
        otherLocalEquivalenceMap.contains(key)
      }
    }

    val commonExpressions = mutable.ListBuffer.empty[ExpressionEquals]

    // Start with the highest expression, remove it from `localEquivalenceMap` and add it to `map`.
    // The remaining highest expression in `localEquivalenceMap` is also common expression so loop
    // until `localEquivalenceMap` is not empty.
    var statsOption = Some(localEquivalenceMap).filter(_.nonEmpty).map(_.maxBy(_._1.height)._2)
    while (statsOption.nonEmpty) {
      val stats = statsOption.get
      updateExprTree(stats.expr, localEquivalenceMap, -stats.useCount, conditionalsEnabled = false)
      commonExpressions += ExpressionEquals(stats.expr)

      statsOption = Some(localEquivalenceMap).filter(_.nonEmpty).map(_.maxBy(_._1.height)._2)
    }
    commonExpressions.toSeq
  }

  private def skipForShortcut(expr: Expression): Expression = {
    if (skipForShortcutEnable) {
      // The subexpression may not need to eval even if it appears more than once.
      // e.g., `if(or(a, and(b, b)))`, the expression `b` would be skipped if `a` is true.
      expr match {
        case and: And => and.left
        case or: Or => or.left
        case other => other
      }
    } else {
      expr
    }
  }

  /**
   * There are some expressions that need special handling:
   *    1. CodegenFallback: It's children will not be used to generate code (call eval() instead).
   *    2. ConditionalExpression: use its children that will always be evaluated.
   */
  private def childrenToRecurse(expr: Expression): RecurseChildren = expr match {
    case _: CodegenFallback => RecurseChildren(Nil)
    case c: ConditionalExpression =>
      RecurseChildren(c.alwaysEvaluatedInputs.map(skipForShortcut), c.branchGroups,
        c.conditionallyEvaluatedInputs)
    case other => RecurseChildren(skipForShortcut(other).children)
  }

  private def supportedExpression(e: Expression): Boolean = {
    // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
    // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
    !(e.containsPattern(LAMBDA_VARIABLE) ||
      // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
      // can cause error like NPE.
      (e.containsPattern(PLAN_EXPRESSION) && Utils.isInRunningSparkTask))
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   */
  def addExprTree(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats] = equivalenceMap): Unit = {
    if (supportedExpression(expr)) {
      updateExprTree(expr, map)
    }
  }

  private def updateExprTree(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats] = equivalenceMap,
      useCount: Int = 1,
      conditionalsEnabled: Boolean = SQLConf.get.subexpressionEliminationConditionalsEnabled,
      conditional: Boolean = false,
      skipExpressions: Set[ExpressionEquals] = Set.empty[ExpressionEquals]
      ): Unit = {
    val skip = useCount == 0 ||
      expr.isInstanceOf[LeafExpression] ||
      skipExpressions.contains(ExpressionEquals(expr))

    if (!skip && !updateExprInMap(expr, map, useCount, conditional)) {
      val uc = useCount.sign
      val recurseChildren = childrenToRecurse(expr)
      recurseChildren.alwaysChildren.foreach { child =>
        updateExprTree(child, map, uc, conditionalsEnabled, conditional, skipExpressions)
      }

      /**
       * If the `commonExpressions` already appears in the equivalence map, calling `addExprTree`
       * will increase the `useCount` and mark it as a common subexpression. Otherwise,
       * `addExprTree` will recursively add `commonExpressions` and its descendant to the
       * equivalence map, in case they also appear in other places. For example,
       * `If(a + b > 1, a + b + c, a + b + c)`, `a + b` also appears in the condition and should
       * be treated as common subexpression.
       */
      val commonExpressions = recurseChildren.commonChildren.flatMap { exprs =>
        if (exprs.nonEmpty) {
          updateCommonExprs(exprs)
        } else {
          Nil
        }
      }
      commonExpressions.foreach { ce =>
        updateExprTree(ce.e, map, uc, conditionalsEnabled, conditional, skipExpressions)
      }

      if (conditionalsEnabled) {
        // Add all conditional expressions, skipping those that were already counted as common
        // expressions.
        recurseChildren.conditionalChildren.foreach { cc =>
          updateExprTree(cc, map, uc, true, true, commonExpressions.toSet)
        }
      }
    }
  }

  /**
   * Returns the state of the given expression in the `equivalenceMap`. Returns None if there is no
   * equivalent expressions.
   */
  def getExprState(e: Expression): Option[ExpressionStats] = {
    if (supportedExpression(e)) {
      equivalenceMap.get(ExpressionEquals(e))
    } else {
      None
    }
  }

  // Exposed for testing.
  private[sql] def getAllExprStates(count: Int = 0): Seq[ExpressionStats] = {
    equivalenceMap.filter(_._2.getUseCount() > count).toSeq.sortBy(_._1.height).map(_._2)
  }

  /**
   * Returns a sequence of expressions that more than one equivalent expressions.
   */
  def getCommonSubexpressions: Seq[Expression] = {
    getAllExprStates(1).map(_.expr)
  }

  /**
   * Returns the state of the data structure as a string. If `all` is false, skips sets of
   * equivalent expressions with cardinality 1.
   */
  def debugString(all: Boolean = false): String = {
    val sb = new java.lang.StringBuilder()
    sb.append("Equivalent expressions:\n")
    equivalenceMap.values.filter(stats => all || stats.getUseCount() > 1).foreach { stats =>
      sb.append("  ")
        .append(s"${stats.expr}: useCount = ${stats.useCount} ")
        .append(s"conditionalUseCount = ${stats.conditionalUseCount}")
        .append('\n')
    }
    sb.toString()
  }
}

/**
 * Wrapper around an Expression that provides semantic equality.
 */
case class ExpressionEquals(e: Expression) {
  // This is used to do a fast pre-check for child-parent relationship. For example, expr1 can
  // only be a parent of expr2 if expr1.height is larger than expr2.height.
  def height: Int = e.height

  override def equals(o: Any): Boolean = o match {
    case other: ExpressionEquals => e.semanticEquals(other.e) && height == other.height
    case _ => false
  }

  override def hashCode: Int = Objects.hash(e.semanticHash(): Integer, height: Integer)
}

/**
 * A wrapper in place of using Seq[Expression] to record a group of equivalent expressions.
 *
 * This saves a lot of memory when there are a lot of expressions in a same equivalence group.
 * Instead of appending to a mutable list/buffer of Expressions, just update the "flattened"
 * useCount in this wrapper in-place.
 */
case class ExpressionStats(expr: Expression)(
    var useCount: Int = 1,
    var conditionalUseCount: Int = 0) {
  def getUseCount(): Int = if (useCount > 0) {
    useCount + conditionalUseCount
  } else {
    0
  }
}

/**
 * A wrapper for the different types of children of expressions. `alwaysChildren` are child
 * expressions that will always be evaluated and should be considered for subexpressions.
 * `commonChildren` are children such that if there are any common expressions among them, those
 * should be considered for subexpressions. `conditionalChildren` are children that are
 * conditionally evaluated, such as in If, CaseWhen, or Coalesce expressions, and should only
 * be considered for subexpressions if they are evaluated non-conditionally elsewhere.
 */
case class RecurseChildren(
    alwaysChildren: Seq[Expression],
    commonChildren: Seq[Seq[Expression]] = Nil,
    conditionalChildren: Seq[Expression] = Nil
  )
