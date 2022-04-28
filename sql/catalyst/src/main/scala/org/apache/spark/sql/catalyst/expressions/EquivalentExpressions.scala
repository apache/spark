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

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable
import org.apache.spark.util.Utils

/**
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 */
class EquivalentExpressions {
  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */
  def addExpr(expr: Expression): Boolean = {
    updateExprInMap(expr, equivalenceMap)
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
      useCount: Int = 1): Boolean = {
    if (expr.deterministic) {
      val wrapper = ExpressionEquals(expr)
      map.get(wrapper) match {
        case Some(stats) =>
          stats.useCount += useCount
          if (stats.useCount > 0) {
            true
          } else if (stats.useCount == 0) {
            map -= wrapper
            false
          } else {
            // Should not happen
            throw new IllegalStateException(
              s"Cannot update expression: $expr in map: $map with use count: $useCount")
          }
        case _ =>
          if (useCount > 0) {
            map.put(wrapper, ExpressionStats(expr)(useCount))
          } else {
            // Should not happen
            throw new IllegalStateException(
              s"Cannot update expression: $expr in map: $map with use count: $useCount")
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
   */
  private def updateCommonExprs(
      exprs: Seq[Expression],
      map: mutable.HashMap[ExpressionEquals, ExpressionStats],
      useCount: Int): Unit = {
    assert(exprs.length > 1)
    var localEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
    updateExprTree(exprs.head, localEquivalenceMap)

    exprs.tail.foreach { expr =>
      val otherLocalEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
      updateExprTree(expr, otherLocalEquivalenceMap)
      localEquivalenceMap = localEquivalenceMap.filter { case (key, _) =>
        otherLocalEquivalenceMap.contains(key)
      }
    }

    // Start with the highest expression, remove it from `localEquivalenceMap` and add it to `map`.
    // The remaining highest expression in `localEquivalenceMap` is also common expression so loop
    // until `localEquivalenceMap` is not empty.
    var statsOption = Some(localEquivalenceMap).filter(_.nonEmpty).map(_.maxBy(_._1.height)._2)
    while (statsOption.nonEmpty) {
      val stats = statsOption.get
      updateExprTree(stats.expr, localEquivalenceMap, -stats.useCount)
      updateExprTree(stats.expr, map, useCount)

      statsOption = Some(localEquivalenceMap).filter(_.nonEmpty).map(_.maxBy(_._1.height)._2)
    }
  }

  // There are some special expressions that we should not recurse into all of its children.
  //   1. CodegenFallback: it's children will not be used to generate code (call eval() instead)
  //   2. If: common subexpressions will always be evaluated at the beginning, but the true and
  //          false expressions in `If` may not get accessed, according to the predicate
  //          expression. We should only recurse into the predicate expression.
  //   3. CaseWhen: like `If`, the children of `CaseWhen` only get accessed in a certain
  //                condition. We should only recurse into the first condition expression as it
  //                will always get accessed.
  //   4. Coalesce: it's also a conditional expression, we should only recurse into the first
  //                children, because others may not get accessed.
  //   5. NaNvl: it's a conditional expression, we can only guarantee the left child can be always
  //             accessed. And if we hit the left child, the right will not be accessed.
  private def childrenToRecurse(expr: Expression): Seq[Expression] = expr match {
    case _: CodegenFallback => Nil
    case i: If => i.predicate :: Nil
    case c: CaseWhen => c.children.head :: Nil
    case c: Coalesce => c.children.head :: Nil
    case n: NaNvl => n.left :: Nil
    case other => other.children
  }

  // For some special expressions we cannot just recurse into all of its children, but we can
  // recursively add the common expressions shared between all of its children.
  private def commonChildrenToRecurse(expr: Expression): Seq[Seq[Expression]] = expr match {
    case _: CodegenFallback => Nil
    case i: If => Seq(Seq(i.trueValue, i.falseValue))
    case c: CaseWhen =>
      // We look at subexpressions in conditions and values of `CaseWhen` separately. It is
      // because a subexpression in conditions will be run no matter which condition is matched
      // if it is shared among conditions, but it doesn't need to be shared in values. Similarly,
      // a subexpression among values doesn't need to be in conditions because no matter which
      // condition is true, it will be evaluated.
      val conditions = if (c.branches.length > 1) {
        c.branches.map(_._1)
      } else {
        // If there is only one branch, the first condition is already covered by
        // `childrenToRecurse` and we should exclude it here.
        Nil
      }
      // For an expression to be in all branch values of a CaseWhen statement, it must also be in
      // the elseValue.
      val values = if (c.elseValue.nonEmpty) {
        c.branches.map(_._2) ++ c.elseValue
      } else {
        Nil
      }

      Seq(conditions, values)
    // If there is only one child, the first child is already covered by
    // `childrenToRecurse` and we should exclude it here.
    case c: Coalesce if c.children.length > 1 => Seq(c.children)
    case n: NaNvl => Seq(n.children)
    case _ => Nil
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   */
  def addExprTree(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats] = equivalenceMap): Unit = {
    updateExprTree(expr, map)
  }

  private def updateExprTree(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats] = equivalenceMap,
      useCount: Int = 1): Unit = {
    val skip = useCount == 0 ||
      expr.isInstanceOf[LeafExpression] ||
      // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
      // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
      expr.exists(_.isInstanceOf[LambdaVariable]) ||
      // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
      // can cause error like NPE.
      (expr.exists(_.isInstanceOf[PlanExpression[_]]) && Utils.isInRunningSparkTask)

    if (!skip && !updateExprInMap(expr, map, useCount)) {
      val uc = useCount.signum
      childrenToRecurse(expr).foreach(updateExprTree(_, map, uc))
      commonChildrenToRecurse(expr).filter(_.nonEmpty).foreach(updateCommonExprs(_, map, uc))
    }
  }

  /**
   * Returns the state of the given expression in the `equivalenceMap`. Returns None if there is no
   * equivalent expressions.
   */
  def getExprState(e: Expression): Option[ExpressionStats] = {
    equivalenceMap.get(ExpressionEquals(e))
  }

  // Exposed for testing.
  private[sql] def getAllExprStates(count: Int = 0): Seq[ExpressionStats] = {
    equivalenceMap.filter(_._2.useCount > count).toSeq.sortBy(_._1.height).map(_._2)
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
    equivalenceMap.values.filter(stats => all || stats.useCount > 1).foreach { stats =>
      sb.append("  ").append(s"${stats.expr}: useCount = ${stats.useCount}").append('\n')
    }
    sb.toString()
  }
}

/**
 * Wrapper around an Expression that provides semantic equality.
 */
case class ExpressionEquals(e: Expression) {
  private def getHeight(tree: Expression): Int = {
    tree.children.map(getHeight).reduceOption(_ max _).getOrElse(0) + 1
  }

  // This is used to do a fast pre-check for child-parent relationship. For example, expr1 can
  // only be a parent of expr2 if expr1.height is larger than expr2.height.
  lazy val height = getHeight(e)

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
case class ExpressionStats(expr: Expression)(var useCount: Int)
