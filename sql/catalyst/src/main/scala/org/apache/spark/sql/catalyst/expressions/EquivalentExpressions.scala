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

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable

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
    addExprToMap(expr, equivalenceMap)
  }

  private def addExprToMap(
      expr: Expression, map: mutable.HashMap[ExpressionEquals, ExpressionStats]): Boolean = {
    if (expr.deterministic) {
      val wrapper = ExpressionEquals(expr)
      map.get(wrapper) match {
        case Some(stats) =>
          stats.useCount += 1
          true
        case _ =>
          map.put(wrapper, ExpressionStats(expr)())
          false
      }
    } else {
      false
    }
  }

  /**
   * Adds only expressions which are common in each of given expressions, in a recursive way.
   * For example, given two expressions `(a + (b + (c + 1)))` and `(d + (e + (c + 1)))`,
   * the common expression `(c + 1)` will be added into `equivalenceMap`.
   *
   * Note that as we don't know in advance if any child node of an expression will be common
   * across all given expressions, we count all child nodes when looking through the given
   * expressions. But when we call `addExprTree` to add common expressions into the map, we
   * will add recursively the child nodes. So we need to filter the child expressions first.
   * For example, if `((a + b) + c)` and `(a + b)` are common expressions, we only add
   * `((a + b) + c)`.
   */
  private def addCommonExprs(
      exprs: Seq[Expression],
      map: mutable.HashMap[ExpressionEquals, ExpressionStats]): Unit = {
    assert(exprs.length > 1)
    var localEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
    addExprTree(exprs.head, localEquivalenceMap)

    exprs.tail.foreach { expr =>
      val otherLocalEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
      addExprTree(expr, otherLocalEquivalenceMap)
      localEquivalenceMap = localEquivalenceMap.filter { case (key, _) =>
        otherLocalEquivalenceMap.contains(key)
      }
    }

    localEquivalenceMap.foreach { case (commonExpr, state) =>
      val possibleParents = localEquivalenceMap.filter { case (_, v) => v.height > state.height }
      val notChild = possibleParents.forall { case (k, _) =>
        k == commonExpr || k.e.find(_.semanticEquals(commonExpr.e)).isEmpty
      }
      if (notChild) {
        // If the `commonExpr` already appears in the equivalence map, calling `addExprTree` will
        // increase the `useCount` and mark it as a common subexpression. Otherwise, `addExprTree`
        // will recursively add `commonExpr` and its descendant to the equivalence map, in case
        // they also appear in other places. For example, `If(a + b > 1, a + b + c, a + b + c)`,
        // `a + b` also appears in the condition and should be treated as common subexpression.
        addExprTree(commonExpr.e, map)
      }
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
  private def childrenToRecurse(expr: Expression): Seq[Expression] = expr match {
    case _: CodegenFallback => Nil
    case i: If => i.predicate :: Nil
    case c: CaseWhen => c.children.head :: Nil
    case c: Coalesce => c.children.head :: Nil
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
    case _ => Nil
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   */
  def addExprTree(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats] = equivalenceMap): Unit = {
    val skip = expr.isInstanceOf[LeafExpression] ||
      // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
      // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
      expr.find(_.isInstanceOf[LambdaVariable]).isDefined ||
      // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
      // can cause error like NPE.
      (expr.isInstanceOf[PlanExpression[_]] && TaskContext.get != null)

    if (!skip && !addExprToMap(expr, map)) {
      childrenToRecurse(expr).foreach(addExprTree(_, map))
      commonChildrenToRecurse(expr).filter(_.nonEmpty).foreach(addCommonExprs(_, map))
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
    equivalenceMap.values.filter(_.useCount > count).toSeq.sortBy(_.height)
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
  override def equals(o: Any): Boolean = o match {
    case other: ExpressionEquals => e.semanticEquals(other.e)
    case _ => false
  }

  override def hashCode: Int = e.semanticHash()
}

/**
 * A wrapper in place of using Seq[Expression] to record a group of equivalent expressions.
 *
 * This saves a lot of memory when there are a lot of expressions in a same equivalence group.
 * Instead of appending to a mutable list/buffer of Expressions, just update the "flattened"
 * useCount in this wrapper in-place.
 */
case class ExpressionStats(expr: Expression)(var useCount: Int = 1) {
  // This is used to do a fast pre-check for child-parent relationship. For example, expr1 can
  // only be a parent of expr2 if expr1.height is larger than expr2.height.
  lazy val height = getHeight(expr)

  private def getHeight(tree: Expression): Int = {
    tree.children.map(getHeight).reduceOption(_ max _).getOrElse(0) + 1
  }
}
