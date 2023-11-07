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
import org.apache.spark.sql.catalyst.trees.TreePattern.{LAMBDA_VARIABLE, PLAN_EXPRESSION}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 *
 * Please note that `EquivalentExpressions` is mainly used in subexpression elimination where common
 * non-leaf expression subtrees are calculated, but there there is one special use case in
 * `PhysicalAggregation` where `EquivalentExpressions` is used as a mutable set of deterministic
 * expressions. For that special use case we have the `allowLeafExpressions` config.
 */
class EquivalentExpressions(
    skipForShortcutEnable: Boolean = SQLConf.get.subexpressionEliminationSkipForShotcutExpr,
    minConditionalCount: Option[Double] =
      Some(SQLConf.get.subexpressionEliminationMinExpectedConditionalEvaluationCount)
        .filter(_ >= 0d),
    allowLeafExpressions: Boolean = false) {

  // The subexpressions are stored by height in separate maps to speed up certain calculations.
  private val maps = mutable.ArrayBuffer[mutable.Map[ExpressionEquals, ExpressionStats]]()

  // `EquivalentExpressions` has 2 states internally, it can be either inflated or not.
  // The inflated state means that all added expressions have been traversed recursively and their
  // subexpressions are also added to `maps`. The idea behind these 2 states is that when an
  // expression tree is added we don't need to traverse/record its subexpressions immediately.
  // The typical use case of this data structure is that multiple expression trees are added and
  // then we want to see the common subexpressions. It might be the case that the same expression
  // trees or partly overlapping expressions trees are added multiple times. With this approach we
  // just need to record how many times an expression tree is explicitly added when later when
  // `getExprState()` or `getCommonSubexpressions()` is called we inflate the data structure (do the
  // recursive traversal and record the subexpressions in `inflate()`) if needed.
  private var inflated: Boolean = true

  /**
   * Adds each expression to this data structure and returns true if there was already a matching
   * expression.
   */
  def addExpr(expr: Expression): Boolean = {
    if (supportedExpression(expr) && expr.deterministic) {
      updateWithExpr(expr, 1, 0d)
    } else {
      false
    }
  }

  /**
   * Adds the expression to this data structure, including its children recursively.
   */
  def addExprTree(expr: Expression): Unit = {
    if (supportedExpression(expr)) {
      updateWithExpr(expr, 1, 0d)
    }
  }

  private def supportedExpression(e: Expression): Boolean = {
    // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
    // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
    !(e.containsPattern(LAMBDA_VARIABLE) ||
      // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
      // can cause error like NPE.
      (e.containsPattern(PLAN_EXPRESSION) && Utils.isInRunningSparkTask))
  }

  private def updateWithExpr(
      expr: Expression,
      evalCount: Int,
      condEvalCount: Double): Boolean = {
    require(evalCount >= 0 && condEvalCount >= 0d)

    inflated = false
    val map = getMapByHeight(expr.height)
    val wrapper = ExpressionEquals(expr)
    map.get(wrapper) match {
      case Some(es) =>
        es.directEvalCount += evalCount
        es.directCondEvalCount += condEvalCount
        true
      case _ =>
        map(wrapper) = ExpressionStats(expr)(evalCount, condEvalCount, 0, 0d)
        false
    }
  }

  private def getMapByHeight(height: Int) = {
    val index = height - 1
    while (maps.size <= index) {
      maps += mutable.Map.empty
    }
    maps(index)
  }

  // Iterate expressions from parents to children and fill `transientEvalCount`s and
  // `transientCondEvalCount`s from explicitly added `directEvalCount`s and `directCondEvalCount`s.
  private def inflate() = {
    if (!inflated) {
      maps.reverse.foreach { map =>
        map.foreach {
          case (_, es) => inflateExprState(es)
          case _ =>
        }
      }
      inflated = true
    }
  }

  private def inflateExprState(exprStats: ExpressionStats): Unit = {
    val expr = exprStats.expr
    if (!expr.isInstanceOf[LeafExpression] || allowLeafExpressions) {
      val evalCount = exprStats.directEvalCount
      val condEvalCount = exprStats.directCondEvalCount

      exprStats.directEvalCount = 0
      exprStats.directCondEvalCount = 0d
      exprStats.transientEvalCount += evalCount
      exprStats.transientCondEvalCount += condEvalCount

      expr match {
        // CodegenFallback's children will not be used to generate code (call eval() instead)
        case _: CodegenFallback =>

        case c: CaseWhen =>
          // Let's consider `CaseWhen(Seq((w1, t1), (w2, t2), (w3, t3), ... (wn, tn)), Some(e))`
          // example and use `Wn`, `Tn` and `E` notations for the local equivalence maps built from
          // `wn`, `tn` and `e` expressions respectively.
          //
          // Let's try to build a local equivalence map of the above `CaseWhen` example and then add
          // that local map to `map`.
          //
          // We know that `w1` is surely evaluated so `W1` should be part of the local map.
          // We also know that based on the result of `w1` either `t1` or `w2` is evaluated so the
          // "intersection" between `T1` and `W2` should be also part of the local map.
          // Please note that "intersection" might not describe well the operation that we need
          // between `T1` and `W2`. It is an intersection in terms of surely evaluated
          // subexpressions between `T1` and `W2` but it is also kind of an union between
          // conditionally evaluated subexpressions. See the details in `intersectWith()`.
          // So the local map can be calculated as `W1 | (T1 & W2)` so far, where `|` and `&` mean
          // the "union" and "intersection" of equivalence maps.
          // But we can continue the previous logic further because if `w2` is evaluated, then based
          // on the result of `w2` either `t2` or `w3` is also evaluated.
          // So eventually the local equivalence map can be calculated as
          // `W1 | (T1 & (W2 | (T2 & (W3 | (T3 & ... & (Wn | (Tn & E)))))))`.

          // As `w1` is always evaluated so we can add it immediately to `map` (instead of adding it
          // to `localMap`).
          updateWithExpr(c.branches.head._1, evalCount, condEvalCount)

          val localMap = new EquivalentExpressions
          if (c.elseValue.isDefined) {
            localMap.updateWithExpr(c.branches.last._2, evalCount, condEvalCount)
            localMap.intersectWithExpr(c.elseValue.get, evalCount, condEvalCount)
          } else {
            localMap.updateWithExpr(c.branches.last._2, 0, (evalCount + condEvalCount) / 2)
          }
          if (c.branches.length > 1) {
            c.branches.reverse.sliding(2).foreach { case Seq((w, _), (_, prevt)) =>
              localMap.updateWithExpr(w, evalCount, condEvalCount)
              localMap.intersectWithExpr(prevt, evalCount, condEvalCount)
            }
          }

          unionWith(localMap)

        case i: If =>
          updateWithExpr(i.predicate, evalCount, condEvalCount)

          val localMap = new EquivalentExpressions
          localMap.updateWithExpr(i.trueValue, evalCount, condEvalCount)
          localMap.intersectWithExpr(i.falseValue, evalCount, condEvalCount)

          unionWith(localMap)

        case a: And if skipForShortcutEnable =>
          updateWithExpr(a.left, evalCount, condEvalCount)
          updateWithExpr(a.right, 0, (evalCount + condEvalCount) / 2)

        case o: Or if skipForShortcutEnable =>
          updateWithExpr(o.left, evalCount, condEvalCount)
          updateWithExpr(o.right, 0, (evalCount + condEvalCount) / 2)

        case n: NaNvl =>
          updateWithExpr(n.left, evalCount, condEvalCount)
          updateWithExpr(n.right, 0, (evalCount + condEvalCount) / 2)

        case c: Coalesce =>
          updateWithExpr(c.children.head, evalCount, condEvalCount)
          var cec = evalCount + condEvalCount
          c.children.tail.foreach {
            cec /= 2
            updateWithExpr(_, 0, cec)
          }

        case e => e.children.foreach(updateWithExpr(_, evalCount, condEvalCount))
      }
    }
  }

  private def intersectWithExpr(
      expr: Expression,
      evalCount: Int,
      condEvalCount: Double) = {
    val localMap = new EquivalentExpressions
    localMap.updateWithExpr(expr, evalCount, condEvalCount)
    intersectWith(localMap)
  }

  /**
   * This method can be used to compute the equivalence map if there is a branching in expression
   * evaluation.
   * E.g. if we have `If(_, a, b)` expression and `A` and `B` are the equivalence maps built from
   * `a` and `b` this method computes the equivalence map `C` in which the keys are the superset of
   * expressions from both `A` and `B`. The `transientEvalCount` statistics of expressions in `C`
   * depends on whether the expression was present in both `A` and `B` or not.
   * If an expression was present in both then the result `transientEvalCount` of the expression is
   * the minimum of `transientEvalCount`s from `A` and `B` (intersection of equivalence maps).
   * For the sake of simplicity branching is modelled with 0.5 / 0.5 probabilities so the
   * `condEvalCount` statistics of expressions in `C` are calculated by adjusting both
   * `condEvalCount` from `A` and `B` by `0.5` and summing them. Also, difference between
   * `transientEvalCount` of an expression from `A` and `B` becomes part of `condEvalCount` and so
   * adjusted by `0.5`.
   *
   * Please note that this method modifies `this` and `other` is no longer safe to use after this
   * method.
   */
  private def intersectWith(other: EquivalentExpressions) = {
    inflate()
    other.inflate()

    val zippedMaps = maps.zip(other.maps)
    zippedMaps.foreach { case (map, otherMap) =>
      map.foreach { case (key, value) =>
        otherMap.remove(key) match {
          case Some(otherValue) =>
            val (min, max) = if (value.transientEvalCount < otherValue.transientEvalCount) {
              (value.transientEvalCount, otherValue.transientEvalCount)
            } else {
              (otherValue.transientEvalCount, value.transientEvalCount)
            }
            value.transientCondEvalCount += otherValue.transientCondEvalCount + max - min
            value.transientEvalCount = min
          case _ =>
            value.transientCondEvalCount += value.transientEvalCount
            value.transientEvalCount = 0
        }
        value.transientCondEvalCount /= 2
      }
      otherMap.foreach { case e @ (_, value) =>
        value.transientCondEvalCount = (value.transientCondEvalCount + value.transientEvalCount) / 2
        value.transientEvalCount = 0
        map += e
      }
    }
    maps ++= other.maps.drop(maps.size)
    maps.drop(zippedMaps.size).foreach { map =>
      map.foreach { case (_, value) =>
        value.transientCondEvalCount = (value.transientCondEvalCount + value.transientEvalCount) / 2
        value.transientEvalCount = 0
      }
    }
  }

  /**
   * This method adds the content of `other` to `this`. It is very similar to `updateWithExprTree()`
   * in terms that it adds expressions to an equivalence map, but `other` might contain more than
   * one expressions.
   *
   * Please note that this method modifies `this` and `other` is no longer safe to use after this
   * method.
   */
  private def unionWith(other: EquivalentExpressions) = {
    maps.zip(other.maps).foreach { case (map, otherMap) =>
      otherMap.foreach { case e @ (key, otherValue) =>
        map.get(key) match {
          case Some(value) =>
            value.directEvalCount += otherValue.directEvalCount
            value.transientEvalCount += otherValue.transientEvalCount
            value.directCondEvalCount += otherValue.directCondEvalCount
            value.transientCondEvalCount += otherValue.transientCondEvalCount
          case _ =>
            map += e
        }
      }
    }
    maps ++= other.maps.drop(maps.size)
  }

  /**
   * Returns the statistics of the given expression.
   */
  def getExprState(expr: Expression): Option[ExpressionStats] = {
    if (supportedExpression(expr) && expr.deterministic) {
      inflate()

      val map = getMapByHeight(expr.height)
      map.get(ExpressionEquals(expr))
    } else {
      None
    }
  }

  // Exposed for testing.
  private[sql] def getAllExprStates(
      evalCount: Int = 0,
      condEvalCount: Option[Double] = None,
      increasingOrder: Boolean = true) = {
    inflate()

    (if (increasingOrder) maps else maps.reverse).flatMap { map =>
      map.collect {
        case (_, es) if es.expr.deterministic && (
            es.transientEvalCount > evalCount ||
            es.transientEvalCount == evalCount &&
              condEvalCount.exists(es.transientCondEvalCount > _)) => es
      }
    }
  }

  /**
   * Returns a sequence of expressions that are:
   * - surely evaluated more than once
   * - or surely evaluated only once but their expected conditional evaluation count satisfies the
   *   `spark.sql.subexpressionElimination.minExpectedConditionalEvaluationCount`
   * requirements and it also makes sense to extract the expression as common subexpression.
   *
   * E.g. in case of `1 * 2 + 1 * 2 * 3 + 1 * 2 * 3 * 4` the equivalence map of the expression
   * looks like this:
   * (1 * 2) -> (3 + 0.0)
   * ((1 * 2) * 3) -> (2 + 0.0)
   * (((1 * 2) * 3) * 4) -> (1 + 0.0)
   * ((1 * 2) + ((1 * 2) * 3)) -> (1 + 0.0)
   * (((1 * 2) + ((1 * 2) * 3)) + (((1 * 2) * 3) * 4)) -> (1 + 0.0)
   * and we want to include both `(1 * 2)` and `((1 * 2) * 3)` in the result.
   *
   * But it is also important that if a child and its parent expression have the same statistics it
   * makes no sense to include the child in the common subexpressions.
   * E.g. in case of `1 * 2 * 3 + 1 * 2 * 3` the equivalence map is:
   * (1 * 2) -> (2 + 0.0)
   * ((1 * 2) * 3) -> (2 + 0.0)
   * (((1 * 2) * 3) + ((1 * 2) * 3)) -> (1 + 0.0)
   * and we want to include only `((1 * 2) * 3)` in the result.
   *
   * The returned sequence of expressions are ordered by their height in increasing order.
   */
  def getCommonSubexpressions: Seq[Expression] = {
    inflate()

    // We use the fact that a child's `transientEvalCount` + `transientCondEvalCount` (total
    // expected evaluation count) is always >= than any of its parent's and if it is > then it make
    // sense to include the child in the result.
    // (Also note that a child's `transientEvalCount` is always >= than any of its parent's.)
    //
    // So start iterating on expressions that satisfy the requirements from higher to lower (parents
    // to children) and record `transientEvalCount` + `transientCondEvalCount` to all its children
    // that don't have a record yet. An expression can be included in the result if there is no
    // recorded value for it or the expression's `transientEvalCount` + `transientCondEvalCount` is
    // > than the recorded value.
    val m = mutable.Map.empty[ExpressionEquals, Double]
    getAllExprStates(1, minConditionalCount, false).filter { es =>
      val wrapper = ExpressionEquals(es.expr)
      val sumEvalCount = es.transientEvalCount + es.transientCondEvalCount
      es.expr.children.map(ExpressionEquals(_)).toSet.foreach { childWrapper: ExpressionEquals =>
        if (!m.contains(childWrapper)) {
          m(childWrapper) = sumEvalCount
        }
      }
      sumEvalCount > m.getOrElse(wrapper, 0d)
    }.map(_.expr).reverse.toSeq
  }

  /**
   * Returns the state of the data structure as a string.
   */
  def debugString(): String = {
    val sb = new java.lang.StringBuilder()
    sb.append("Equivalent expressions:\n")
    getAllExprStates(0, Some(0d)).foreach { es =>
      sb.append(s"  $es\n")
    }
    sb.toString()
  }
}

/**
 * Wrapper around an Expression that provides semantic equality.
 */
case class ExpressionEquals(e: Expression) {
  override def equals(o: Any): Boolean = o match {
    case other: ExpressionEquals =>
      e.canonicalized == other.e.canonicalized && e.height == other.e.height
    case _ => false
  }

  override def hashCode: Int = Objects.hash(e.semanticHash(): Integer, e.height: Integer)
}

/**
 * This class stores the expected evaluation count of expressions split into `directEvalCount` +
 * `transientEvalCount` that records sure evaluations and `directCondEvalCount` +
 * `transientCondEvalCount` that records conditional evaluations. The `transient...` fields are
 * filled up during `inflate()`.
 *
 * Here are a few example expressions and the statistics of a non-leaf `c` subexpression from the
 * equivalence maps built from the expressions:
 * `c`               => `c -> (1 + 0.0)`
 * `c + c`           => `c -> (2 + 0.0)`
 * `If(_, c, _)`     => `c -> (0 + 0.5)`
 * `If(_, c + c, _)` => `c -> (0 + 1.0)`
 * `If(_, c, c)`     => `c -> (1 + 0.0)`
 * `If(c, c, _)`     => `c -> (1 + 0.5)`
 */
case class ExpressionStats(expr: Expression)(
    var directEvalCount: Int,
    var directCondEvalCount: Double,
    var transientEvalCount: Int,
    var transientCondEvalCount: Double) {
  override def toString: String =
    s"$expr -> (${directEvalCount + transientEvalCount} + " +
      s"${directCondEvalCount + transientCondEvalCount})"
}
