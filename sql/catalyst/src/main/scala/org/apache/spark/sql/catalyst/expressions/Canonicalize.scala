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

import org.apache.spark.sql.types.DataType

/**
 * Rewrites an expression using rules that are guaranteed preserve the result while attempting
 * to remove cosmetic variations. Deterministic expressions that are `equal` after canonicalization
 * will always return the same answer given the same input (i.e. false positives should not be
 * possible). However, it is possible that two canonical expressions that are not equal will in fact
 * return the same answer given any input (i.e. false negatives are possible).
 *
 * The following rules are applied:
 *  - Names and nullability hints for [[org.apache.spark.sql.types.DataType]]s are stripped.
 *  - Commutative and associative operations ([[Add]] and [[Multiply]]) have their children ordered
 *    by `hashCode`.
 *  - [[EqualTo]] and [[EqualNullSafe]] are reordered by `hashCode`.
 *  - Other comparisons ([[GreaterThan]], [[LessThan]]) are reversed by `hashCode`.
 */
object Canonicalize extends {
  def execute(e: Expression): Expression = {
    expressionReorder(ignoreNamesTypes(e))
  }

  /** Remove names and nullability from types. */
  private[expressions] def ignoreNamesTypes(e: Expression): Expression = e match {
    case a: AttributeReference =>
      AttributeReference("none", a.dataType.asNullable)(exprId = a.exprId)
    case _ => e
  }

  /** Collects adjacent operations. The operations can be non commutative. */
  private def gatherAdjacent(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] = e match {
    case c if f.isDefinedAt(c) => f(c).flatMap(gatherAdjacent(_, f))
    case other => other :: Nil
  }

  /** Orders a set of commutative operations by their hash code. */
  private def orderCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] =
    gatherAdjacent(e, f).sortBy(_.hashCode())

  /** Rearrange expressions that are commutative or associative or semantically equal. */
  private def expressionReorder(e: Expression): Expression = e match {
    case UnaryMinus(UnaryMinus(c)) => c

    // If the expression is composed of `Add` and `Subtract`, we rearrange it by extracting all
    // sub-expressions like:
    // a + b => a, b
    // a - b => a, -b
    // -(a + b) => -a, -b
    // -(a - b) => -a, b
    // Then we concatenate those sub-expressions by:
    // 1. Remove the pairs of sub-expressions like (b, -b).
    // 2. Concatenate remainning sub-expressions with `Add`.
    case a: Add =>
      // Extract sub-expressions.
      val (positiveExprs, negativeExprs) = gatherAdjacent(a, {
        case Add(l, r) => Seq(l, r)
        case Subtract(l, r) => Seq(l, UnaryMinus(r))
        case UnaryMinus(Add(l, r)) => Seq(UnaryMinus(l), UnaryMinus(r))
        case UnaryMinus(Subtract(l, r)) => Seq(UnaryMinus(l), r)
      }).map { e =>
        e.transform { case UnaryMinus(UnaryMinus(c)) => c }
      }.filter {
        case Literal(0, _) => false
        case UnaryMinus(Literal(0, _)) => false
        case _ => true
      }.partition(!_.isInstanceOf[UnaryMinus])

      // Remove the pairs of sub-expressions like (b, -b).
      val (newLeftExprs, newRightExprs) = filterOutExprs(positiveExprs,
        negativeExprs.map(_.asInstanceOf[UnaryMinus].child))

      val finalExprs = (newLeftExprs ++ newRightExprs.map(UnaryMinus(_))).sortBy(_.hashCode())
      if (finalExprs.isEmpty) {
        Literal(0, a.dataType)
      } else {
        finalExprs.reduce(Add)
      }

    case Subtract(sl, sr) => expressionReorder(Add(sl, UnaryMinus(sr)))

    // If the expression is composed of `Multiply` and `Divide`, we rearrange it by extracting all
    // sub-expressions like:
    // a * b => a, b
    // a / b => a, 1 / b
    // 1 / (a * b) => 1 / a, 1 / b
    // 1 / (a / b) => 1 / a, b
    // Then we concatenate those sub-expressions by:
    // 1. Remove the pairs of sub-expressions like (b, 1 / b).
    // 2. Concatenate remainning sub-expressions with `Multiply` and `Divide`.
    case m: Multiply =>
      // Extract sub-expressions.
      val (multiplyExprs, reciprocalExprs) = gatherAdjacent(m, {
        case Multiply(l, r) => Seq(l, r)
        case Divide(l, r) => Seq(l, UnaryReciprocal(r))
        case UnaryReciprocal(Multiply(l, r)) => Seq(UnaryReciprocal(l), UnaryReciprocal(r))
        case UnaryReciprocal(Divide(l, r)) => Seq(UnaryReciprocal(l), r)
      }).map { e =>
        e.transform { case UnaryReciprocal(UnaryReciprocal(c)) => c }
      }.filter {
        case Literal(1, _) => false
        case UnaryReciprocal(Literal(1, _)) => false
        case _ => true
      }.partition(!_.isInstanceOf[UnaryReciprocal])

      // Remove the pairs of sub-expressions like (b, 1 / b).
      val (newLeftExprs, newRightExprs) = filterOutExprs(multiplyExprs,
        reciprocalExprs.map(_.asInstanceOf[UnaryReciprocal].child))

      val finalExprs = (newLeftExprs ++ newRightExprs.map(UnaryReciprocal(_))).sortBy(_.hashCode())
      if (finalExprs.isEmpty) {
        Literal(1, m.dataType)
      } else {
        finalExprs.map {
          case u: UnaryReciprocal => Divide(Literal(1, u.dataType), u.child)
          case other => other
        }.reduce(Multiply)
      }

    case Divide(dl, dr) => expressionReorder(Multiply(dl, UnaryReciprocal(dr)))

    case o: Or =>
      orderCommutative(o, { case Or(l, r) if l.deterministic && r.deterministic => Seq(l, r) })
        .reduce(Or)
    case a: And =>
      orderCommutative(a, { case And(l, r) if l.deterministic && r.deterministic => Seq(l, r)})
        .reduce(And)

    case EqualTo(l, r) if l.hashCode() > r.hashCode() => EqualTo(r, l)
    case EqualNullSafe(l, r) if l.hashCode() > r.hashCode() => EqualNullSafe(r, l)

    case GreaterThan(l, r) if l.hashCode() > r.hashCode() => LessThan(r, l)
    case LessThan(l, r) if l.hashCode() > r.hashCode() => GreaterThan(r, l)

    case GreaterThanOrEqual(l, r) if l.hashCode() > r.hashCode() => LessThanOrEqual(r, l)
    case LessThanOrEqual(l, r) if l.hashCode() > r.hashCode() => GreaterThanOrEqual(r, l)

    // Note in the following `NOT` cases, `l.hashCode() <= r.hashCode()` holds. The reason is that
    // canonicalization is conducted bottom-up -- see [[Expression.canonicalized]].
    case Not(GreaterThan(l, r)) => LessThanOrEqual(l, r)
    case Not(LessThan(l, r)) => GreaterThanOrEqual(l, r)
    case Not(GreaterThanOrEqual(l, r)) => LessThan(l, r)
    case Not(LessThanOrEqual(l, r)) => GreaterThan(l, r)

    case _ => e
  }

  /** Finds the expressions existing in both set of expressions and drops them from two set. */
  private def filterOutExprs(
      leftExprs: Seq[Expression],
      rightExprs: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    var newLeftExprs = leftExprs
    val foundIndexes = rightExprs.zipWithIndex.map { case (r, rIndex) =>
      val found = newLeftExprs.indexWhere(_.semanticEquals(r))
      if (found >= 0) {
        newLeftExprs = newLeftExprs.slice(0, found) ++
          newLeftExprs.slice(found + 1, newLeftExprs.length)
      }
      (found, rIndex)
    }
    val dropRightIndexes = foundIndexes.filter(_._1 >= 0).unzip._2
    val newRightExprs = rightExprs.zipWithIndex.filterNot { case (r, index) =>
      dropRightIndexes.contains(index)
    }.unzip._1
    (newLeftExprs, newRightExprs)
  }
}

/** A private [[UnaryExpression]] only used in expression canonicalization. */
private[expressions] case class UnaryReciprocal(child: Expression)
    extends UnaryExpression with Unevaluable {
  override def dataType: DataType = child.dataType
}
