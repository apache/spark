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

/**
 * Reorders adjacent commutative operators such as [[And]] in the expression tree, according to
 * the `hashCode` of non-commutative nodes, to remove cosmetic variations. Caller side should only
 * call it on the root node of an expression tree that needs to be canonicalized.
 */
object Canonicalize {
  /** Collects adjacent commutative operations. */
  private def gatherCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] = e match {
    case c if f.isDefinedAt(c) => f(c).flatMap(gatherCommutative(_, f))
    case other => reorderOperators(other) :: Nil
  }

  /** Orders a set of commutative operations by their hash code. */
  private def orderCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] = {
    gatherCommutative(e, f).map(_.preCanonicalized).sortBy(_.hashCode())
  }

  private def orderBinaryComparison(
      bc: BinaryComparison,
      reverse: (Expression, Expression) => BinaryComparison) = {
    val l = reorderOperators(bc.left).preCanonicalized
    val r = reorderOperators(bc.right).preCanonicalized
    if (l.hashCode() > r.hashCode()) {
      reverse(r, l)
    } else {
      bc.withNewChildren(Seq(l, r))
    }
  }

  def reorderOperators(e: Expression): Expression = e match {
    // TODO: do not reorder consecutive `Add`s or `Multiply`s with different `failOnError` flags
    case a @ Add(_, _, f) =>
      orderCommutative(a, { case Add(l, r, _) => Seq(l, r) }).reduce(Add(_, _, f))
    case m @ Multiply(_, _, f) =>
      orderCommutative(m, { case Multiply(l, r, _) => Seq(l, r) }).reduce(Multiply(_, _, f))

    case o @ Or(l, r) if l.deterministic && r.deterministic =>
      orderCommutative(o, { case Or(l, r) if l.deterministic && r.deterministic => Seq(l, r) })
        .reduce(Or)
    case a @ And(l, r) if l.deterministic && r.deterministic =>
      orderCommutative(a, { case And(l, r) if l.deterministic && r.deterministic => Seq(l, r)})
        .reduce(And)

    case o: BitwiseOr =>
      orderCommutative(o, { case BitwiseOr(l, r) => Seq(l, r) }).reduce(BitwiseOr)
    case a: BitwiseAnd =>
      orderCommutative(a, { case BitwiseAnd(l, r) => Seq(l, r) }).reduce(BitwiseAnd)
    case x: BitwiseXor =>
      orderCommutative(x, { case BitwiseXor(l, r) => Seq(l, r) }).reduce(BitwiseXor)

    case g: Greatest =>
      val newChildren = orderCommutative(g, { case Greatest(children) => children })
      Greatest(newChildren)
    case l: Least =>
      val newChildren = orderCommutative(l, { case Least(children) => children })
      Least(newChildren)

    case bc: EqualTo => orderBinaryComparison(bc, EqualTo)
    case bc: EqualNullSafe => orderBinaryComparison(bc, EqualNullSafe)

    case bc: GreaterThan => orderBinaryComparison(bc, LessThan)
    case bc: LessThan => orderBinaryComparison(bc, GreaterThan)

    case bc: GreaterThanOrEqual => orderBinaryComparison(bc, LessThanOrEqual)
    case bc: LessThanOrEqual => orderBinaryComparison(bc, GreaterThanOrEqual)

    case _ => e.mapChildren(reorderOperators).preCanonicalized
  }
}
