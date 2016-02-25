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

import org.apache.spark.sql.catalyst.rules._

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
*   - [[EqualTo]] and [[EqualNullSafe]] are reordered by `hashCode`.
 *  - Other comparisons ([[GreaterThan]], [[LessThan]]) are reversed by `hashCode`.
 */
object Canonicalize extends RuleExecutor[Expression] {
  override protected def batches: Seq[Batch] =
    Batch(
      "Expression Canonicalization", FixedPoint(100),
      IgnoreNamesTypes,
      Reorder) :: Nil

  /** Remove names and nullability from types. */
  protected object IgnoreNamesTypes extends Rule[Expression] {
    override def apply(e: Expression): Expression = e transformUp {
      case a: AttributeReference =>
        AttributeReference("none", a.dataType.asNullable)(exprId = a.exprId)
    }
  }

  /** Collects adjacent commutative operations. */
  protected def gatherCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] = e match {
    case c if f.isDefinedAt(c) => f(c).flatMap(gatherCommutative(_, f))
    case other => other :: Nil
  }

  /** Orders a set of commutative operations by their hash code. */
  protected def orderCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] =
    gatherCommutative(e, f).sortBy(_.hashCode())

  /** Rearrange expressions that are commutative or associative. */
  protected object Reorder extends Rule[Expression] {
    override def apply(e: Expression): Expression = e transformUp {
      case a: Add => orderCommutative(a, { case Add(l, r) => Seq(l, r) }).reduce(Add)
      case m: Multiply => orderCommutative(m, { case Multiply(l, r) => Seq(l, r) }).reduce(Multiply)

      case EqualTo(l, r) if l.hashCode() > r.hashCode() => EqualTo(r, l)
      case EqualNullSafe(l, r) if l.hashCode() > r.hashCode() => EqualNullSafe(r, l)

      case GreaterThan(l, r) if l.hashCode() > r.hashCode() => LessThan(r, l)
      case LessThan(l, r) if l.hashCode() > r.hashCode() => GreaterThan(r, l)

      case GreaterThanOrEqual(l, r) if l.hashCode() > r.hashCode() => LessThanOrEqual(r, l)
      case LessThanOrEqual(l, r) if l.hashCode() > r.hashCode() => GreaterThanOrEqual(r, l)
    }
  }
}
