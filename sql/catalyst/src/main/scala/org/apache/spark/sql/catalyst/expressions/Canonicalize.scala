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
  private def ignoreNamesTypes(e: Expression): Expression = e match {
    case a: AttributeReference =>
      AttributeReference("none", a.dataType.asNullable)(exprId = a.exprId)
    case _ => e
  }

  /** Collects adjacent commutative operations. */
  private def gatherCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] = e match {
    case c if f.isDefinedAt(c) => f(c).flatMap(gatherCommutative(_, f))
    case other => other :: Nil
  }

  /** Orders a set of commutative operations by their hash code. */
  private def orderCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] =
    gatherCommutative(e, f).sortBy(_.hashCode())

  /** Rearrange expressions that are commutative or associative. */
  private def expressionReorder(e: Expression): Expression = e match {
    case a: Add => orderCommutative(a, { case Add(a, b) => Seq(a, b) }).reduce(Add)
    case m: Multiply => orderCommutative(m, { case Multiply(a, b) => Seq(a, b) }).reduce(Multiply)

    case EqualTo(a, b) if a.hashCode() > b.hashCode() => EqualTo(b, a)
    case EqualNullSafe(a, b) if a.hashCode() > b.hashCode() => EqualNullSafe(b, a)

    case GreaterThan(a, b) if a.hashCode() > b.hashCode() => LessThan(b, a)
    case LessThan(a, b) if a.hashCode() > b.hashCode() => GreaterThan(b, a)

    case GreaterThanOrEqual(a, b) if a.hashCode() > b.hashCode() => LessThanOrEqual(b, a)
    case LessThanOrEqual(a, b) if a.hashCode() > b.hashCode() => GreaterThanOrEqual(b, a)

    case Not(GreaterThan(a, b)) if a.hashCode() > b.hashCode() => GreaterThan(b, a)
    case Not(GreaterThan(a, b)) => LessThanOrEqual(a, b)
    case Not(LessThan(a, b)) if a.hashCode() > b.hashCode() => LessThan(b, a)
    case Not(LessThan(a, b)) => GreaterThanOrEqual(a, b)
    case Not(GreaterThanOrEqual(a, b)) if a.hashCode() > b.hashCode() => GreaterThanOrEqual(b, a)
    case Not(GreaterThanOrEqual(a, b)) => LessThan(a, b)
    case Not(LessThanOrEqual(a, b)) if a.hashCode() > b.hashCode() => LessThanOrEqual(b, a)
    case Not(LessThanOrEqual(a, b)) => GreaterThan(a, b)

    case _ => e
  }
}
