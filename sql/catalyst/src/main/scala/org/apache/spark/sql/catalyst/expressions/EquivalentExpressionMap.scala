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

import org.apache.spark.sql.catalyst.expressions.EquivalentExpressionMap.SemanticallyEqualExpr

/**
 * ﻿A class that allows you to map an expression into a set of equivalent expressions. The keys are
 * handled based on their semantic meaning and ignoring cosmetic differences. The values are
 * represented as [[ExpressionSet]]s.
 *
 * The underlying representation of keys depends on the [[Expression.semanticHash]] and
 * [[Expression.semanticEquals]] methods.
 *
 * {{{
 *   val map = new EquivalentExpressionMap()
 *
 *   map.put(1 + 2, a)
 *   map.put(rand(), b)
 *
 *   map.get(2 + 1) => Set(a) // 1 + 2 and 2 + 1 are semantically equivalent
 *   map.get(1 + 2) => Set(a) // 1 + 2 and 2 + 1 are semantically equivalent
 *   map.get(rand()) => Set() // non-deterministic expressions are not equivalent
 * }}}
 */
class EquivalentExpressionMap {

  private val equivalenceMap = mutable.HashMap.empty[SemanticallyEqualExpr, ExpressionSet]

  def put(expression: Expression, equivalentExpression: Expression): Unit = {
    val equivalentExpressions = equivalenceMap.getOrElseUpdate(expression, ExpressionSet.empty)
    equivalenceMap(expression) = equivalentExpressions + equivalentExpression
  }

  def get(expression: Expression): Set[Expression] =
    equivalenceMap.getOrElse(expression, ExpressionSet.empty)
}

object EquivalentExpressionMap {

  private implicit class SemanticallyEqualExpr(val expr: Expression) {
    override def equals(obj: Any): Boolean = obj match {
      case other: SemanticallyEqualExpr => expr.semanticEquals(other.expr)
      case _ => false
    }

    override def hashCode: Int = expr.semanticHash()
  }
}
