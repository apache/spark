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

private[expressions] object ExpressionEquals {
  def normalize(expr: Expression): Expression = expr.transformUp {
    case n: AttributeReference =>
      // We don't care about the name of AttributeReference in its semantic equality check
      new AttributeReference(null, n.dataType, n.nullable, n.metadata)(n.exprId, n.qualifiers)
  }
}

object ExpressionSet {
  def apply(exprs: Iterable[Expression]): ExpressionSet = {
    val set = new ExpressionSet()
    exprs.foreach(e => set.add(e))

    set
  }
}

/**
 * Builds a Expression Set that used to be looked up even when the attributes used
 * differ cosmetically (i.e., the capitalization of the name, or the expected nullability).
 */
sealed class ExpressionSet extends Serializable {
  private val baseSet: java.util.Set[Expression] = new java.util.HashSet[Expression]()
  def contains(expr: Expression): Boolean = contains(ExpressionEquals.normalize(expr))
  def add(expr: Expression): Unit = baseSet.add(ExpressionEquals.normalize(expr))
}
