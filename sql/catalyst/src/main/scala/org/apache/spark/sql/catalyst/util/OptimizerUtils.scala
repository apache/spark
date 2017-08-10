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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.expressions._


/**
 * Common utility methods used by Optimizer stuffs.
 */
object OptimizerUtils {

  private def containsNonConjunctionPredicates(expression: Expression): Boolean = expression.find {
    case _: Not | _: Or => true
    case _ => false
  }.isDefined

  def getLiteralEqualityPredicates(conjunctivePredicates: Seq[Expression])
    : Seq[((AttributeReference, Literal), BinaryComparison)] = {
    val conjunctiveEqualPredicates =
      conjunctivePredicates
        .filter(expr => expr.isInstanceOf[EqualTo] || expr.isInstanceOf[EqualNullSafe])
        .filterNot(expr => containsNonConjunctionPredicates(expr))
    conjunctiveEqualPredicates.collect {
      case e @ EqualTo(left: AttributeReference, right: Literal) => ((left, right), e)
      case e @ EqualTo(left: Literal, right: AttributeReference) => ((right, left), e)
      case e @ EqualNullSafe(left: AttributeReference, right: Literal) => ((left, right), e)
      case e @ EqualNullSafe(left: Literal, right: AttributeReference) => ((right, left), e)
    }
  }
}
