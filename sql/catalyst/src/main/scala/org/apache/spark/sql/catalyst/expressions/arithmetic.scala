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

package org.apache.spark.sql
package catalyst
package expressions

import catalyst.analysis.UnresolvedException


case class UnaryMinus(child: Expression) extends UnaryExpression {
  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"-$child"
}

abstract class BinaryArithmetic extends BinaryExpression {
  self: Product =>

  def nullable = left.nullable || right.nullable

  override lazy val resolved =
    left.resolved && right.resolved && left.dataType == right.dataType

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "+"
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "-"
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "*"
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "/"
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "%"
}
