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

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.types._

trait NumericHolder {
  self: Expression =>

  protected def baseType: DataType = dataType

  lazy val numeric = baseType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case NullType => UnresolvedNumeric
  }
}

trait OrderingHolder {
  self: Expression =>

  protected def baseType: DataType = dataType

  lazy val ordering = baseType match {
    case a: AtomicType => a.ordering.asInstanceOf[Ordering[Any]]
    case NullType => UnresolvedOrdering
  }
}

trait TypeEqualConstraint {
  self: Expression =>

  protected def typeChecker: DataType => Boolean = _ => true

  def left: Expression
  def right: Expression

  final override lazy val resolved =
    childrenResolved &&
    left.dataType == right.dataType &&
    typeChecker(left.dataType)
}

trait DataTypeMerger {
  self: Expression with TypeEqualConstraint =>

  final override def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }
}

object NumericTypeChecker extends (DataType => Boolean) {
  override def apply(t: DataType): Boolean =
    t.isInstanceOf[NumericType] || t.isInstanceOf[NullType]
}

object BitwiseTypeChecker extends (DataType => Boolean) {
  override def apply(t: DataType): Boolean =
    t.isInstanceOf[IntegralType] || t.isInstanceOf[NullType]
}

object OrderingTypeChecker extends (DataType => Boolean) {
  override def apply(t: DataType): Boolean =
    t.isInstanceOf[AtomicType] || t.isInstanceOf[NullType]
}

object BooleanTypeChecker extends (DataType => Boolean) {
  override def apply(t: DataType): Boolean =
    t == BooleanType || t.isInstanceOf[NullType]
}

object UnresolvedOrdering extends Ordering[Any] {
  override def compare(x: Any, y: Any): Int = sys.error(s"Type does not support ordered operations")
}

object UnresolvedNumeric extends Numeric[Any] {
  override def plus(x: Any, y: Any): Any = error

  override def toDouble(x: Any): Double = error

  override def toFloat(x: Any): Float = error

  override def toInt(x: Any): Int = error

  override def negate(x: Any): Any = error

  override def fromInt(x: Int): Any = error

  override def toLong(x: Any): Long = error

  override def times(x: Any, y: Any): Any = error

  override def minus(x: Any, y: Any): Any = error

  override def compare(x: Any, y: Any): Int = UnresolvedOrdering.compare(x, y)

  private[this] def error = sys.error(s"Type does not support numeric operations")
}

object UnresolvedIntegral extends Integral[Any] {
  override def quot(x: Any, y: Any): Any = error

  override def rem(x: Any, y: Any): Any = error

  override def toDouble(x: Any): Double = error

  override def plus(x: Any, y: Any): Any = error

  override def toFloat(x: Any): Float = error

  override def toInt(x: Any): Int = error

  override def negate(x: Any): Any = error

  override def fromInt(x: Int): Any = error

  override def toLong(x: Any): Long = error

  override def times(x: Any, y: Any): Any = error

  override def minus(x: Any, y: Any): Any = error

  override def compare(x: Any, y: Any): Int = UnresolvedOrdering.compare(x, y)

  def bitwiseAnd(x: Any, y: Any): Any = error("&")

  def bitwiseOr(x: Any, y: Any): Any = error("|")

  def bitwiseXor(x: Any, y: Any): Any = error("^")

  def bitwiseNot(x: Any): Any = error("~")

  private[this] def error = sys.error(s"Type does not support numeric operations")
  private[this] def error(op: String) = sys.error(s"Type does not support bitwise $op operation")
}