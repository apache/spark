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

  lazy val numeric =
    baseType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
}

trait OrderingHolder {
  self: Expression =>

  protected def baseType: DataType = dataType

  lazy val ordering =
    baseType.asInstanceOf[AtomicType].ordering.asInstanceOf[Ordering[Any]]
}

trait TypeEqualConstraint {
  self: Expression =>

  protected def typeChecker: DataType => Boolean = _ => true

  def left: Expression
  def right: Expression

  final override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType &&
    typeChecker(left.dataType)
}

trait TypeEqualConstraintWithDataType extends TypeEqualConstraint {
  self: Expression =>

  override def dataType: DataType = {
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
