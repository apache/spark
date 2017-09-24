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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/**
 * Helper functions to check for valid data types.
 */
object TypeUtils {
  def checkForNumericExpr(dt: DataType, caller: String): TypeCheckResult = {
    if (dt.isInstanceOf[NumericType] || dt == NullType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"$caller requires numeric types, not $dt")
    }
  }

  def checkForOrderingExpr(dt: DataType, caller: String): TypeCheckResult = {
    if (isOrderable(dt)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"$caller does not support ordering on type $dt")
    }
  }

  def checkForSameTypeInputExpr(types: Seq[DataType], caller: String): TypeCheckResult = {
    if (types.size <= 1) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      val firstType = types.head
      types.foreach { t =>
        if (!t.sameType(firstType)) {
          return TypeCheckResult.TypeCheckFailure(
            s"input to $caller should all be the same type, but it's " +
              types.map(_.simpleString).mkString("[", ", ", "]"))
        }
      }
      TypeCheckResult.TypeCheckSuccess
    }
  }

  def getNumeric(t: DataType): Numeric[Any] =
    t.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]

  def getInterpretedOrdering(t: DataType): Ordering[Any] = {
    t match {
      case i: AtomicType => i.ordering.asInstanceOf[Ordering[Any]]
      case a: ArrayType => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case m: MapType if m.ordered => m.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case s: StructType => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case udt: UserDefinedType[_] => getInterpretedOrdering(udt.sqlType)
      case other =>
        throw new IllegalArgumentException(s"Type $other does not support ordered operations")
    }
  }

  /**
   * Returns true iff the data type can be ordered (i.e. can be sorted).
   */
  def isOrderable(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case dt: AtomicType => true
    case struct: StructType => struct.fields.forall(f => isOrderable(f.dataType))
    case array: ArrayType => isOrderable(array.elementType)
    case MapType(keyType, valueType, _, true) => isOrderable(keyType) && isOrderable(valueType)
    case udt: UserDefinedType[_] => isOrderable(udt.sqlType)
    case _ => false
  }


  def isOrderable(exprs: Seq[Expression]): Boolean = exprs.forall(e => isOrderable(e.dataType))

  def compareBinary(x: Array[Byte], y: Array[Byte]): Int = {
    for (i <- 0 until x.length; if i < y.length) {
      val v1 = x(i) & 0xff
      val v2 = y(i) & 0xff
      val res = v1 - v2
      if (res != 0) return res
    }
    x.length - y.length
  }
}
