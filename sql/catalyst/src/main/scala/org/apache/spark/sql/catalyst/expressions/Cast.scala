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

import org.apache.spark.sql.catalyst.types._

/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType) extends UnaryExpression {
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"CAST($child, $dataType)"

  type EvaluatedType = Any

  lazy val castingFunction: Any => Any = (child.dataType, dataType) match {
    case (BinaryType, StringType) => a: Any => new String(a.asInstanceOf[Array[Byte]])
    case (StringType, BinaryType) => a: Any => a.asInstanceOf[String].getBytes
    case (_, StringType) => a: Any => a.toString
    case (StringType, IntegerType) => a: Any => castOrNull(a, _.toInt)
    case (StringType, DoubleType) => a: Any => castOrNull(a, _.toDouble)
    case (StringType, FloatType) => a: Any => castOrNull(a, _.toFloat)
    case (StringType, LongType) => a: Any => castOrNull(a, _.toLong)
    case (StringType, ShortType) => a: Any => castOrNull(a, _.toShort)
    case (StringType, ByteType) => a: Any => castOrNull(a, _.toByte)
    case (StringType, DecimalType) => a: Any => castOrNull(a, BigDecimal(_))
    case (BooleanType, ByteType) => {
      case null => null
      case true => 1.toByte
      case false => 0.toByte
    }
    case (dt, IntegerType) =>
      a: Any => dt.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].toInt(a)
    case (dt, DoubleType) =>
      a: Any => dt.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].toDouble(a)
    case (dt, FloatType) =>
      a: Any => dt.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].toFloat(a)
    case (dt, LongType) =>
      a: Any => dt.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].toLong(a)
    case (dt, ShortType) =>
      a: Any => dt.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].toInt(a).toShort
    case (dt, ByteType) =>
      a: Any => dt.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].toInt(a).toByte
    case (dt, DecimalType) =>
      a: Any =>
        BigDecimal(dt.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]].toDouble(a))
  }

  @inline
  protected def castOrNull[A](a: Any, f: String => A) =
    try f(a.asInstanceOf[String]) catch {
      case _: java.lang.NumberFormatException => null
    }

  override def apply(input: Row): Any = {
    val evaluated = child.apply(input)
    if (evaluated == null) {
      null
    } else {
      castingFunction(evaluated)
    }
  }
}
