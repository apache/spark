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

package org.apache.spark.sql.catalyst.types

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.util.SQLOrderingUtil
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AtomicType, BinaryType, BooleanType, ByteType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, NumericType, ShortType, StringType, StructField, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

sealed abstract class PhysicalDataType

object PhysicalDataType {
  def apply(dt: DataType): PhysicalDataType = dt match {
    case NullType => PhysicalNullType
    case BooleanType => PhysicalBooleanType
    case ByteType => PhysicalByteType
    case ShortType => PhysicalShortType
    case IntegerType => PhysicalIntegerType
    case LongType => PhysicalLongType
    case FloatType => PhysicalFloatType
    case DoubleType => PhysicalDoubleType
    case DecimalType.Fixed(p, s) => PhysicalDecimalType(p, s)
    case BinaryType => PhysicalBinaryType
    case _ => UninitializedPhysicalType
  }
}

trait PhysicalPrimitiveType

sealed abstract class TypedPhysicalDataType extends PhysicalDataType {
  private[sql] type InternalType
}

sealed abstract class OrderedPhysicalDataType extends TypedPhysicalDataType {
  private[sql] def ordering: Ordering[InternalType]
}

object OrderedPhysicalDataType {
  def apply(dt: DataType): OrderedPhysicalDataType =
    PhysicalDataType(dt).asInstanceOf[OrderedPhysicalDataType]

  def ordering(dt: DataType): Ordering[Any] = {
    try apply(dt).ordering.asInstanceOf[Ordering[Any]] catch {
      case NonFatal(_) =>
        throw QueryExecutionErrors.unsupportedTypeError(dt)
    }
  }
}

sealed abstract class PhysicalAtomicType extends OrderedPhysicalDataType {
  private[sql] val tag: TypeTag[InternalType]
}

object PhysicalAtomicType extends PhysicalAtomicType {
  def apply(dt: AtomicType): PhysicalAtomicType = dt match {
    case ByteType => PhysicalByteType
    case ShortType => PhysicalShortType
    case IntegerType => PhysicalIntegerType
    case LongType => PhysicalLongType
    case StringType => PhysicalStringType
    case FloatType => PhysicalFloatType
    case DoubleType => PhysicalDoubleType
    case DecimalType.Fixed(p, s) => PhysicalDecimalType(p, s)
    case BooleanType => PhysicalBooleanType
    case BinaryType => PhysicalBinaryType
    case TimestampType => PhysicalLongType
    case TimestampNTZType => PhysicalLongType
    case DayTimeIntervalType(_, _) => PhysicalLongType
    case DateType => PhysicalIntegerType
    case _ => throw QueryExecutionErrors.unsupportedOperationExceptionError()
  }

  override private[sql] val tag = null
  override private[sql] def ordering = null
  override private[sql] type InternalType = Null
}

sealed abstract class PhysicalNumericType extends PhysicalAtomicType {
}

object PhysicalNumericType extends PhysicalNumericType {
  def apply(nt: NumericType): PhysicalNumericType = {
    PhysicalDataType(nt).asInstanceOf[PhysicalNumericType]
  }

  override private[sql] val tag = null
  override private[sql] def ordering = null
  override private[sql] type InternalType = Null
}

sealed abstract class PhysicalIntegralType extends PhysicalNumericType {
}

object PhysicalIntegralType extends PhysicalIntegralType {
  def apply(dt: DataType): PhysicalIntegralType = dt match {
    case IntegerType => PhysicalIntegerType
    case ByteType => PhysicalByteType
    case ShortType => PhysicalShortType
    case LongType => PhysicalLongType
    case _ => UninitializedPhysicalIntegralType
  }

  def ordering(dt: DataType): Ordering[Any] = {
    try apply(dt).ordering.asInstanceOf[Ordering[Any]] catch {
      case NonFatal(_) =>
        throw QueryExecutionErrors.unsupportedTypeError(dt)
    }
  }

  override private[sql] val tag = null
  override private[sql] def ordering = null
  override private[sql] type InternalType = Null
}

case class PhysicalArrayType(elementType: DataType, containsNull: Boolean) extends PhysicalDataType

class PhysicalBinaryType() extends PhysicalAtomicType {

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering =
    (x: Array[Byte], y: Array[Byte]) => ByteArray.compareBinary(x, y)

  private[sql] type InternalType = Array[Byte]
}
case object PhysicalBinaryType extends PhysicalBinaryType

class PhysicalBooleanType extends PhysicalAtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "BooleanType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Boolean
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering = implicitly[Ordering[InternalType]]
}
case object PhysicalBooleanType extends PhysicalBooleanType with PhysicalPrimitiveType

class PhysicalByteType() extends PhysicalIntegralType {
  private[sql] type InternalType = Byte
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering = implicitly[Ordering[InternalType]]
}
case object PhysicalByteType extends PhysicalByteType with PhysicalPrimitiveType

class PhysicalCalendarIntervalType() extends PhysicalDataType
case object PhysicalCalendarIntervalType extends PhysicalCalendarIntervalType

case class PhysicalDecimalType(precision: Int, scale: Int) extends PhysicalNumericType {
  private[sql] type InternalType = Decimal
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering = Decimal.DecimalIsFractional
}

case object PhysicalDecimalType {
  def apply(precision: Int, scale: Int): PhysicalDecimalType = {
    new PhysicalDecimalType(precision, scale)
  }
}

class PhysicalDoubleType() extends PhysicalNumericType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DoubleType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Double
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering =
    (x: Double, y: Double) => SQLOrderingUtil.compareDoubles(x, y)
}
case object PhysicalDoubleType extends PhysicalDoubleType with PhysicalPrimitiveType

class PhysicalFloatType() extends PhysicalNumericType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "FloatType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Float
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering =
    (x: Float, y: Float) => SQLOrderingUtil.compareFloats(x, y)
}
case object PhysicalFloatType extends PhysicalFloatType with PhysicalPrimitiveType

class PhysicalIntegerType() extends PhysicalIntegralType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "IntegerType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Int
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering = implicitly[Ordering[InternalType]]
}
case object PhysicalIntegerType extends PhysicalIntegerType with PhysicalPrimitiveType

class PhysicalLongType() extends PhysicalIntegralType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "LongType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Long
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering = implicitly[Ordering[InternalType]]
}
case object PhysicalLongType extends PhysicalLongType with PhysicalPrimitiveType

case class PhysicalMapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean)
    extends PhysicalDataType

class PhysicalNullType() extends PhysicalDataType
case object PhysicalNullType extends PhysicalNullType with PhysicalPrimitiveType

class PhysicalShortType() extends PhysicalIntegralType {
  private[sql] type InternalType = Short
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering = implicitly[Ordering[InternalType]]
}
case object PhysicalShortType extends PhysicalShortType with PhysicalPrimitiveType

class PhysicalStringType() extends PhysicalAtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "StringType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = UTF8String
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering = implicitly[Ordering[InternalType]]
}
case object PhysicalStringType extends PhysicalStringType

case class PhysicalStructType(fields: Array[StructField]) extends PhysicalDataType

object UninitializedPhysicalType extends PhysicalDataType
object UninitializedPhysicalIntegralType extends PhysicalIntegralType {
  override private[sql] val tag = null
  override private[sql] def ordering = null
  override private[sql] type InternalType = Null
}
