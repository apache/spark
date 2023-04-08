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

import org.apache.spark.sql.catalyst.expressions.{Ascending, BoundReference, InterpretedOrdering, SortOrder}
import org.apache.spark.sql.catalyst.util.{ArrayData, SQLOrderingUtil}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, NullType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

sealed abstract class PhysicalDataType {
  private[sql] type InternalType
  private[sql] def ordering: Ordering[InternalType]
  private[sql] val tag: TypeTag[InternalType]
}

object PhysicalDataType {
  def apply(dt: DataType): PhysicalDataType = dt match {
    case NullType => PhysicalNullType
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
    case YearMonthIntervalType(_, _) => PhysicalIntegerType
    case DateType => PhysicalIntegerType
    case ArrayType(elementType, containsNull) => PhysicalArrayType(elementType, containsNull)
    case StructType(fields) => PhysicalStructType(fields)
    case MapType(keyType, valueType, valueContainsNull) =>
      PhysicalMapType(keyType, valueType, valueContainsNull)
    case _ => UninitializedPhysicalType
  }

  def ordering(dt: DataType): Ordering[Any] = apply(dt).ordering.asInstanceOf[Ordering[Any]]
}

trait PhysicalPrimitiveType

class PhysicalBinaryType() extends PhysicalDataType {
  private[sql] val ordering =
    (x: Array[Byte], y: Array[Byte]) => ByteArray.compareBinary(x, y)

  private[sql] type InternalType = Array[Byte]
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalBinaryType extends PhysicalBinaryType

class PhysicalBooleanType extends PhysicalDataType with PhysicalPrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "BooleanType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Boolean
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalBooleanType extends PhysicalBooleanType

class PhysicalByteType() extends PhysicalDataType with PhysicalPrimitiveType {
  private[sql] type InternalType = Byte
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalByteType extends PhysicalByteType

class PhysicalCalendarIntervalType() extends PhysicalDataType {
  override private[sql] def ordering =
    throw QueryExecutionErrors.orderedOperationUnsupportedByDataTypeError(
      "PhysicalCalendarIntervalType")
  override private[sql] type InternalType = Any
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalCalendarIntervalType extends PhysicalCalendarIntervalType

case class PhysicalDecimalType(precision: Int, scale: Int) extends PhysicalDataType {
  private[sql] type InternalType = Decimal
  private[sql] val ordering = Decimal.DecimalIsFractional
  @transient private[sql] lazy val tag = typeTag[InternalType]
}

case object PhysicalDecimalType {
  def apply(precision: Int, scale: Int): PhysicalDecimalType = {
    new PhysicalDecimalType(precision, scale)
  }
}

class PhysicalDoubleType() extends PhysicalDataType with PhysicalPrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DoubleType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Double
  private[sql] val ordering =
    (x: Double, y: Double) => SQLOrderingUtil.compareDoubles(x, y)
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalDoubleType extends PhysicalDoubleType

class PhysicalFloatType() extends PhysicalDataType with PhysicalPrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "FloatType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Float
  private[sql] val ordering =
    (x: Float, y: Float) => SQLOrderingUtil.compareFloats(x, y)
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalFloatType extends PhysicalFloatType

class PhysicalIntegerType() extends PhysicalDataType with PhysicalPrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "IntegerType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Int
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalIntegerType extends PhysicalIntegerType

class PhysicalLongType() extends PhysicalDataType with PhysicalPrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "LongType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Long
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalLongType extends PhysicalLongType

case class PhysicalMapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean)
    extends PhysicalDataType {
  override private[sql] def ordering =
    throw QueryExecutionErrors.orderedOperationUnsupportedByDataTypeError("PhysicalMapType")
  override private[sql] type InternalType = Any
  @transient private[sql] lazy val tag = typeTag[InternalType]
}

class PhysicalNullType() extends PhysicalDataType with PhysicalPrimitiveType {
  override private[sql] def ordering =
    throw QueryExecutionErrors.orderedOperationUnsupportedByDataTypeError(
      "PhysicalNullType")
  override private[sql] type InternalType = Any
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalNullType extends PhysicalNullType

class PhysicalShortType() extends PhysicalDataType with PhysicalPrimitiveType {
  private[sql] type InternalType = Short
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalShortType extends PhysicalShortType

class PhysicalStringType() extends PhysicalDataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "StringType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = UTF8String
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalStringType extends PhysicalStringType

case class PhysicalArrayType(
    elementType: DataType, containsNull: Boolean) extends PhysicalDataType {
  override private[sql] type InternalType = ArrayData
  override private[sql] def ordering = interpretedOrdering
  @transient private[sql] lazy val tag = typeTag[InternalType]

  @transient
  private[sql] lazy val interpretedOrdering: Ordering[ArrayData] = new Ordering[ArrayData] {
    private[this] val elementOrdering: Ordering[Any] =
      PhysicalDataType(elementType).ordering.asInstanceOf[Ordering[Any]]

    def compare(x: ArrayData, y: ArrayData): Int = {
      val leftArray = x
      val rightArray = y
      val minLength = scala.math.min(leftArray.numElements(), rightArray.numElements())
      var i = 0
      while (i < minLength) {
        val isNullLeft = leftArray.isNullAt(i)
        val isNullRight = rightArray.isNullAt(i)
        if (isNullLeft && isNullRight) {
          // Do nothing.
        } else if (isNullLeft) {
          return -1
        } else if (isNullRight) {
          return 1
        } else {
          val comp =
            elementOrdering.compare(
              leftArray.get(i, elementType),
              rightArray.get(i, elementType))
          if (comp != 0) {
            return comp
          }
        }
        i += 1
      }
      if (leftArray.numElements() < rightArray.numElements()) {
        -1
      } else if (leftArray.numElements() > rightArray.numElements()) {
        1
      } else {
        0
      }
    }
  }
}

case class PhysicalStructType(fields: Array[StructField]) extends PhysicalDataType {
  override private[sql] type InternalType = Any
  override private[sql] def ordering =
    forSchema(this.fields.map(_.dataType)).asInstanceOf[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] def forSchema(dataTypes: Seq[DataType]): InterpretedOrdering = {
    new InterpretedOrdering(dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    })
  }
}

object UninitializedPhysicalType extends PhysicalDataType {
  override private[sql] def ordering =
    throw QueryExecutionErrors.orderedOperationUnsupportedByDataTypeError(
      "UninitializedPhysicalType")
  override private[sql] type InternalType = Any
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
