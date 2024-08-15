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
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, MapData, SQLOrderingUtil}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteExactNumeric, ByteType, CalendarIntervalType, CharType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalExactNumeric, DecimalType, DoubleExactNumeric, DoubleType, FloatExactNumeric, FloatType, FractionalType, IntegerExactNumeric, IntegerType, IntegralType, LongExactNumeric, LongType, MapType, NullType, NumericType, ShortExactNumeric, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, VarcharType, VariantType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.{ByteArray, UTF8String, VariantVal}
import org.apache.spark.util.ArrayImplicits._

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
    case VarcharType(_) => PhysicalStringType(SqlApiConf.get.defaultStringType.collationId)
    case CharType(_) => PhysicalStringType(SqlApiConf.get.defaultStringType.collationId)
    case s: StringType => PhysicalStringType(s.collationId)
    case FloatType => PhysicalFloatType
    case DoubleType => PhysicalDoubleType
    case DecimalType.Fixed(p, s) => PhysicalDecimalType(p, s)
    case BooleanType => PhysicalBooleanType
    case BinaryType => PhysicalBinaryType
    case TimestampType => PhysicalLongType
    case TimestampNTZType => PhysicalLongType
    case CalendarIntervalType => PhysicalCalendarIntervalType
    case DayTimeIntervalType(_, _) => PhysicalLongType
    case YearMonthIntervalType(_, _) => PhysicalIntegerType
    case DateType => PhysicalIntegerType
    case ArrayType(elementType, containsNull) => PhysicalArrayType(elementType, containsNull)
    case StructType(fields) => PhysicalStructType(fields)
    case MapType(keyType, valueType, valueContainsNull) =>
      PhysicalMapType(keyType, valueType, valueContainsNull)
    case VariantType => PhysicalVariantType
    case _ => UninitializedPhysicalType
  }

  def ordering(dt: DataType): Ordering[Any] = apply(dt).ordering.asInstanceOf[Ordering[Any]]
}

sealed trait PhysicalPrimitiveType

sealed abstract class PhysicalNumericType extends PhysicalDataType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization. In order for
  // implicitly[Numeric[JvmType]] to be valid, we have to change JvmType from a type variable to a
  // type parameter and add a numeric annotation (i.e., [JvmType : Numeric]). This gets
  // desugared by the compiler into an argument to the objects constructor. This means there is no
  // longer a no argument constructor and thus the JVM cannot serialize the object anymore.
  private[sql] val numeric: Numeric[InternalType]

  private[sql] def exactNumeric: Numeric[InternalType] = numeric
}

object PhysicalNumericType {
  def apply(dt: NumericType): PhysicalNumericType = {
    PhysicalDataType(dt).asInstanceOf[PhysicalNumericType]
  }

  def numeric(dt: NumericType): Numeric[Any] = {
    apply(dt).numeric.asInstanceOf[Numeric[Any]]
  }

  def exactNumeric(dt: NumericType): Numeric[Any] = {
    apply(dt).exactNumeric.asInstanceOf[Numeric[Any]]
  }
}

sealed abstract class PhysicalFractionalType extends PhysicalNumericType {
  private[sql] val fractional: Fractional[InternalType]
  private[sql] val asIntegral: Integral[InternalType]
}

object PhysicalFractionalType {
  def apply(dt: FractionalType): PhysicalFractionalType = {
    PhysicalDataType(dt).asInstanceOf[PhysicalFractionalType]
  }

  def fractional(dt: FractionalType): Fractional[Any] = {
    apply(dt).fractional.asInstanceOf[Fractional[Any]]
  }
}

sealed abstract class PhysicalIntegralType extends PhysicalNumericType {
  private[sql] val integral: Integral[InternalType]
}

object PhysicalIntegralType {
  def apply(dt: IntegralType): PhysicalIntegralType = {
    PhysicalDataType(dt).asInstanceOf[PhysicalIntegralType]
  }

  def integral(dt: IntegralType): Integral[Any] = {
    apply(dt).integral.asInstanceOf[Integral[Any]]
  }
}

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

class PhysicalByteType() extends PhysicalIntegralType with PhysicalPrimitiveType {
  private[sql] type InternalType = Byte
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = implicitly[Numeric[Byte]]
  override private[sql] val exactNumeric = ByteExactNumeric
  private[sql] val integral = implicitly[Integral[Byte]]
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

case class PhysicalDecimalType(precision: Int, scale: Int) extends PhysicalFractionalType {
  private[sql] type InternalType = Decimal
  private[sql] val ordering = Decimal.DecimalIsFractional
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = Decimal.DecimalIsFractional
  override private[sql] def exactNumeric = DecimalExactNumeric
  private[sql] val fractional = Decimal.DecimalIsFractional
  private[sql] val asIntegral = Decimal.DecimalAsIfIntegral
}

case object PhysicalDecimalType {
  def apply(precision: Int, scale: Int): PhysicalDecimalType = {
    new PhysicalDecimalType(precision, scale)
  }
}

class PhysicalDoubleType() extends PhysicalFractionalType with PhysicalPrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DoubleType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Double
  private[sql] val ordering =
    (x: Double, y: Double) => SQLOrderingUtil.compareDoubles(x, y)
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = implicitly[Numeric[Double]]
  override private[sql] def exactNumeric = DoubleExactNumeric
  private[sql] val fractional = implicitly[Fractional[Double]]
  private[sql] val asIntegral = DoubleType.DoubleAsIfIntegral
}
case object PhysicalDoubleType extends PhysicalDoubleType

class PhysicalFloatType() extends PhysicalFractionalType with PhysicalPrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "FloatType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Float
  private[sql] val ordering =
    (x: Float, y: Float) => SQLOrderingUtil.compareFloats(x, y)
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = implicitly[Numeric[Float]]
  override private[sql] def exactNumeric = FloatExactNumeric
  private[sql] val fractional = implicitly[Fractional[Float]]
  private[sql] val asIntegral = FloatType.FloatAsIfIntegral
}
case object PhysicalFloatType extends PhysicalFloatType

class PhysicalIntegerType() extends PhysicalIntegralType with PhysicalPrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "IntegerType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Int
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = implicitly[Numeric[Int]]
  override private[sql] val exactNumeric = IntegerExactNumeric
  private[sql] val integral = implicitly[Integral[Int]]
}
case object PhysicalIntegerType extends PhysicalIntegerType

class PhysicalLongType() extends PhysicalIntegralType with PhysicalPrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "LongType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Long
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = implicitly[Numeric[Long]]
  override private[sql] val exactNumeric = LongExactNumeric
  private[sql] val integral = implicitly[Integral[Long]]
}
case object PhysicalLongType extends PhysicalLongType

case class PhysicalMapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean)
    extends PhysicalDataType {
  override private[sql] def ordering = interpretedOrdering
  override private[sql] type InternalType = MapData
  @transient private[sql] lazy val tag = typeTag[InternalType]

  @transient
  private[sql] lazy val interpretedOrdering: Ordering[MapData] = new Ordering[MapData] {
    private[this] val keyOrdering =
      PhysicalDataType(keyType).ordering.asInstanceOf[Ordering[Any]]
    private[this] val valuesOrdering =
      PhysicalDataType(valueType).ordering.asInstanceOf[Ordering[Any]]

    override def compare(left: MapData, right: MapData): Int = {
      val lengthLeft = left.numElements()
      val lengthRight = right.numElements()
      val keyArrayLeft = left.keyArray()
      val valueArrayLeft = left.valueArray()
      val keyArrayRight = right.keyArray()
      val valueArrayRight = right.valueArray()
      val minLength = math.min(lengthLeft, lengthRight)
      var i = 0
      while (i < minLength) {
        var comp = compareElements(keyArrayLeft, keyArrayRight, keyType, i, keyOrdering)
        if (comp != 0) {
          return comp
        }
        comp = compareElements(valueArrayLeft, valueArrayRight, valueType, i, valuesOrdering)
        if (comp != 0) {
          return comp
        }

        i += 1
      }

      if (lengthLeft < lengthRight) {
        -1
      } else if (lengthLeft > lengthRight) {
        1
      } else {
        0
      }
    }

    private def compareElements(arrayLeft: ArrayData, arrayRight: ArrayData, dataType: DataType,
                        position: Int, ordering: Ordering[Any]): Int = {
      val isNullLeft = arrayLeft.isNullAt(position)
      val isNullRight = arrayRight.isNullAt(position)

      if (isNullLeft && isNullRight) {
        0
      } else if (isNullLeft) {
        -1
      } else if (isNullRight) {
        1
      } else {
        ordering.compare(
          arrayLeft.get(position, dataType),
          arrayRight.get(position, dataType)
        )
      }
    }
  }
}

class PhysicalNullType() extends PhysicalDataType with PhysicalPrimitiveType {
  override private[sql] def ordering =
    throw QueryExecutionErrors.orderedOperationUnsupportedByDataTypeError(
      "PhysicalNullType")
  override private[sql] type InternalType = Any
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
case object PhysicalNullType extends PhysicalNullType

class PhysicalShortType() extends PhysicalIntegralType with PhysicalPrimitiveType {
  private[sql] type InternalType = Short
  private[sql] val ordering = implicitly[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = implicitly[Numeric[Short]]
  override private[sql] val exactNumeric = ShortExactNumeric
  private[sql] val integral = implicitly[Integral[Short]]
}
case object PhysicalShortType extends PhysicalShortType

case class PhysicalStringType(collationId: Int) extends PhysicalDataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "StringType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = UTF8String
  private[sql] val ordering = CollationFactory.fetchCollation(collationId).comparator.compare(_, _)
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
object PhysicalStringType {
  def apply(collationId: Int): PhysicalStringType = new PhysicalStringType(collationId)
}

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
    forSchema(this.fields.map(_.dataType).toImmutableArraySeq).asInstanceOf[Ordering[InternalType]]
  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] def forSchema(dataTypes: Seq[DataType]): InterpretedOrdering = {
    new InterpretedOrdering(dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    })
  }
}

class PhysicalVariantType extends PhysicalDataType {
  private[sql] type InternalType = VariantVal
  @transient private[sql] lazy val tag = typeTag[InternalType]

  // TODO(SPARK-45891): Support comparison for the Variant type.
  override private[sql] def ordering =
    throw QueryExecutionErrors.orderedOperationUnsupportedByDataTypeError(
      "PhysicalVariantType")
}

object PhysicalVariantType extends PhysicalVariantType

object UninitializedPhysicalType extends PhysicalDataType {
  override private[sql] def ordering =
    throw QueryExecutionErrors.orderedOperationUnsupportedByDataTypeError(
      "UninitializedPhysicalType")
  override private[sql] type InternalType = Any
  @transient private[sql] lazy val tag = typeTag[InternalType]
}
