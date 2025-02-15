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
package org.apache.spark.sql.catalyst.encoders

import scala.collection.Map

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{BinaryEncoder, CalendarIntervalEncoder, NullEncoder, PrimitiveBooleanEncoder, PrimitiveByteEncoder, PrimitiveDoubleEncoder, PrimitiveFloatEncoder, PrimitiveIntEncoder, PrimitiveLongEncoder, PrimitiveShortEncoder, SparkDecimalEncoder, VariantEncoder}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.types.{PhysicalBinaryType, PhysicalIntegerType, PhysicalLongType}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ObjectType, ShortType, StringType, StructType, TimestampNTZType, TimestampType, UserDefinedType, VariantType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String, VariantVal}

/**
 * :: DeveloperApi ::
 * Extensible [[AgnosticEncoder]] providing conversion extension points over type T
 * @tparam T over T
 */
@DeveloperApi
trait AgnosticExpressionPathEncoder[T]
  extends AgnosticEncoder[T] {
  /**
   * Converts from T to InternalRow
   * @param input the starting input path
   * @return
   */
  def toCatalyst(input: Expression): Expression

  /**
   * Converts from InternalRow to T
   * @param inputPath path expression from InternalRow
   * @return
   */
  def fromCatalyst(inputPath: Expression): Expression
}

/**
 * Helper class for Generating [[ExpressionEncoder]]s.
 */
object EncoderUtils {
  /**
   * Return the data type we expect to see when deserializing a value with encoder `enc`.
   */
  private[catalyst] def externalDataTypeFor(enc: AgnosticEncoder[_]): DataType = {
    externalDataTypeFor(enc, lenientSerialization = false)
  }

  private[catalyst]  def lenientExternalDataTypeFor(enc: AgnosticEncoder[_]): DataType =
    externalDataTypeFor(enc, enc.lenientSerialization)

  private def externalDataTypeFor(
      enc: AgnosticEncoder[_],
      lenientSerialization: Boolean): DataType = {
    // DataType can be native.
    if (isNativeEncoder(enc)) {
      enc.dataType
    } else if (lenientSerialization) {
      ObjectType(classOf[java.lang.Object])
    } else {
      ObjectType(enc.clsTag.runtimeClass)
    }
  }

  /**
   * Returns true if the encoders' internal and external data type is the same.
   */
  private[catalyst] def isNativeEncoder(enc: AgnosticEncoder[_]): Boolean = enc match {
    case PrimitiveBooleanEncoder => true
    case PrimitiveByteEncoder => true
    case PrimitiveShortEncoder => true
    case PrimitiveIntEncoder => true
    case PrimitiveLongEncoder => true
    case PrimitiveFloatEncoder => true
    case PrimitiveDoubleEncoder => true
    case NullEncoder => true
    case CalendarIntervalEncoder => true
    case BinaryEncoder => true
    case _: SparkDecimalEncoder => true
    case VariantEncoder => true
    case _ => false
  }

  def dataTypeJavaClass(dt: DataType): Class[_] = {
    dt match {
      case _: DecimalType => classOf[Decimal]
      case _: DayTimeIntervalType => classOf[PhysicalLongType.InternalType]
      case _: YearMonthIntervalType => classOf[PhysicalIntegerType.InternalType]
      case _: StringType => classOf[UTF8String]
      case _: StructType => classOf[InternalRow]
      case _: ArrayType => classOf[ArrayData]
      case _: MapType => classOf[MapData]
      case ObjectType(cls) => cls
      case _ => typeJavaMapping.getOrElse(dt, classOf[java.lang.Object])
    }
  }

  @scala.annotation.tailrec
  def javaBoxedType(dt: DataType): Class[_] = dt match {
    case _: DecimalType => classOf[Decimal]
    case _: DayTimeIntervalType => classOf[java.lang.Long]
    case _: YearMonthIntervalType => classOf[java.lang.Integer]
    case BinaryType => classOf[Array[Byte]]
    case _: StringType => classOf[UTF8String]
    case CalendarIntervalType => classOf[CalendarInterval]
    case _: StructType => classOf[InternalRow]
    case _: ArrayType => classOf[ArrayData]
    case _: MapType => classOf[MapData]
    case udt: UserDefinedType[_] => javaBoxedType(udt.sqlType)
    case ObjectType(cls) => cls
    case _ => typeBoxedJavaMapping.getOrElse(dt, classOf[java.lang.Object])
  }

  def expressionJavaClasses(arguments: Seq[Expression]): Seq[Class[_]] = {
    if (arguments != Nil) {
      arguments.map(e => dataTypeJavaClass(e.dataType))
    } else {
      Seq.empty
    }
  }

  val typeJavaMapping: Map[DataType, Class[_]] = Map[DataType, Class[_]](
    BooleanType -> classOf[Boolean],
    ByteType -> classOf[Byte],
    ShortType -> classOf[Short],
    IntegerType -> classOf[Int],
    LongType -> classOf[Long],
    FloatType -> classOf[Float],
    DoubleType -> classOf[Double],
    StringType -> classOf[UTF8String],
    DateType -> classOf[PhysicalIntegerType.InternalType],
    TimestampType -> classOf[PhysicalLongType.InternalType],
    TimestampNTZType -> classOf[PhysicalLongType.InternalType],
    BinaryType -> classOf[PhysicalBinaryType.InternalType],
    CalendarIntervalType -> classOf[CalendarInterval],
    VariantType -> classOf[VariantVal]
  )

  val typeBoxedJavaMapping: Map[DataType, Class[_]] = Map[DataType, Class[_]](
    BooleanType -> classOf[java.lang.Boolean],
    ByteType -> classOf[java.lang.Byte],
    ShortType -> classOf[java.lang.Short],
    IntegerType -> classOf[java.lang.Integer],
    LongType -> classOf[java.lang.Long],
    FloatType -> classOf[java.lang.Float],
    DoubleType -> classOf[java.lang.Double],
    DateType -> classOf[java.lang.Integer],
    TimestampType -> classOf[java.lang.Long],
    TimestampNTZType -> classOf[java.lang.Long]
  )
}
