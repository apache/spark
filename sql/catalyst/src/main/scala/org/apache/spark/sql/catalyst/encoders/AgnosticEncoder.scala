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

import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInt}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period}

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * A non implementation specific encoder. This encoder containers all the information needed
 * to generate an implementation specific encoder (e.g. InternalRow <=> Custom Object).
 */
trait AgnosticEncoder[T] extends Encoder[T] {
  def isPrimitive: Boolean
  def nullable: Boolean = !isPrimitive
  def dataType: DataType
  override def schema: StructType = StructType(StructField("value", dataType, nullable) :: Nil)
}

// TODO check RowEncoder
// TODO check BeanEncoder
object AgnosticEncoders {
  case class OptionEncoder[E](elementEncoder: AgnosticEncoder[E])
    extends AgnosticEncoder[Option[E]] {
    override def isPrimitive: Boolean = false
    override def dataType: DataType = elementEncoder.dataType
    override val clsTag: ClassTag[Option[E]] = ClassTag(classOf[Option[E]])
  }

  case class ArrayEncoder[E](element: AgnosticEncoder[E])
    extends AgnosticEncoder[Array[E]] {
    override def isPrimitive: Boolean = false
    override def dataType: DataType = ArrayType(element.dataType, element.nullable)
    override val clsTag: ClassTag[Array[E]] = element.clsTag.wrap
  }

  case class IterableEncoder[C <: Iterable[E], E](
      override val clsTag: ClassTag[C],
      element: AgnosticEncoder[E])
    extends AgnosticEncoder[C] {
    override def isPrimitive: Boolean = false
    override val dataType: DataType = ArrayType(element.dataType, element.nullable)
  }

  case class MapEncoder[C, K, V](
      override val clsTag: ClassTag[C],
      keyEncoder: AgnosticEncoder[K],
      valueEncoder: AgnosticEncoder[V])
    extends AgnosticEncoder[C] {
    override def isPrimitive: Boolean = false
    override val dataType: DataType = MapType(
      keyEncoder.dataType,
      valueEncoder.dataType,
      valueEncoder.nullable)
  }

  case class EncoderField(name: String, enc: AgnosticEncoder[_]) {
    def structField: StructField = StructField(name, enc.dataType, enc.nullable)
  }

  // This supports both Product and DefinedByConstructorParams
  case class ProductEncoder[K](
      override val clsTag: ClassTag[K],
      fields: Seq[EncoderField])
    extends AgnosticEncoder[K] {
    override def isPrimitive: Boolean = false
    override val schema: StructType = StructType(fields.map(_.structField))
    override def dataType: DataType = schema
  }

  // This will only work for encoding from/to Sparks' InternalRow format.
  // It is here for compatibility.
  case class UDTEncoder[E >: Null](
      udt: UserDefinedType[E],
      udtClass: Class[_ <: UserDefinedType[_]])
    extends AgnosticEncoder[E] {
    override def isPrimitive: Boolean = false
    override def dataType: DataType = udt
    override def clsTag: ClassTag[E] = ClassTag(udt.userClass)
  }

  // Enums are special leafs because we need to capture the class.
  protected abstract class EnumEncoder[E] extends AgnosticEncoder[E] {
    override def isPrimitive: Boolean = false
    override def dataType: DataType = StringType
  }
  case class ScalaEnumEncoder[T, E](
     parent: Class[T],
     override val clsTag: ClassTag[E])
    extends EnumEncoder[E]
  case class JavaEnumEncoder[E](override val clsTag: ClassTag[E]) extends EnumEncoder[E]

  protected abstract class LeafEncoder[E : ClassTag](override val dataType: DataType)
    extends AgnosticEncoder[E] {
    override val clsTag: ClassTag[E] = classTag[E]
    override val isPrimitive: Boolean = clsTag.runtimeClass.isPrimitive
  }

  // Primitive encoders
  case object PrimitiveBooleanEncoder extends LeafEncoder[Boolean](BooleanType)
  case object PrimitiveByteEncoder extends LeafEncoder[Byte](ByteType)
  case object PrimitiveShortEncoder extends LeafEncoder[Short](ShortType)
  case object PrimitiveIntEncoder extends LeafEncoder[Int](IntegerType)
  case object PrimitiveLongEncoder extends LeafEncoder[Long](LongType)
  case object PrimitiveFloatEncoder extends LeafEncoder[Float](FloatType)
  case object PrimitiveDoubleEncoder extends LeafEncoder[Double](DoubleType)

  // Primitive wrapper encoders.
  case object NullEncoder extends LeafEncoder[java.lang.Void](NullType)
  case object BoxedBooleanEncoder extends LeafEncoder[java.lang.Boolean](BooleanType)
  case object BoxedByteEncoder extends LeafEncoder[java.lang.Byte](ByteType)
  case object BoxedShortEncoder extends LeafEncoder[java.lang.Short](ShortType)
  case object BoxedIntEncoder extends LeafEncoder[java.lang.Integer](IntegerType)
  case object BoxedLongEncoder extends LeafEncoder[java.lang.Long](LongType)
  case object BoxedFloatEncoder extends LeafEncoder[java.lang.Float](FloatType)
  case object BoxedDoubleEncoder extends LeafEncoder[java.lang.Double](DoubleType)

  // Nullable leaf encoders
  case object StringEncoder extends LeafEncoder[String](StringType)
  case object BinaryEncoder extends LeafEncoder[Array[Byte]](BinaryType)
  case object SparkDecimalEncoder extends LeafEncoder[Decimal](DecimalType.SYSTEM_DEFAULT)
  case object ScalaDecimalEncoder extends LeafEncoder[BigDecimal](DecimalType.SYSTEM_DEFAULT)
  case object JavaDecimalEncoder extends LeafEncoder[JBigDecimal](DecimalType.SYSTEM_DEFAULT)
  case object ScalaBigIntEncoder extends LeafEncoder[BigInt](DecimalType.BigIntDecimal)
  case object JavaBigIntEncoder extends LeafEncoder[JBigInt](DecimalType.BigIntDecimal)
  case object CalendarIntervalEncoder extends LeafEncoder[CalendarInterval](CalendarIntervalType)
  case object DayTimeIntervalEncoder extends LeafEncoder[Duration](DayTimeIntervalType())
  case object YearMonthIntervalEncoder extends LeafEncoder[Period](YearMonthIntervalType())
  case object DateEncoder extends LeafEncoder[java.sql.Date](DateType)
  case object LocalDateEncoder extends LeafEncoder[LocalDate](DateType)
  case object TimestampEncoder extends LeafEncoder[java.sql.Timestamp](TimestampType)
  case object InstantEncoder extends LeafEncoder[Instant](TimestampType)
  case object LocalDateTimeEncoder extends LeafEncoder[LocalDateTime](TimestampNTZType)
}

