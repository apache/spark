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

import java.{sql => jsql}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInt}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period}
import java.util.concurrent.ConcurrentHashMap

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, VariantVal}
import org.apache.spark.util.SparkClassUtils

/**
 * A non implementation specific encoder. This encoder containers all the information needed
 * to generate an implementation specific encoder (e.g. InternalRow <=> Custom Object).
 *
 * The input of the serialization does not need to match the external type of the encoder. This is
 * called lenient serialization. An example of this is lenient date serialization, in this case both
 * [[java.sql.Date]] and [[java.time.LocalDate]] are allowed. Deserialization is never lenient; it
 * will always produce instance of the external type.
 *
 */
trait AgnosticEncoder[T] extends Encoder[T] {
  def isPrimitive: Boolean
  def nullable: Boolean = !isPrimitive
  def dataType: DataType
  override def schema: StructType = StructType(StructField("value", dataType, nullable) :: Nil)
  def lenientSerialization: Boolean = false
  def isStruct: Boolean = false
}

object AgnosticEncoders {
  object UnboundEncoder extends AgnosticEncoder[Any] {
    override def isPrimitive: Boolean = false
    override def dataType: DataType = NullType
    override def clsTag: ClassTag[Any] = classTag[Any]
  }

  case class OptionEncoder[E](elementEncoder: AgnosticEncoder[E])
    extends AgnosticEncoder[Option[E]] {
    override def isPrimitive: Boolean = false
    override def dataType: DataType = elementEncoder.dataType
    override val clsTag: ClassTag[Option[E]] = ClassTag(classOf[Option[E]])
  }

  case class ArrayEncoder[E](element: AgnosticEncoder[E], containsNull: Boolean)
    extends AgnosticEncoder[Array[E]] {
    override def isPrimitive: Boolean = false
    override def dataType: DataType = ArrayType(element.dataType, containsNull)
    override val clsTag: ClassTag[Array[E]] = element.clsTag.wrap
  }

  /**
   * Encoder for collections.
   *
   * This encoder can be lenient for [[Row]] encoders. In that case we allow [[Seq]], primitive
   * array (if any), and generic arrays as input.
   */
  case class IterableEncoder[C, E](
      override val clsTag: ClassTag[C],
      element: AgnosticEncoder[E],
      containsNull: Boolean,
      override val lenientSerialization: Boolean)
    extends AgnosticEncoder[C] {
    override def isPrimitive: Boolean = false
    override val dataType: DataType = ArrayType(element.dataType, containsNull)
  }

  case class MapEncoder[C, K, V](
      override val clsTag: ClassTag[C],
      keyEncoder: AgnosticEncoder[K],
      valueEncoder: AgnosticEncoder[V],
      valueContainsNull: Boolean)
    extends AgnosticEncoder[C] {
    override def isPrimitive: Boolean = false
    override val dataType: DataType = MapType(
      keyEncoder.dataType,
      valueEncoder.dataType,
      valueContainsNull)
  }

  case class EncoderField(
      name: String,
      enc: AgnosticEncoder[_],
      nullable: Boolean,
      metadata: Metadata,
      readMethod: Option[String] = None,
      writeMethod: Option[String] = None) {
    def structField: StructField = StructField(name, enc.dataType, nullable, metadata)
  }

  // Contains a sequence of fields.
  trait StructEncoder[K] extends AgnosticEncoder[K] {
    val fields: Seq[EncoderField]
    override def isPrimitive: Boolean = false
    override def schema: StructType = StructType(fields.map(_.structField))
    override def dataType: DataType = schema
    override val isStruct: Boolean = true
  }

  // This supports both Product and DefinedByConstructorParams
  case class ProductEncoder[K](
      override val clsTag: ClassTag[K],
      override val fields: Seq[EncoderField],
      outerPointerGetter: Option[() => AnyRef]) extends StructEncoder[K]

  object ProductEncoder {
    val cachedCls = new ConcurrentHashMap[Int, Class[_]]
    private[sql] def tuple(encoders: Seq[AgnosticEncoder[_]]): AgnosticEncoder[_] = {
      val fields = encoders.zipWithIndex.map {
        case (e, id) => EncoderField(s"_${id + 1}", e, e.nullable, Metadata.empty)
      }
      val cls = cachedCls.computeIfAbsent(encoders.size,
        _ => SparkClassUtils.getContextOrSparkClassLoader.loadClass(s"scala.Tuple${encoders.size}"))
      ProductEncoder[Any](ClassTag(cls), fields, None)
    }

    private[sql] def isTuple(tag: ClassTag[_]): Boolean = {
      tag.runtimeClass.getName.startsWith("scala.Tuple")
    }
  }

  abstract class BaseRowEncoder extends StructEncoder[Row] {
    override def clsTag: ClassTag[Row] = classTag[Row]
  }

  case class RowEncoder(override val fields: Seq[EncoderField]) extends BaseRowEncoder

  object UnboundRowEncoder extends BaseRowEncoder {
    override val schema: StructType = new StructType()
    override val fields: Seq[EncoderField] = Seq.empty
}

  case class JavaBeanEncoder[K](
      override val clsTag: ClassTag[K],
      override val fields: Seq[EncoderField])
    extends StructEncoder[K]

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
  abstract class PrimitiveLeafEncoder[E : ClassTag](dataType: DataType)
    extends LeafEncoder[E](dataType)
  case object PrimitiveBooleanEncoder extends PrimitiveLeafEncoder[Boolean](BooleanType)
  case object PrimitiveByteEncoder extends PrimitiveLeafEncoder[Byte](ByteType)
  case object PrimitiveShortEncoder extends PrimitiveLeafEncoder[Short](ShortType)
  case object PrimitiveIntEncoder extends PrimitiveLeafEncoder[Int](IntegerType)
  case object PrimitiveLongEncoder extends PrimitiveLeafEncoder[Long](LongType)
  case object PrimitiveFloatEncoder extends PrimitiveLeafEncoder[Float](FloatType)
  case object PrimitiveDoubleEncoder extends PrimitiveLeafEncoder[Double](DoubleType)

  // Primitive wrapper encoders.
  abstract class BoxedLeafEncoder[E : ClassTag, P](
      dataType: DataType,
      val primitive: PrimitiveLeafEncoder[P])
    extends LeafEncoder[E](dataType)
  case object BoxedBooleanEncoder
    extends BoxedLeafEncoder[java.lang.Boolean, Boolean](BooleanType, PrimitiveBooleanEncoder)
  case object BoxedByteEncoder
    extends BoxedLeafEncoder[java.lang.Byte, Byte](ByteType, PrimitiveByteEncoder)
  case object BoxedShortEncoder
    extends BoxedLeafEncoder[java.lang.Short, Short](ShortType, PrimitiveShortEncoder)
  case object BoxedIntEncoder
    extends BoxedLeafEncoder[java.lang.Integer, Int](IntegerType, PrimitiveIntEncoder)
  case object BoxedLongEncoder
    extends BoxedLeafEncoder[java.lang.Long, Long](LongType, PrimitiveLongEncoder)
  case object BoxedFloatEncoder
    extends BoxedLeafEncoder[java.lang.Float, Float](FloatType, PrimitiveFloatEncoder)
  case object BoxedDoubleEncoder
    extends BoxedLeafEncoder[java.lang.Double, Double](DoubleType, PrimitiveDoubleEncoder)

  // Nullable leaf encoders
  case object NullEncoder extends LeafEncoder[java.lang.Void](NullType)
  case object StringEncoder extends LeafEncoder[String](StringType)
  case object BinaryEncoder extends LeafEncoder[Array[Byte]](BinaryType)
  case object ScalaBigIntEncoder extends LeafEncoder[BigInt](DecimalType.BigIntDecimal)
  case object JavaBigIntEncoder extends LeafEncoder[JBigInt](DecimalType.BigIntDecimal)
  case object CalendarIntervalEncoder extends LeafEncoder[CalendarInterval](CalendarIntervalType)
  case object DayTimeIntervalEncoder extends LeafEncoder[Duration](DayTimeIntervalType())
  case object YearMonthIntervalEncoder extends LeafEncoder[Period](YearMonthIntervalType())
  case object VariantEncoder extends LeafEncoder[VariantVal](VariantType)
  case class DateEncoder(override val lenientSerialization: Boolean)
    extends LeafEncoder[jsql.Date](DateType)
  case class LocalDateEncoder(override val lenientSerialization: Boolean)
    extends LeafEncoder[LocalDate](DateType)
  case class TimestampEncoder(override val lenientSerialization: Boolean)
    extends LeafEncoder[jsql.Timestamp](TimestampType)
  case class InstantEncoder(override val lenientSerialization: Boolean)
    extends LeafEncoder[Instant](TimestampType)
  case object LocalDateTimeEncoder extends LeafEncoder[LocalDateTime](TimestampNTZType)

  case class SparkDecimalEncoder(dt: DecimalType) extends LeafEncoder[Decimal](dt)
  case class ScalaDecimalEncoder(dt: DecimalType) extends LeafEncoder[BigDecimal](dt)
  case class JavaDecimalEncoder(dt: DecimalType, override val lenientSerialization: Boolean)
    extends LeafEncoder[JBigDecimal](dt)

  val STRICT_DATE_ENCODER: DateEncoder = DateEncoder(lenientSerialization = false)
  val STRICT_LOCAL_DATE_ENCODER: LocalDateEncoder = LocalDateEncoder(lenientSerialization = false)
  val STRICT_TIMESTAMP_ENCODER: TimestampEncoder = TimestampEncoder(lenientSerialization = false)
  val STRICT_INSTANT_ENCODER: InstantEncoder = InstantEncoder(lenientSerialization = false)
  val LENIENT_DATE_ENCODER: DateEncoder = DateEncoder(lenientSerialization = true)
  val LENIENT_LOCAL_DATE_ENCODER: LocalDateEncoder = LocalDateEncoder(lenientSerialization = true)
  val LENIENT_TIMESTAMP_ENCODER: TimestampEncoder = TimestampEncoder(lenientSerialization = true)
  val LENIENT_INSTANT_ENCODER: InstantEncoder = InstantEncoder(lenientSerialization = true)

  val DEFAULT_SPARK_DECIMAL_ENCODER: SparkDecimalEncoder =
    SparkDecimalEncoder(DecimalType.SYSTEM_DEFAULT)
  val DEFAULT_SCALA_DECIMAL_ENCODER: ScalaDecimalEncoder =
    ScalaDecimalEncoder(DecimalType.SYSTEM_DEFAULT)
  val DEFAULT_JAVA_DECIMAL_ENCODER: JavaDecimalEncoder =
    JavaDecimalEncoder(DecimalType.SYSTEM_DEFAULT, lenientSerialization = false)
}

