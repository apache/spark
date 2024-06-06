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
package org.apache.spark.sql

import scala.collection.Map
import scala.language.implicitConversions
import scala.reflect.classTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticEncoders}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._

/**
 * A collection of implicit methods for converting names and Symbols into [[Column]]s, and for
 * converting common Scala objects into [[Dataset]]s.
 *
 * @since 3.4.0
 */
abstract class SQLImplicits private[sql] (session: SparkSession) extends LowPrioritySQLImplicits {

  /**
   * Converts $"col name" into a [[Column]].
   *
   * @since 3.4.0
   */
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  /**
   * An implicit conversion that turns a Scala `Symbol` into a [[Column]].
   * @since 3.4.0
   */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

  /** @since 3.4.0 */
  implicit val newIntEncoder: Encoder[Int] = PrimitiveIntEncoder

  /** @since 3.4.0 */
  implicit val newLongEncoder: Encoder[Long] = PrimitiveLongEncoder

  /** @since 3.4.0 */
  implicit val newDoubleEncoder: Encoder[Double] = PrimitiveDoubleEncoder

  /** @since 3.4.0 */
  implicit val newFloatEncoder: Encoder[Float] = PrimitiveFloatEncoder

  /** @since 3.4.0 */
  implicit val newByteEncoder: Encoder[Byte] = PrimitiveByteEncoder

  /** @since 3.4.0 */
  implicit val newShortEncoder: Encoder[Short] = PrimitiveShortEncoder

  /** @since 3.4.0 */
  implicit val newBooleanEncoder: Encoder[Boolean] = PrimitiveBooleanEncoder

  /** @since 3.4.0 */
  implicit val newStringEncoder: Encoder[String] = StringEncoder

  /** @since 3.4.0 */
  implicit val newJavaDecimalEncoder: Encoder[java.math.BigDecimal] =
    AgnosticEncoders.DEFAULT_JAVA_DECIMAL_ENCODER

  /** @since 3.4.0 */
  implicit val newScalaDecimalEncoder: Encoder[scala.math.BigDecimal] =
    AgnosticEncoders.DEFAULT_SCALA_DECIMAL_ENCODER

  /** @since 3.4.0 */
  implicit val newDateEncoder: Encoder[java.sql.Date] = AgnosticEncoders.STRICT_DATE_ENCODER

  /** @since 3.4.0 */
  implicit val newLocalDateEncoder: Encoder[java.time.LocalDate] =
    AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER

  /** @since 3.4.0 */
  implicit val newLocalDateTimeEncoder: Encoder[java.time.LocalDateTime] =
    AgnosticEncoders.LocalDateTimeEncoder

  /** @since 3.4.0 */
  implicit val newTimeStampEncoder: Encoder[java.sql.Timestamp] =
    AgnosticEncoders.STRICT_TIMESTAMP_ENCODER

  /** @since 3.4.0 */
  implicit val newInstantEncoder: Encoder[java.time.Instant] =
    AgnosticEncoders.STRICT_INSTANT_ENCODER

  /** @since 3.4.0 */
  implicit val newDurationEncoder: Encoder[java.time.Duration] = DayTimeIntervalEncoder

  /** @since 3.4.0 */
  implicit val newPeriodEncoder: Encoder[java.time.Period] = YearMonthIntervalEncoder

  /** @since 3.4.0 */
  implicit def newJavaEnumEncoder[A <: java.lang.Enum[_]: TypeTag]: Encoder[A] = {
    ScalaReflection.encoderFor[A]
  }

  // Boxed primitives

  /** @since 3.4.0 */
  implicit val newBoxedIntEncoder: Encoder[java.lang.Integer] = BoxedIntEncoder

  /** @since 3.4.0 */
  implicit val newBoxedLongEncoder: Encoder[java.lang.Long] = BoxedLongEncoder

  /** @since 3.4.0 */
  implicit val newBoxedDoubleEncoder: Encoder[java.lang.Double] = BoxedDoubleEncoder

  /** @since 3.4.0 */
  implicit val newBoxedFloatEncoder: Encoder[java.lang.Float] = BoxedFloatEncoder

  /** @since 3.4.0 */
  implicit val newBoxedByteEncoder: Encoder[java.lang.Byte] = BoxedByteEncoder

  /** @since 3.4.0 */
  implicit val newBoxedShortEncoder: Encoder[java.lang.Short] = BoxedShortEncoder

  /** @since 3.4.0 */
  implicit val newBoxedBooleanEncoder: Encoder[java.lang.Boolean] = BoxedBooleanEncoder

  // Seqs
  private def newSeqEncoder[E](elementEncoder: AgnosticEncoder[E]): AgnosticEncoder[Seq[E]] = {
    IterableEncoder(
      classTag[Seq[E]],
      elementEncoder,
      elementEncoder.nullable,
      elementEncoder.lenientSerialization)
  }

  /**
   * @since 3.4.0
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newIntSeqEncoder: Encoder[Seq[Int]] = newSeqEncoder(PrimitiveIntEncoder)

  /**
   * @since 3.4.0
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newLongSeqEncoder: Encoder[Seq[Long]] = newSeqEncoder(PrimitiveLongEncoder)

  /**
   * @since 3.4.0
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newDoubleSeqEncoder: Encoder[Seq[Double]] = newSeqEncoder(PrimitiveDoubleEncoder)

  /**
   * @since 3.4.0
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newFloatSeqEncoder: Encoder[Seq[Float]] = newSeqEncoder(PrimitiveFloatEncoder)

  /**
   * @since 3.4.0
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newByteSeqEncoder: Encoder[Seq[Byte]] = newSeqEncoder(PrimitiveByteEncoder)

  /**
   * @since 3.4.0
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newShortSeqEncoder: Encoder[Seq[Short]] = newSeqEncoder(PrimitiveShortEncoder)

  /**
   * @since 3.4.0
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newBooleanSeqEncoder: Encoder[Seq[Boolean]] = newSeqEncoder(PrimitiveBooleanEncoder)

  /**
   * @since 3.4.0
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newStringSeqEncoder: Encoder[Seq[String]] = newSeqEncoder(StringEncoder)

  /**
   * @since 3.4.0
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  def newProductSeqEncoder[A <: Product: TypeTag]: Encoder[Seq[A]] =
    newSeqEncoder(ScalaReflection.encoderFor[A])

  /** @since 3.4.0 */
  implicit def newSequenceEncoder[T <: Seq[_]: TypeTag]: Encoder[T] =
    ScalaReflection.encoderFor[T]

  // Maps
  /** @since 3.4.0 */
  implicit def newMapEncoder[T <: Map[_, _]: TypeTag]: Encoder[T] = ScalaReflection.encoderFor[T]

  /**
   * Notice that we serialize `Set` to Catalyst array. The set property is only kept when
   * manipulating the domain objects. The serialization format doesn't keep the set property. When
   * we have a Catalyst array which contains duplicated elements and convert it to
   * `Dataset[Set[T]]` by using the encoder, the elements will be de-duplicated.
   *
   * @since 3.4.0
   */
  implicit def newSetEncoder[T <: Set[_]: TypeTag]: Encoder[T] = ScalaReflection.encoderFor[T]

  // Arrays
  private def newArrayEncoder[E](
      elementEncoder: AgnosticEncoder[E]): AgnosticEncoder[Array[E]] = {
    ArrayEncoder(elementEncoder, elementEncoder.nullable)
  }

  /** @since 3.4.0 */
  implicit val newIntArrayEncoder: Encoder[Array[Int]] = newArrayEncoder(PrimitiveIntEncoder)

  /** @since 3.4.0 */
  implicit val newLongArrayEncoder: Encoder[Array[Long]] = newArrayEncoder(PrimitiveLongEncoder)

  /** @since 3.4.0 */
  implicit val newDoubleArrayEncoder: Encoder[Array[Double]] =
    newArrayEncoder(PrimitiveDoubleEncoder)

  /** @since 3.4.0 */
  implicit val newFloatArrayEncoder: Encoder[Array[Float]] = newArrayEncoder(
    PrimitiveFloatEncoder)

  /** @since 3.4.0 */
  implicit val newByteArrayEncoder: Encoder[Array[Byte]] = BinaryEncoder

  /** @since 3.4.0 */
  implicit val newShortArrayEncoder: Encoder[Array[Short]] = newArrayEncoder(
    PrimitiveShortEncoder)

  /** @since 3.4.0 */
  implicit val newBooleanArrayEncoder: Encoder[Array[Boolean]] =
    newArrayEncoder(PrimitiveBooleanEncoder)

  /** @since 3.4.0 */
  implicit val newStringArrayEncoder: Encoder[Array[String]] = newArrayEncoder(StringEncoder)

  /** @since 3.4.0 */
  implicit def newProductArrayEncoder[A <: Product: TypeTag]: Encoder[Array[A]] = {
    newArrayEncoder(ScalaReflection.encoderFor[A])
  }

  /**
   * Creates a [[Dataset]] from a local Seq.
   * @since 3.4.0
   */
  implicit def localSeqToDatasetHolder[T: Encoder](s: Seq[T]): DatasetHolder[T] = {
    DatasetHolder(session.createDataset(s))
  }
}

/**
 * Lower priority implicit methods for converting Scala objects into [[Dataset]]s. Conflicting
 * implicits are placed here to disambiguate resolution.
 *
 * Reasons for including specific implicits: newProductEncoder - to disambiguate for `List`s which
 * are both `Seq` and `Product`
 */
trait LowPrioritySQLImplicits {

  /** @since 3.4.0 */
  implicit def newProductEncoder[T <: Product: TypeTag]: Encoder[T] =
    ScalaReflection.encoderFor[T]
}
