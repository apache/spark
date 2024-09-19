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
package org.apache.spark.sql.api

import scala.collection.Map
import scala.language.implicitConversions
import scala.reflect.classTag
import scala.reflect.runtime.universe.TypeTag

import _root_.java

import org.apache.spark.sql.{ColumnName, DatasetHolder, Encoder, Encoders}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{ArrayEncoder, DEFAULT_SCALA_DECIMAL_ENCODER, IterableEncoder, PrimitiveBooleanEncoder, PrimitiveByteEncoder, PrimitiveDoubleEncoder, PrimitiveFloatEncoder, PrimitiveIntEncoder, PrimitiveLongEncoder, PrimitiveShortEncoder, StringEncoder}

/**
 * A collection of implicit methods for converting common Scala objects into
 * [[org.apache.spark.sql.api.Dataset]]s.
 *
 * @since 1.6.0
 */
abstract class SQLImplicits[DS[U] <: Dataset[U]]
    extends LowPrioritySQLImplicits
    with Serializable {

  protected def session: SparkSession

  /**
   * Converts $"col name" into a [[org.apache.spark.sql.Column]].
   *
   * @since 2.0.0
   */
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  // Primitives

  /** @since 1.6.0 */
  implicit def newIntEncoder: Encoder[Int] = Encoders.scalaInt

  /** @since 1.6.0 */
  implicit def newLongEncoder: Encoder[Long] = Encoders.scalaLong

  /** @since 1.6.0 */
  implicit def newDoubleEncoder: Encoder[Double] = Encoders.scalaDouble

  /** @since 1.6.0 */
  implicit def newFloatEncoder: Encoder[Float] = Encoders.scalaFloat

  /** @since 1.6.0 */
  implicit def newByteEncoder: Encoder[Byte] = Encoders.scalaByte

  /** @since 1.6.0 */
  implicit def newShortEncoder: Encoder[Short] = Encoders.scalaShort

  /** @since 1.6.0 */
  implicit def newBooleanEncoder: Encoder[Boolean] = Encoders.scalaBoolean

  /** @since 1.6.0 */
  implicit def newStringEncoder: Encoder[String] = Encoders.STRING

  /** @since 2.2.0 */
  implicit def newJavaDecimalEncoder: Encoder[java.math.BigDecimal] = Encoders.DECIMAL

  /** @since 2.2.0 */
  implicit def newScalaDecimalEncoder: Encoder[scala.math.BigDecimal] =
    DEFAULT_SCALA_DECIMAL_ENCODER

  /** @since 2.2.0 */
  implicit def newDateEncoder: Encoder[java.sql.Date] = Encoders.DATE

  /** @since 3.0.0 */
  implicit def newLocalDateEncoder: Encoder[java.time.LocalDate] = Encoders.LOCALDATE

  /** @since 3.4.0 */
  implicit def newLocalDateTimeEncoder: Encoder[java.time.LocalDateTime] = Encoders.LOCALDATETIME

  /** @since 2.2.0 */
  implicit def newTimeStampEncoder: Encoder[java.sql.Timestamp] = Encoders.TIMESTAMP

  /** @since 3.0.0 */
  implicit def newInstantEncoder: Encoder[java.time.Instant] = Encoders.INSTANT

  /** @since 3.2.0 */
  implicit def newDurationEncoder: Encoder[java.time.Duration] = Encoders.DURATION

  /** @since 3.2.0 */
  implicit def newPeriodEncoder: Encoder[java.time.Period] = Encoders.PERIOD

  /** @since 3.2.0 */
  implicit def newJavaEnumEncoder[A <: java.lang.Enum[_]: TypeTag]: Encoder[A] =
    ScalaReflection.encoderFor[A]

  // Boxed primitives

  /** @since 2.0.0 */
  implicit def newBoxedIntEncoder: Encoder[java.lang.Integer] = Encoders.INT

  /** @since 2.0.0 */
  implicit def newBoxedLongEncoder: Encoder[java.lang.Long] = Encoders.LONG

  /** @since 2.0.0 */
  implicit def newBoxedDoubleEncoder: Encoder[java.lang.Double] = Encoders.DOUBLE

  /** @since 2.0.0 */
  implicit def newBoxedFloatEncoder: Encoder[java.lang.Float] = Encoders.FLOAT

  /** @since 2.0.0 */
  implicit def newBoxedByteEncoder: Encoder[java.lang.Byte] = Encoders.BYTE

  /** @since 2.0.0 */
  implicit def newBoxedShortEncoder: Encoder[java.lang.Short] = Encoders.SHORT

  /** @since 2.0.0 */
  implicit def newBoxedBooleanEncoder: Encoder[java.lang.Boolean] = Encoders.BOOLEAN

  // Seqs
  private def newSeqEncoder[E](elementEncoder: AgnosticEncoder[E]): AgnosticEncoder[Seq[E]] = {
    IterableEncoder(
      classTag[Seq[E]],
      elementEncoder,
      elementEncoder.nullable,
      elementEncoder.lenientSerialization)
  }

  /**
   * @since 1.6.1
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newIntSeqEncoder: Encoder[Seq[Int]] = newSeqEncoder(PrimitiveIntEncoder)

  /**
   * @since 1.6.1
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newLongSeqEncoder: Encoder[Seq[Long]] = newSeqEncoder(PrimitiveLongEncoder)

  /**
   * @since 1.6.1
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newDoubleSeqEncoder: Encoder[Seq[Double]] = newSeqEncoder(PrimitiveDoubleEncoder)

  /**
   * @since 1.6.1
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newFloatSeqEncoder: Encoder[Seq[Float]] = newSeqEncoder(PrimitiveFloatEncoder)

  /**
   * @since 1.6.1
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newByteSeqEncoder: Encoder[Seq[Byte]] = newSeqEncoder(PrimitiveByteEncoder)

  /**
   * @since 1.6.1
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newShortSeqEncoder: Encoder[Seq[Short]] = newSeqEncoder(PrimitiveShortEncoder)

  /**
   * @since 1.6.1
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newBooleanSeqEncoder: Encoder[Seq[Boolean]] = newSeqEncoder(PrimitiveBooleanEncoder)

  /**
   * @since 1.6.1
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  val newStringSeqEncoder: Encoder[Seq[String]] = newSeqEncoder(StringEncoder)

  /**
   * @since 1.6.1
   * @deprecated
   *   use [[newSequenceEncoder]]
   */
  @deprecated("Use newSequenceEncoder instead", "2.2.0")
  def newProductSeqEncoder[A <: Product: TypeTag]: Encoder[Seq[A]] =
    newSeqEncoder(ScalaReflection.encoderFor[A])

  /** @since 2.2.0 */
  implicit def newSequenceEncoder[T <: Seq[_]: TypeTag]: Encoder[T] =
    ScalaReflection.encoderFor[T]

  // Maps
  /** @since 2.3.0 */
  implicit def newMapEncoder[T <: Map[_, _]: TypeTag]: Encoder[T] = ScalaReflection.encoderFor[T]

  /**
   * Notice that we serialize `Set` to Catalyst array. The set property is only kept when
   * manipulating the domain objects. The serialization format doesn't keep the set property. When
   * we have a Catalyst array which contains duplicated elements and convert it to
   * `Dataset[Set[T]]` by using the encoder, the elements will be de-duplicated.
   *
   * @since 2.3.0
   */
  implicit def newSetEncoder[T <: Set[_]: TypeTag]: Encoder[T] = ScalaReflection.encoderFor[T]

  // Arrays
  private def newArrayEncoder[E](
      elementEncoder: AgnosticEncoder[E]): AgnosticEncoder[Array[E]] = {
    ArrayEncoder(elementEncoder, elementEncoder.nullable)
  }

  /** @since 1.6.1 */
  implicit val newIntArrayEncoder: Encoder[Array[Int]] = newArrayEncoder(PrimitiveIntEncoder)

  /** @since 1.6.1 */
  implicit val newLongArrayEncoder: Encoder[Array[Long]] = newArrayEncoder(PrimitiveLongEncoder)

  /** @since 1.6.1 */
  implicit val newDoubleArrayEncoder: Encoder[Array[Double]] =
    newArrayEncoder(PrimitiveDoubleEncoder)

  /** @since 1.6.1 */
  implicit val newFloatArrayEncoder: Encoder[Array[Float]] =
    newArrayEncoder(PrimitiveFloatEncoder)

  /** @since 1.6.1 */
  implicit val newByteArrayEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  /** @since 1.6.1 */
  implicit val newShortArrayEncoder: Encoder[Array[Short]] =
    newArrayEncoder(PrimitiveShortEncoder)

  /** @since 1.6.1 */
  implicit val newBooleanArrayEncoder: Encoder[Array[Boolean]] =
    newArrayEncoder(PrimitiveBooleanEncoder)

  /** @since 1.6.1 */
  implicit val newStringArrayEncoder: Encoder[Array[String]] =
    newArrayEncoder(StringEncoder)

  /** @since 1.6.1 */
  implicit def newProductArrayEncoder[A <: Product: TypeTag]: Encoder[Array[A]] =
    newArrayEncoder(ScalaReflection.encoderFor[A])

  /**
   * Creates a [[Dataset]] from a local Seq.
   * @since 1.6.0
   */
  implicit def localSeqToDatasetHolder[T: Encoder](s: Seq[T]): DatasetHolder[T, DS] = {
    new DatasetHolder[T, DS](session.createDataset(s).asInstanceOf[DS[T]])
  }

  /**
   * An implicit conversion that turns a Scala `Symbol` into a [[org.apache.spark.sql.Column]].
   * @since 1.3.0
   */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)
}

/**
 * Lower priority implicit methods for converting Scala objects into
 * [[org.apache.spark.sql.api.Dataset]]s. Conflicting implicits are placed here to disambiguate
 * resolution.
 *
 * Reasons for including specific implicits: newProductEncoder - to disambiguate for `List`s which
 * are both `Seq` and `Product`
 */
trait LowPrioritySQLImplicits {

  /** @since 1.6.0 */
  implicit def newProductEncoder[T <: Product: TypeTag]: Encoder[T] = Encoders.product[T]
}
