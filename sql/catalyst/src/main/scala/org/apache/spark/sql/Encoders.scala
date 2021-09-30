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

import java.lang.reflect.Modifier

import scala.reflect.{classTag, ClassTag}
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast}
import org.apache.spark.sql.catalyst.expressions.objects.{DecodeUsingSerializer, EncodeUsingSerializer}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._

/**
 * Methods for creating an [[Encoder]].
 *
 * @since 1.6.0
 */
object Encoders {

  /**
   * An encoder for nullable boolean type.
   * The Scala primitive encoder is available as [[scalaBoolean]].
   * @since 1.6.0
   */
  def BOOLEAN: Encoder[java.lang.Boolean] = ExpressionEncoder()

  /**
   * An encoder for nullable byte type.
   * The Scala primitive encoder is available as [[scalaByte]].
   * @since 1.6.0
   */
  def BYTE: Encoder[java.lang.Byte] = ExpressionEncoder()

  /**
   * An encoder for nullable short type.
   * The Scala primitive encoder is available as [[scalaShort]].
   * @since 1.6.0
   */
  def SHORT: Encoder[java.lang.Short] = ExpressionEncoder()

  /**
   * An encoder for nullable int type.
   * The Scala primitive encoder is available as [[scalaInt]].
   * @since 1.6.0
   */
  def INT: Encoder[java.lang.Integer] = ExpressionEncoder()

  /**
   * An encoder for nullable long type.
   * The Scala primitive encoder is available as [[scalaLong]].
   * @since 1.6.0
   */
  def LONG: Encoder[java.lang.Long] = ExpressionEncoder()

  /**
   * An encoder for nullable float type.
   * The Scala primitive encoder is available as [[scalaFloat]].
   * @since 1.6.0
   */
  def FLOAT: Encoder[java.lang.Float] = ExpressionEncoder()

  /**
   * An encoder for nullable double type.
   * The Scala primitive encoder is available as [[scalaDouble]].
   * @since 1.6.0
   */
  def DOUBLE: Encoder[java.lang.Double] = ExpressionEncoder()

  /**
   * An encoder for nullable string type.
   *
   * @since 1.6.0
   */
  def STRING: Encoder[java.lang.String] = ExpressionEncoder()

  /**
   * An encoder for nullable decimal type.
   *
   * @since 1.6.0
   */
  def DECIMAL: Encoder[java.math.BigDecimal] = ExpressionEncoder()

  /**
   * An encoder for nullable date type.
   *
   * @since 1.6.0
   */
  def DATE: Encoder[java.sql.Date] = ExpressionEncoder()

  /**
   * Creates an encoder that serializes instances of the `java.time.LocalDate` class
   * to the internal representation of nullable Catalyst's DateType.
   *
   * @since 3.0.0
   */
  def LOCALDATE: Encoder[java.time.LocalDate] = ExpressionEncoder()

  /**
   * Creates an encoder that serializes instances of the `java.time.LocalDateTime` class
   * to the internal representation of nullable Catalyst's TimestampNTZType.
   *
   * @since 3.3.0
   */
  def LOCALDATETIME: Encoder[java.time.LocalDateTime] = ExpressionEncoder()

  /**
   * An encoder for nullable timestamp type.
   *
   * @since 1.6.0
   */
  def TIMESTAMP: Encoder[java.sql.Timestamp] = ExpressionEncoder()

  /**
   * Creates an encoder that serializes instances of the `java.time.Instant` class
   * to the internal representation of nullable Catalyst's TimestampType.
   *
   * @since 3.0.0
   */
  def INSTANT: Encoder[java.time.Instant] = ExpressionEncoder()

  /**
   * An encoder for arrays of bytes.
   *
   * @since 1.6.1
   */
  def BINARY: Encoder[Array[Byte]] = ExpressionEncoder()

  /**
   * Creates an encoder that serializes instances of the `java.time.Duration` class
   * to the internal representation of nullable Catalyst's DayTimeIntervalType.
   *
   * @since 3.2.0
   */
  def DURATION: Encoder[java.time.Duration] = ExpressionEncoder()

  /**
   * Creates an encoder that serializes instances of the `java.time.Period` class
   * to the internal representation of nullable Catalyst's YearMonthIntervalType.
   *
   * @since 3.2.0
   */
  def PERIOD: Encoder[java.time.Period] = ExpressionEncoder()

  /**
   * Creates an encoder for Java Bean of type T.
   *
   * T must be publicly accessible.
   *
   * supported types for java bean field:
   *  - primitive types: boolean, int, double, etc.
   *  - boxed types: Boolean, Integer, Double, etc.
   *  - String
   *  - java.math.BigDecimal, java.math.BigInteger
   *  - time related: java.sql.Date, java.sql.Timestamp, java.time.LocalDate, java.time.Instant
   *  - collection types: array, java.util.List, and map
   *  - nested java bean.
   *
   * @since 1.6.0
   */
  def bean[T](beanClass: Class[T]): Encoder[T] = ExpressionEncoder.javaBean(beanClass)

  /**
   * (Scala-specific) Creates an encoder that serializes objects of type T using Kryo.
   * This encoder maps T into a single byte array (binary) field.
   *
   * T must be publicly accessible.
   *
   * @since 1.6.0
   */
  def kryo[T: ClassTag]: Encoder[T] = genericSerializer(useKryo = true)

  /**
   * Creates an encoder that serializes objects of type T using Kryo.
   * This encoder maps T into a single byte array (binary) field.
   *
   * T must be publicly accessible.
   *
   * @since 1.6.0
   */
  def kryo[T](clazz: Class[T]): Encoder[T] = kryo(ClassTag[T](clazz))

  /**
   * (Scala-specific) Creates an encoder that serializes objects of type T using generic Java
   * serialization. This encoder maps T into a single byte array (binary) field.
   *
   * T must be publicly accessible.
   *
   * @note This is extremely inefficient and should only be used as the last resort.
   *
   * @since 1.6.0
   */
  def javaSerialization[T: ClassTag]: Encoder[T] = genericSerializer(useKryo = false)

  /**
   * Creates an encoder that serializes objects of type T using generic Java serialization.
   * This encoder maps T into a single byte array (binary) field.
   *
   * T must be publicly accessible.
   *
   * @note This is extremely inefficient and should only be used as the last resort.
   *
   * @since 1.6.0
   */
  def javaSerialization[T](clazz: Class[T]): Encoder[T] = javaSerialization(ClassTag[T](clazz))

  /** Throws an exception if T is not a public class. */
  private def validatePublicClass[T: ClassTag](): Unit = {
    if (!Modifier.isPublic(classTag[T].runtimeClass.getModifiers)) {
      throw QueryExecutionErrors.notPublicClassError(classTag[T].runtimeClass.getName)
    }
  }

  /** A way to construct encoders using generic serializers. */
  private def genericSerializer[T: ClassTag](useKryo: Boolean): Encoder[T] = {
    if (classTag[T].runtimeClass.isPrimitive) {
      throw QueryExecutionErrors.primitiveTypesNotSupportedError()
    }

    validatePublicClass[T]()

    ExpressionEncoder[T](
      objSerializer =
        EncodeUsingSerializer(
          BoundReference(0, ObjectType(classOf[AnyRef]), nullable = true), kryo = useKryo),
      objDeserializer =
        DecodeUsingSerializer[T](
          Cast(GetColumnByOrdinal(0, BinaryType), BinaryType),
          classTag[T],
          kryo = useKryo),
      clsTag = classTag[T]
    )
  }

  /**
   * An encoder for 2-ary tuples.
   *
   * @since 1.6.0
   */
  def tuple[T1, T2](
    e1: Encoder[T1],
    e2: Encoder[T2]): Encoder[(T1, T2)] = {
    ExpressionEncoder.tuple(encoderFor(e1), encoderFor(e2))
  }

  /**
   * An encoder for 3-ary tuples.
   *
   * @since 1.6.0
   */
  def tuple[T1, T2, T3](
    e1: Encoder[T1],
    e2: Encoder[T2],
    e3: Encoder[T3]): Encoder[(T1, T2, T3)] = {
    ExpressionEncoder.tuple(encoderFor(e1), encoderFor(e2), encoderFor(e3))
  }

  /**
   * An encoder for 4-ary tuples.
   *
   * @since 1.6.0
   */
  def tuple[T1, T2, T3, T4](
    e1: Encoder[T1],
    e2: Encoder[T2],
    e3: Encoder[T3],
    e4: Encoder[T4]): Encoder[(T1, T2, T3, T4)] = {
    ExpressionEncoder.tuple(encoderFor(e1), encoderFor(e2), encoderFor(e3), encoderFor(e4))
  }

  /**
   * An encoder for 5-ary tuples.
   *
   * @since 1.6.0
   */
  def tuple[T1, T2, T3, T4, T5](
    e1: Encoder[T1],
    e2: Encoder[T2],
    e3: Encoder[T3],
    e4: Encoder[T4],
    e5: Encoder[T5]): Encoder[(T1, T2, T3, T4, T5)] = {
    ExpressionEncoder.tuple(
      encoderFor(e1), encoderFor(e2), encoderFor(e3), encoderFor(e4), encoderFor(e5))
  }

  /**
   * An encoder for Scala's product type (tuples, case classes, etc).
   * @since 2.0.0
   */
  def product[T <: Product : TypeTag]: Encoder[T] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive int type.
   * @since 2.0.0
   */
  def scalaInt: Encoder[Int] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive long type.
   * @since 2.0.0
   */
  def scalaLong: Encoder[Long] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive double type.
   * @since 2.0.0
   */
  def scalaDouble: Encoder[Double] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive float type.
   * @since 2.0.0
   */
  def scalaFloat: Encoder[Float] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive byte type.
   * @since 2.0.0
   */
  def scalaByte: Encoder[Byte] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive short type.
   * @since 2.0.0
   */
  def scalaShort: Encoder[Short] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive boolean type.
   * @since 2.0.0
   */
  def scalaBoolean: Encoder[Boolean] = ExpressionEncoder()

}
