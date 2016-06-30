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

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
 * A collection of implicit methods for converting common Scala objects into [[Dataset]]s.
 *
 * @since 1.6.0
 */
abstract class SQLImplicits {

  protected def _sqlContext: SQLContext

  /**
   * Converts $"col name" into a [[Column]].
   *
   * @since 2.0.0
   */
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  /** @since 1.6.0 */
  implicit def newProductEncoder[T <: Product : TypeTag]: Encoder[T] = Encoders.product[T]

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

  /** @since 1.6.1 */
  implicit def newIntSeqEncoder: Encoder[Seq[Int]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newLongSeqEncoder: Encoder[Seq[Long]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newDoubleSeqEncoder: Encoder[Seq[Double]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newFloatSeqEncoder: Encoder[Seq[Float]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newByteSeqEncoder: Encoder[Seq[Byte]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newShortSeqEncoder: Encoder[Seq[Short]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newBooleanSeqEncoder: Encoder[Seq[Boolean]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newStringSeqEncoder: Encoder[Seq[String]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newProductSeqEncoder[A <: Product : TypeTag]: Encoder[Seq[A]] = ExpressionEncoder()

  // Arrays

  /** @since 1.6.1 */
  implicit def newIntArrayEncoder: Encoder[Array[Int]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newLongArrayEncoder: Encoder[Array[Long]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newDoubleArrayEncoder: Encoder[Array[Double]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newFloatArrayEncoder: Encoder[Array[Float]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newByteArrayEncoder: Encoder[Array[Byte]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newShortArrayEncoder: Encoder[Array[Short]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newBooleanArrayEncoder: Encoder[Array[Boolean]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newStringArrayEncoder: Encoder[Array[String]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newProductArrayEncoder[A <: Product : TypeTag]: Encoder[Array[A]] =
    ExpressionEncoder()

  /**
   * Creates a [[Dataset]] from an RDD.
   *
   * @since 1.6.0
   */
  implicit def rddToDatasetHolder[T : Encoder](rdd: RDD[T]): DatasetHolder[T] = {
    DatasetHolder(_sqlContext.createDataset(rdd))
  }

  /**
   * Creates a [[Dataset]] from a local Seq.
   * @since 1.6.0
   */
  implicit def localSeqToDatasetHolder[T : Encoder](s: Seq[T]): DatasetHolder[T] = {
    DatasetHolder(_sqlContext.createDataset(s))
  }

  /**
   * An implicit conversion that turns a Scala `Symbol` into a [[Column]].
   * @since 1.3.0
   */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

}
