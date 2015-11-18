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

import scala.reflect.{ClassTag, classTag}

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.{DeserializeWithKryo, BoundReference, SerializeWithKryo}
import org.apache.spark.sql.types._

/**
 * Used to convert a JVM object of type `T` to and from the internal Spark SQL representation.
 *
 * Encoders are not intended to be thread-safe and thus they are allow to avoid internal locking
 * and reuse internal buffers to improve performance.
 */
trait Encoder[T] extends Serializable {

  /** Returns the schema of encoding this type of object as a Row. */
  def schema: StructType

  /** A ClassTag that can be used to construct and Array to contain a collection of `T`. */
  def clsTag: ClassTag[T]
}

/**
 * Methods for creating encoders.
 */
object Encoders {

  /**
   * (Scala-specific) Creates an encoder that serializes objects of type T using Kryo.
   * This encoder maps T into a single byte array (binary) field.
   */
  def kryo[T: ClassTag]: Encoder[T] = {
    val ser = SerializeWithKryo(BoundReference(0, ObjectType(classOf[AnyRef]), nullable = true))
    val deser = DeserializeWithKryo[T](BoundReference(0, BinaryType, nullable = true), classTag[T])
    ExpressionEncoder[T](
      schema = new StructType().add("value", BinaryType),
      flat = true,
      toRowExpressions = Seq(ser),
      fromRowExpression = deser,
      clsTag = classTag[T]
    )
  }

  /**
   * Creates an encoder that serializes objects of type T using Kryo.
   * This encoder maps T into a single byte array (binary) field.
   */
  def kryo[T](clazz: Class[T]): Encoder[T] = kryo(ClassTag[T](clazz))

  def BOOLEAN: Encoder[java.lang.Boolean] = ExpressionEncoder(flat = true)
  def BYTE: Encoder[java.lang.Byte] = ExpressionEncoder(flat = true)
  def SHORT: Encoder[java.lang.Short] = ExpressionEncoder(flat = true)
  def INT: Encoder[java.lang.Integer] = ExpressionEncoder(flat = true)
  def LONG: Encoder[java.lang.Long] = ExpressionEncoder(flat = true)
  def FLOAT: Encoder[java.lang.Float] = ExpressionEncoder(flat = true)
  def DOUBLE: Encoder[java.lang.Double] = ExpressionEncoder(flat = true)
  def STRING: Encoder[java.lang.String] = ExpressionEncoder(flat = true)

  def tuple[T1, T2](
      e1: Encoder[T1],
      e2: Encoder[T2]): Encoder[(T1, T2)] = {
    ExpressionEncoder.tuple(encoderFor(e1), encoderFor(e2))
  }

  def tuple[T1, T2, T3](
      e1: Encoder[T1],
      e2: Encoder[T2],
      e3: Encoder[T3]): Encoder[(T1, T2, T3)] = {
    ExpressionEncoder.tuple(encoderFor(e1), encoderFor(e2), encoderFor(e3))
  }

  def tuple[T1, T2, T3, T4](
      e1: Encoder[T1],
      e2: Encoder[T2],
      e3: Encoder[T3],
      e4: Encoder[T4]): Encoder[(T1, T2, T3, T4)] = {
    ExpressionEncoder.tuple(encoderFor(e1), encoderFor(e2), encoderFor(e3), encoderFor(e4))
  }

  def tuple[T1, T2, T3, T4, T5](
      e1: Encoder[T1],
      e2: Encoder[T2],
      e3: Encoder[T3],
      e4: Encoder[T4],
      e5: Encoder[T5]): Encoder[(T1, T2, T3, T4, T5)] = {
    ExpressionEncoder.tuple(
      encoderFor(e1), encoderFor(e2), encoderFor(e3), encoderFor(e4), encoderFor(e5))
  }
}
