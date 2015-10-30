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

import scala.reflect.ClassTag

import org.apache.spark.sql.types.StructType

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

object Encoder {
  import scala.reflect.runtime.universe._

  def forBoolean: Encoder[java.lang.Boolean] = ExpressionEncoder(flat = true)
  def forByte: Encoder[java.lang.Byte] = ExpressionEncoder(flat = true)
  def forShort: Encoder[java.lang.Short] = ExpressionEncoder(flat = true)
  def forInt: Encoder[java.lang.Integer] = ExpressionEncoder(flat = true)
  def forLong: Encoder[java.lang.Long] = ExpressionEncoder(flat = true)
  def forFloat: Encoder[java.lang.Float] = ExpressionEncoder(flat = true)
  def forDouble: Encoder[java.lang.Double] = ExpressionEncoder(flat = true)
  def forString: Encoder[java.lang.String] = ExpressionEncoder(flat = true)

  def typeTagOfTuple2[T1 : TypeTag, T2 : TypeTag]: TypeTag[(T1, T2)] = typeTag[(T1, T2)]

  private def getTypeTag[T](c: Class[T]): TypeTag[T] = {
    import scala.reflect.api

    // val mirror = runtimeMirror(c.getClassLoader)
    val mirror = rootMirror
    val sym = mirror.staticClass(c.getName)
    val tpe = sym.selfType
    TypeTag(mirror, new api.TypeCreator {
      def apply[U <: api.Universe with Singleton](m: api.Mirror[U]) =
        if (m eq mirror) tpe.asInstanceOf[U # Type]
        else throw new IllegalArgumentException(
          s"Type tag defined in $mirror cannot be migrated to other mirrors.")
    })
  }

  def forTuple2[T1, T2](c1: Class[T1], c2: Class[T2]): Encoder[(T1, T2)] = {
    implicit val typeTag1 = getTypeTag(c1)
    implicit val typeTag2 = getTypeTag(c2)
    ExpressionEncoder[(T1, T2)]()
  }
}
