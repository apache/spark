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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{typeTag, TypeTag}

import org.apache.spark.sql.catalyst.{ScalaReflection, InternalRow}
import org.apache.spark.sql.types.{ObjectType, StructType}

/**
 * A factory for constructing encoders that convert Scala's product type to/from the Spark SQL
 * internal binary representation.
 */
object ProductEncoder {
  def apply[T <: Product : TypeTag]: Encoder[T] = {
    // We convert the not-serializable TypeTag into StructType and ClassTag.
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    val mirror = typeTag[T].mirror
    val cls = mirror.runtimeClass(typeTag[T].tpe)

    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val extractExpressions = ScalaReflection.extractorsFor[T](inputObject)
    new ClassEncoder[T](schema, extractExpressions, ClassTag[T](cls))
  }
}

/**
 * A generic encoder for JVM objects.
 *
 * @param schema The schema after converting `T` to a Spark SQL row.
 * @param extractExpressions A set of expressions, one for each top-level field that can be used to
 *                           extract the values from a raw object.
 * @param clsTag A classtag for `T`.
 */
case class ClassEncoder[T](
    schema: StructType,
    extractExpressions: Seq[Expression],
    clsTag: ClassTag[T])
  extends Encoder[T] {

  private val extractProjection = GenerateUnsafeProjection.generate(extractExpressions)
  private val inputRow = new GenericMutableRow(1)

  override def toRow(t: T): InternalRow = {
    inputRow(0) = t
    extractProjection(inputRow)
  }
}
