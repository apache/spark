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
import scala.reflect.runtime.universe.{typeTag, TypeTag}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.{Literal, CreateNamedStruct, BoundReference}
import org.apache.spark.sql.catalyst.ScalaReflection

object FlatEncoder {
  import ScalaReflection.schemaFor
  import ScalaReflection.dataTypeFor

  def apply[T : TypeTag]: ExpressionEncoder[T] = {
    // We convert the not-serializable TypeTag into StructType and ClassTag.
    val tpe = typeTag[T].tpe
    val mirror = typeTag[T].mirror
    val cls = mirror.runtimeClass(tpe)
    assert(!schemaFor(tpe).dataType.isInstanceOf[StructType])

    val input = BoundReference(0, dataTypeFor(tpe), nullable = true)
    val toRowExpression = CreateNamedStruct(
      Literal("value") :: ProductEncoder.extractorFor(input, tpe) :: Nil)
    val fromRowExpression = ProductEncoder.constructorFor(tpe)

    new ExpressionEncoder[T](
      toRowExpression.dataType,
      flat = true,
      toRowExpression.flatten,
      fromRowExpression,
      ClassTag[T](cls))
  }
}
