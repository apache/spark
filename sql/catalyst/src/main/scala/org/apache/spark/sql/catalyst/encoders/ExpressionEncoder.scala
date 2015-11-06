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

import org.apache.spark.sql.catalyst.analysis.{SimpleAnalyzer, UnresolvedExtractValue, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.util.Utils

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{typeTag, TypeTag}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, DataType, ObjectType, StructType}

/**
 * A factory for constructing encoders that convert objects and primitves to and from the
 * internal row format using catalyst expressions and code generation.  By default, the
 * expressions used to retrieve values from an input row when producing an object will be created as
 * follows:
 *  - Classes will have their sub fields extracted by name using [[UnresolvedAttribute]] expressions
 *    and [[UnresolvedExtractValue]] expressions.
 *  - Tuples will have their subfields extracted by position using [[BoundReference]] expressions.
 *  - Primitives will have their values extracted from the first ordinal with a schema that defaults
 *    to the name `value`.
 */
object ExpressionEncoder {
  def apply[T : TypeTag](flat: Boolean = false): ExpressionEncoder[T] = {
    // We convert the not-serializable TypeTag into StructType and ClassTag.
    val mirror = typeTag[T].mirror
    val cls = mirror.runtimeClass(typeTag[T].tpe)

    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val extractExpression = ScalaReflection.extractorsFor[T](inputObject)
    val constructExpression = ScalaReflection.constructorFor[T]

    new ExpressionEncoder[T](
      extractExpression.dataType,
      flat,
      extractExpression.flatten,
      constructExpression,
      ClassTag[T](cls))
  }

  def tuple[T1, T2](enc1: Encoder[T1], enc2: Encoder[T2]): Encoder[(T1, T2)] = {
    tuple(Seq(enc1, enc2).map(_.asInstanceOf[ExpressionEncoder[_]]))
      .asInstanceOf[ExpressionEncoder[(T1, T2)]]
  }

  def tuple[T1, T2, T3](
      enc1: Encoder[T1],
      enc2: Encoder[T2],
      enc3: Encoder[T3]): Encoder[(T1, T2, T3)] = {
    tuple(Seq(enc1, enc2, enc3).map(_.asInstanceOf[ExpressionEncoder[_]]))
      .asInstanceOf[ExpressionEncoder[(T1, T2, T3)]]
  }

  def tuple[T1, T2, T3, T4](
      enc1: Encoder[T1],
      enc2: Encoder[T2],
      enc3: Encoder[T3],
      enc4: Encoder[T4]): Encoder[(T1, T2, T3, T4)] = {
    tuple(Seq(enc1, enc2, enc3, enc4).map(_.asInstanceOf[ExpressionEncoder[_]]))
      .asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4)]]
  }

  def tuple[T1, T2, T3, T4, T5](
      enc1: Encoder[T1],
      enc2: Encoder[T2],
      enc3: Encoder[T3],
      enc4: Encoder[T4],
      enc5: Encoder[T5]): Encoder[(T1, T2, T3, T4, T5)] = {
    tuple(Seq(enc1, enc2, enc3, enc4, enc5).map(_.asInstanceOf[ExpressionEncoder[_]]))
      .asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4, T5)]]
  }

  def tuple(encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[_] = {
    assert(encoders.length > 1)

    val schema = StructType(encoders.zipWithIndex.map {
      case (e, i) => StructField(s"_${i + 1}", if (e.flat) e.schema.head.dataType else e.schema)
    })

    val cls = Utils.getContextOrSparkClassLoader.loadClass(s"scala.Tuple${encoders.size}")

    val extractExpressions = encoders.map {
      case e if e.flat => e.extractExpressions.head
      case other => CreateStruct(other.extractExpressions)
    }.zipWithIndex.map { case (expr, index) =>
      expr.transformUp {
        case BoundReference(0, t: ObjectType, _) =>
          Invoke(
            BoundReference(0, ObjectType(cls), true),
            s"_${index + 1}",
            t)
      }
    }

    val constructExpressions = encoders.zipWithIndex.map { case (enc, index) =>
      if (enc.flat) {
        enc.constructExpression.transform {
          case b: BoundReference => b.copy(ordinal = index)
        }
      } else {
        enc.constructExpression.transformUp {
          case InputInternalRow(schema) => BoundReference(index, schema, true)
          case BoundReference(ordinal, dt, _) =>
            GetInternalRowField(BoundReference(index, enc.schema, true), ordinal, dt)
        }
      }
    }

    val constructExpression =
      NewInstance(cls, constructExpressions, false, ObjectType(cls))

    new ExpressionEncoder[Any](
      schema,
      flat = false,
      extractExpressions,
      constructExpression,
      ClassTag.apply(cls))
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
case class ExpressionEncoder[T](
    schema: StructType,
    flat: Boolean,
    extractExpressions: Seq[Expression],
    constructExpression: Expression,
    clsTag: ClassTag[T])
  extends Encoder[T] {

  if (flat) require(extractExpressions.size == 1)

  @transient
  private lazy val extractProjection = GenerateUnsafeProjection.generate(extractExpressions)
  private val inputRow = new GenericMutableRow(1)

  @transient
  private lazy val constructProjection = GenerateSafeProjection.generate(constructExpression :: Nil)

  /**
   * Returns an encoded version of `t` as a Spark SQL row.  Note that multiple calls to
   * toRow are allowed to return the same actual [[InternalRow]] object.  Thus, the caller should
   * copy the result before making another call if required.
   */
  def toRow(t: T): InternalRow = {
    inputRow(0) = t
    extractProjection(inputRow)
  }

  /**
   * Returns an object of type `T`, extracting the required values from the provided row.  Note that
   * you must `resolve` and `bind` an encoder to a specific schema before you can call this
   * function.
   */
  def fromRow(row: InternalRow): T = try {
    constructProjection(row).get(0, ObjectType(clsTag.runtimeClass)).asInstanceOf[T]
  } catch {
    case e: Exception =>
      throw new RuntimeException(s"Error while decoding: $e\n${constructExpression.treeString}", e)
  }
}
