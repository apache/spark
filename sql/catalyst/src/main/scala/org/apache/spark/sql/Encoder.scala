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

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{ObjectType, StructField, StructType}
import org.apache.spark.util.Utils

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

object Encoders {
  def BOOLEAN: Encoder[java.lang.Boolean] = ExpressionEncoder(flat = true)
  def BYTE: Encoder[java.lang.Byte] = ExpressionEncoder(flat = true)
  def SHORT: Encoder[java.lang.Short] = ExpressionEncoder(flat = true)
  def INT: Encoder[java.lang.Integer] = ExpressionEncoder(flat = true)
  def LONG: Encoder[java.lang.Long] = ExpressionEncoder(flat = true)
  def FLOAT: Encoder[java.lang.Float] = ExpressionEncoder(flat = true)
  def DOUBLE: Encoder[java.lang.Double] = ExpressionEncoder(flat = true)
  def STRING: Encoder[java.lang.String] = ExpressionEncoder(flat = true)

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

  private def tuple(encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[_] = {
    assert(encoders.length > 1)
    // make sure all encoders are resolved, i.e. `Attribute` has been resolved to `BoundReference`.
    assert(encoders.forall(_.fromRowExpression.find(_.isInstanceOf[Attribute]).isEmpty))

    val schema = StructType(encoders.zipWithIndex.map {
      case (e, i) => StructField(s"_${i + 1}", if (e.flat) e.schema.head.dataType else e.schema)
    })

    val cls = Utils.getContextOrSparkClassLoader.loadClass(s"scala.Tuple${encoders.size}")

    val extractExpressions = encoders.map {
      case e if e.flat => e.toRowExpressions.head
      case other => CreateStruct(other.toRowExpressions)
    }.zipWithIndex.map { case (expr, index) =>
      expr.transformUp {
        case BoundReference(0, t: ObjectType, _) =>
          Invoke(
            BoundReference(0, ObjectType(cls), nullable = true),
            s"_${index + 1}",
            t)
      }
    }

    val constructExpressions = encoders.zipWithIndex.map { case (enc, index) =>
      if (enc.flat) {
        enc.fromRowExpression.transform {
          case b: BoundReference => b.copy(ordinal = index)
        }
      } else {
        enc.fromRowExpression.transformUp {
          case BoundReference(ordinal, dt, _) =>
            GetInternalRowField(BoundReference(index, enc.schema, nullable = true), ordinal, dt)
        }
      }
    }

    val constructExpression =
      NewInstance(cls, constructExpressions, propagateNull = false, ObjectType(cls))

    new ExpressionEncoder[Any](
      schema,
      flat = false,
      extractExpressions,
      constructExpression,
      ClassTag(cls))
  }
}
