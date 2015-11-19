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

import org.apache.spark.util.Utils
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.{SimpleAnalyzer, UnresolvedExtractValue, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{NullType, StructField, ObjectType, StructType}

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

  /**
   * Given a set of N encoders, constructs a new encoder that produce objects as items in an
   * N-tuple.  Note that these encoders should be unresolved so that information about
   * name/positional binding is preserved.
   */
  def tuple(encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[_] = {
    encoders.foreach(_.assertUnresolved())

    val schema = StructType(encoders.zipWithIndex.map {
      case (e, i) => StructField(s"_${i + 1}", if (e.flat) e.schema.head.dataType else e.schema)
    })

    val cls = Utils.getContextOrSparkClassLoader.loadClass(s"scala.Tuple${encoders.size}")

    val toRowExpressions = encoders.map {
      case e if e.flat => e.toRowExpressions.head
      case other => CreateStruct(other.toRowExpressions)
    }.zipWithIndex.map { case (expr, index) =>
      expr.transformUp {
        case BoundReference(0, t, _) =>
          Invoke(
            BoundReference(0, ObjectType(cls), nullable = true),
            s"_${index + 1}",
            t)
      }
    }

    val fromRowExpressions = encoders.zipWithIndex.map { case (enc, index) =>
      if (enc.flat) {
        enc.fromRowExpression.transform {
          case b: BoundReference => b.copy(ordinal = index)
        }
      } else {
        val input = BoundReference(index, enc.schema, nullable = true)
        enc.fromRowExpression.transformUp {
          case UnresolvedAttribute(nameParts) =>
            assert(nameParts.length == 1)
            UnresolvedExtractValue(input, Literal(nameParts.head))
          case BoundReference(ordinal, dt, _) => GetInternalRowField(input, ordinal, dt)
        }
      }
    }

    val fromRowExpression =
      NewInstance(cls, fromRowExpressions, propagateNull = false, ObjectType(cls))

    new ExpressionEncoder[Any](
      schema,
      flat = false,
      toRowExpressions,
      fromRowExpression,
      ClassTag(cls))
  }

  def tuple[T1, T2](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2]): ExpressionEncoder[(T1, T2)] =
    tuple(Seq(e1, e2)).asInstanceOf[ExpressionEncoder[(T1, T2)]]

  def tuple[T1, T2, T3](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3]): ExpressionEncoder[(T1, T2, T3)] =
    tuple(Seq(e1, e2, e3)).asInstanceOf[ExpressionEncoder[(T1, T2, T3)]]

  def tuple[T1, T2, T3, T4](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3],
      e4: ExpressionEncoder[T4]): ExpressionEncoder[(T1, T2, T3, T4)] =
    tuple(Seq(e1, e2, e3, e4)).asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4)]]

  def tuple[T1, T2, T3, T4, T5](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3],
      e4: ExpressionEncoder[T4],
      e5: ExpressionEncoder[T5]): ExpressionEncoder[(T1, T2, T3, T4, T5)] =
    tuple(Seq(e1, e2, e3, e4, e5)).asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4, T5)]]
}

/**
 * A generic encoder for JVM objects.
 *
 * @param schema The schema after converting `T` to a Spark SQL row.
 * @param toRowExpressions A set of expressions, one for each top-level field that can be used to
 *                           extract the values from a raw object into an [[InternalRow]].
 * @param fromRowExpression An expression that will construct an object given an [[InternalRow]].
 * @param clsTag A classtag for `T`.
 */
case class ExpressionEncoder[T](
    schema: StructType,
    flat: Boolean,
    toRowExpressions: Seq[Expression],
    fromRowExpression: Expression,
    clsTag: ClassTag[T])
  extends Encoder[T] {

  if (flat) require(toRowExpressions.size == 1)

  @transient
  private lazy val extractProjection = GenerateUnsafeProjection.generate(toRowExpressions)

  @transient
  private lazy val inputRow = new GenericMutableRow(1)

  @transient
  private lazy val constructProjection = GenerateSafeProjection.generate(fromRowExpression :: Nil)

  /**
   * Returns an encoded version of `t` as a Spark SQL row.  Note that multiple calls to
   * toRow are allowed to return the same actual [[InternalRow]] object.  Thus, the caller should
   * copy the result before making another call if required.
   */
  def toRow(t: T): InternalRow = try {
    inputRow(0) = t
    extractProjection(inputRow)
  } catch {
    case e: Exception =>
      throw new RuntimeException(
        s"Error while encoding: $e\n${toRowExpressions.map(_.treeString).mkString("\n")}", e)
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
      throw new RuntimeException(s"Error while decoding: $e\n${fromRowExpression.treeString}", e)
  }

  /**
   * The process of resolution to a given schema throws away information about where a given field
   * is being bound by ordinal instead of by name.  This method checks to make sure this process
   * has not been done already in places where we plan to do later composition of encoders.
   */
  def assertUnresolved(): Unit = {
    (fromRowExpression +:  toRowExpressions).foreach(_.foreach {
      case a: AttributeReference =>
        sys.error(s"Unresolved encoder expected, but $a was found.")
      case _ =>
    })
  }

  /**
   * Returns a new copy of this encoder, where the expressions used by `fromRow` are resolved to the
   * given schema.
   */
  def resolve(schema: Seq[Attribute]): ExpressionEncoder[T] = {
    val positionToAttribute = AttributeMap.toIndex(schema)
    val unbound = fromRowExpression transform {
      case b: BoundReference => positionToAttribute(b.ordinal)
    }

    val plan = Project(Alias(unbound, "")() :: Nil, LocalRelation(schema))
    val analyzedPlan = SimpleAnalyzer.execute(plan)
    copy(fromRowExpression = analyzedPlan.expressions.head.children.head)
  }

  /**
   * Returns a copy of this encoder where the expressions used to construct an object from an input
   * row have been bound to the ordinals of the given schema.  Note that you need to first call
   * resolve before bind.
   */
  def bind(schema: Seq[Attribute]): ExpressionEncoder[T] = {
    copy(fromRowExpression = BindReferences.bindReference(fromRowExpression, schema))
  }

  /**
   * Returns a new encoder with input columns shifted by `delta` ordinals
   */
  def shift(delta: Int): ExpressionEncoder[T] = {
    copy(fromRowExpression = fromRowExpression transform {
      case r: BoundReference => r.copy(ordinal = r.ordinal + delta)
    })
  }

  protected val attrs = toRowExpressions.flatMap(_.collect {
    case _: UnresolvedAttribute => ""
    case a: Attribute => s"#${a.exprId}"
    case b: BoundReference => s"[${b.ordinal}]"
  })

  protected val schemaString =
    schema
      .zip(attrs)
      .map { case(f, a) => s"${f.name}$a: ${f.dataType.simpleString}"}.mkString(", ")

  override def toString: String = s"class[$schemaString]"
}
