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

  /**
   * Given a set of N encoders, constructs a new encoder that produce objects as items in an
   * N-tuple.  Note that these encoders should first be bound correctly to the combined input
   * schema.
   */
  def tuple(encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[_] = {
    val schema =
      StructType(
        encoders.zipWithIndex.map { case (e, i) => StructField(s"_${i + 1}", e.schema)})
    val cls = Utils.getContextOrSparkClassLoader.loadClass(s"scala.Tuple${encoders.size}")
    val extractExpressions = encoders.map {
      case e if e.flat => e.extractExpressions.head
      case other => CreateStruct(other.extractExpressions)
    }
    val constructExpression =
      NewInstance(cls, encoders.map(_.constructExpression), false, ObjectType(cls))

    new ExpressionEncoder[Any](
      schema,
      false,
      extractExpressions,
      constructExpression,
      ClassTag.apply(cls))
  }

  /** A helper for producing encoders of Tuple2 from other encoders. */
  def tuple[T1, T2](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2]): ExpressionEncoder[(T1, T2)] =
    tuple(e1 :: e2 :: Nil).asInstanceOf[ExpressionEncoder[(T1, T2)]]
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

  /**
   * Returns a new copy of this encoder, where the expressions used by `fromRow` are resolved to the
   * given schema.
   */
  def resolve(schema: Seq[Attribute]): ExpressionEncoder[T] = {
    val plan = Project(Alias(constructExpression, "")() :: Nil, LocalRelation(schema))
    val analyzedPlan = SimpleAnalyzer.execute(plan)
    copy(constructExpression = analyzedPlan.expressions.head.children.head)
  }

  /**
   * Returns a copy of this encoder where the expressions used to construct an object from an input
   * row have been bound to the ordinals of the given schema.  Note that you need to first call
   * resolve before bind.
   */
  def bind(schema: Seq[Attribute]): ExpressionEncoder[T] = {
    copy(constructExpression = BindReferences.bindReference(constructExpression, schema))
  }

  /**
   * Replaces any bound references in the schema with the attributes at the corresponding ordinal
   * in the provided schema.  This can be used to "relocate" a given encoder to pull values from
   * a different schema than it was initially bound to.  It can also be used to assign attributes
   * to ordinal based extraction (i.e. because the input data was a tuple).
   */
  def unbind(schema: Seq[Attribute]): ExpressionEncoder[T] = {
    val positionToAttribute = AttributeMap.toIndex(schema)
    copy(constructExpression = constructExpression transform {
      case b: BoundReference => positionToAttribute(b.ordinal)
    })
  }

  /**
   * Given an encoder that has already been bound to a given schema, returns a new encoder
   * where the positions are mapped from `oldSchema` to `newSchema`.  This can be used, for example,
   * when you are trying to use an encoder on grouping keys that were originally part of a larger
   * row, but now you have projected out only the key expressions.
   */
  def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): ExpressionEncoder[T] = {
    val positionToAttribute = AttributeMap.toIndex(oldSchema)
    val attributeToNewPosition = AttributeMap.byIndex(newSchema)
    copy(constructExpression = constructExpression transform {
      case r: BoundReference =>
        r.copy(ordinal = attributeToNewPosition(positionToAttribute(r.ordinal)))
    })
  }

  /**
   * Returns a copy of this encoder where the expressions used to create an object given an
   * input row have been modified to pull the object out from a nested struct, instead of the
   * top level fields.
   */
  def nested(input: Expression = BoundReference(0, schema, true)): ExpressionEncoder[T] = {
    copy(constructExpression = constructExpression transform {
      case u: Attribute if u != input =>
        UnresolvedExtractValue(input, Literal(u.name))
      case b: BoundReference if b != input =>
        GetStructField(
          input,
          StructField(s"i[${b.ordinal}]", b.dataType),
          b.ordinal)
    })
  }

  protected val attrs = extractExpressions.flatMap(_.collect {
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
