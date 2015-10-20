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

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, SimpleAnalyzer}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{typeTag, TypeTag}

import org.apache.spark.sql.catalyst.{ScalaReflection, InternalRow}
import org.apache.spark.sql.types.{ObjectType, StructType}

/**
 * A factory for constructing encoders that convert Scala's product type to/from the Spark SQL
 * internal binary representation.
 */
object ProductEncoder {
  def apply[T <: Product : TypeTag]: ClassEncoder[T] = {
    // We convert the not-serializable TypeTag into StructType and ClassTag.
    val mirror = typeTag[T].mirror
    val cls = mirror.runtimeClass(typeTag[T].tpe)

    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val extractExpression = ScalaReflection.extractorsFor[T](inputObject)
    val constructExpression = ScalaReflection.constructorFor[T]

    new ClassEncoder[T](
      extractExpression.dataType,
      extractExpression.flatten,
      constructExpression,
      ClassTag[T](cls))
  }
}

/**
 * A generic encoder for JVM objects.
 *
 * @param schema The schema after converting `T` to a Spark SQL row.
 * @param extractExpressions A set of expressions, one for each top-level field that can be used to
 *                           extract the values from a raw object.
 * @param constructExpression An expression that can be used to construct a new instance of `T`
 *                            given an input row.
 * @param clsTag A [[ClassTag]] for `T`.
 */
case class ClassEncoder[T](
    schema: StructType,
    extractExpressions: Seq[NamedExpression],
    constructExpression: Expression,
    clsTag: ClassTag[T])
  extends Encoder[T] {

  @transient
  private lazy val extractProjection =
    GenerateUnsafeProjection.generate(extractExpressions)
  private val inputRow = new GenericMutableRow(1)

  @transient
  private lazy val constructProjection = GenerateSafeProjection.generate(constructExpression :: Nil)
  private val dataType = ObjectType(clsTag.runtimeClass)

  override def toRow(t: T): InternalRow = {
    inputRow(0) = t
    extractProjection(inputRow)
  }

  override def fromRow(row: InternalRow): T = {
    constructProjection(row).get(0, dataType).asInstanceOf[T]
  }

  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): ClassEncoder[T] = {
    val positionToAttribute = AttributeMap.toIndex(oldSchema)
    val attributeToNewPosition = AttributeMap.byIndex(newSchema)
    copy(constructExpression = constructExpression transform {
      case r: BoundReference =>
        r.copy(ordinal = attributeToNewPosition(positionToAttribute(r.ordinal)))
    })
  }

  override def bind(schema: Seq[Attribute]): ClassEncoder[T] = {
    val plan = Project(Alias(constructExpression, "object")() :: Nil, LocalRelation(schema))
    val analyzedPlan = SimpleAnalyzer.execute(plan)
    val resolvedExpression = analyzedPlan.expressions.head.children.head
    val boundExpression = BindReferences.bindReference(resolvedExpression, schema)

    copy(constructExpression = boundExpression)
  }

  override def bindOrdinals(schema: Seq[Attribute]): ClassEncoder[T] = {
    var remaining = schema
    copy(constructExpression = constructExpression transform {
      case u: UnresolvedAttribute =>
        var pos = remaining.head
        remaining = remaining.drop(1)
        pos
    })
  }

  protected val attrs = extractExpressions.map(_.collect {
    case a: Attribute => s"#${a.exprId}"
    case b: BoundReference => s"[${b.ordinal}]"
  }.headOption.getOrElse(""))


  protected val schemaString =
    schema
      .zip(attrs)
      .map { case(f, a) => s"${f.name}$a: ${f.dataType.simpleString}"}.mkString(", ")

  override def toString: String = s"class[$schemaString]"
}
