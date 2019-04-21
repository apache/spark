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

package org.apache.spark.sql.catalyst.bytecode

import scala.collection.mutable

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CreateNamedStructUnsafe, Expression, LeafExpression, Literal, Unevaluable}
import org.apache.spark.sql.types.{DataType, ObjectType, StructType}

/**
 * A base trait for all expressions that represent references to JVM objects.
 */
private[bytecode] trait Ref extends LeafExpression with Unevaluable {
  def clazz: Class[_]
  def resolve(): Expression
}

/**
 * An expression that represents a reference to a JVM object.
 *
 * @param value the reference value.
 * @param clazz the Java class.
 */
private[bytecode] case class ObjectRef(var value: Expression, clazz: Class[_]) extends Ref {
  // TODO: better way?
  lazy val isBoxedPrimitive: Boolean = boxedPrimitiveClasses.contains(clazz.getName)
  override def nullable: Boolean = value.nullable
  override def dataType: DataType = value.dataType
  override def resolve(): Expression = resolveRefs(value)
}

/**
 * An expression that represents a reference to a Scala object such as [[scala.Predef]].
 *
 * @param clazz the Java class.
 */
private[bytecode] case class ScalaObjectRef(clazz: Class[_]) extends Ref {
  override def nullable: Boolean = false
  override def dataType: DataType = ObjectType(clazz)
  override def resolve(): Expression = {
    throw new RuntimeException("ScalaObjectRef cannot be resolved")
  }
}

/**
 * An expression that represents a reference to an object that maps into a struct
 * in Catalyst (e.g., case classes, POJOs).
 *
 * At the bytecode level, we first need to push a reference onto the operand stack and then call
 * a dedicated constructor to handle the creation of new objects. Later on, other operations might
 * read/modify field values.
 *
 * @param clazz the runtime Java class.
 */
private[bytecode] case class StructRef(clazz: Class[_]) extends Ref {

  private val fieldValueMap = new mutable.HashMap[String, Expression]()
  private val structType: StructType = getStructType(clazz)

  override def nullable: Boolean = false
  override def dataType: DataType = structType

  def setFieldValue(fieldName: String, value: Expression): Unit = {
    fieldValueMap(fieldName) = value
  }

  def getFieldValue(fieldName: String): Expression = {
    fieldValueMap(fieldName)
  }

  override def resolve(): Expression = {
    // we need to preserve the order of the fields
    val structExprs = structType.fields.toSeq.flatMap { field =>
      val fieldName = Literal(field.name)
      val fieldValue = resolveRefs(fieldValueMap(field.name))
      Seq[Expression](fieldName, fieldValue)
    }
    CreateNamedStructUnsafe(structExprs)
  }

  private def getStructType(clazz: Class[_]): StructType = {
    ScalaReflection.schemaFor(clazz).dataType match {
      case st: StructType => st
      case _ => throw new RuntimeException(s"$clazz cannot be represented as a struct")
    }
  }
}
