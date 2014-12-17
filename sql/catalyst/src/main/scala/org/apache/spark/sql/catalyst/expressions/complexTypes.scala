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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.Map

import org.apache.spark.sql.catalyst.types._

/**
 * Returns the item at `ordinal` in the Array `child` or the Key `ordinal` in Map `child`.
 */
case class GetItem(child: Expression, ordinal: Expression) extends Expression {
  type EvaluatedType = Any

  val children = child :: ordinal :: Nil
  /** `Null` is returned for invalid ordinals. */
  override def nullable = true
  override def foldable = child.foldable && ordinal.foldable

  def dataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case MapType(_, vt, _) => vt
  }
  override lazy val resolved =
    childrenResolved &&
    (child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType])

  override def toString = s"$child[$ordinal]"

  override def eval(input: Row): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      val key = ordinal.eval(input)
      if (key == null) {
        null
      } else {
        if (child.dataType.isInstanceOf[ArrayType]) {
          // TODO: consider using Array[_] for ArrayType child to avoid
          // boxing of primitives
          val baseValue = value.asInstanceOf[Seq[_]]
          val o = key.asInstanceOf[Int]
          if (o >= baseValue.size || o < 0) {
            null
          } else {
            baseValue(o)
          }
        } else {
          val baseValue = value.asInstanceOf[Map[Any, _]]
          baseValue.get(key).orNull
        }
      }
    }
  }
}

/**
 * Returns the value of fields in the Struct `child`.
 */
case class GetField(child: Expression, fieldName: String) extends UnaryExpression {
  type EvaluatedType = Any

  def dataType = field.dataType
  override def nullable = child.nullable || field.nullable
  override def foldable = child.foldable

  protected def structType = child.dataType match {
    case s: StructType => s
    case otherType => sys.error(s"GetField is not valid on fields of type $otherType")
  }

  lazy val field =
    structType.fields
        .find(_.name == fieldName)
        .getOrElse(sys.error(s"No such field $fieldName in ${child.dataType}"))

  lazy val ordinal = structType.fields.indexOf(field)

  override lazy val resolved = childrenResolved && fieldResolved

  /** Returns true only if the fieldName is found in the child struct. */
  private def fieldResolved = child.dataType match {
    case StructType(fields) => fields.map(_.name).contains(fieldName)
    case _ => false
  }

  override def eval(input: Row): Any = {
    val baseValue = child.eval(input).asInstanceOf[Row]
    if (baseValue == null) null else baseValue(ordinal)
  }

  override def toString = s"$child.$fieldName"
}

/**
 * Returns an Array containing the evaluation of all children expressions.
 */
case class CreateArray(children: Seq[Expression]) extends Expression {
  override type EvaluatedType = Any

  lazy val childTypes = children.map(_.dataType).distinct

  override lazy val resolved =
    childrenResolved && childTypes.size <= 1

  override def dataType: DataType = {
    assert(resolved, s"Invalid dataType of mixed ArrayType ${childTypes.mkString(",")}")
    ArrayType(
      childTypes.headOption.getOrElse(NullType),
      containsNull = children.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def eval(input: Row): Any = {
    children.map(_.eval(input))
  }

  override def toString = s"Array(${children.mkString(",")})"
}
