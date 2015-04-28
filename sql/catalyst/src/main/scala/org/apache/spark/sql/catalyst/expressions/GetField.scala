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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.types._

object GetField {
  /**
   * Returns the resolved `GetField`. It will return one kind of concrete `GetField`,
   * depend on the type of `child` and `fieldExpr`.
   */
  def apply(
      child: Expression,
      fieldExpr: Expression,
      resolver: Resolver): GetField = {

    (child.dataType, fieldExpr) match {
      case (StructType(fields), Literal(fieldName, StringType)) =>
        val ordinal = findField(fields, fieldName.toString, resolver)
        SimpleStructGetField(child, fields(ordinal), ordinal)
      case (ArrayType(StructType(fields), containsNull), Literal(fieldName, StringType)) =>
        val ordinal = findField(fields, fieldName.toString, resolver)
        ArrayStructGetField(child, fields(ordinal), ordinal, containsNull)
      case (_: ArrayType, _) if fieldExpr.dataType.isInstanceOf[IntegralType]  =>
        ArrayOrdinalGetField(child, fieldExpr)
      case (_: MapType, _) =>
        MapOrdinalGetField(child, fieldExpr)
      case (otherType, _) =>
        throw new AnalysisException(
          "GetField is not valid on child of type " +
            s"$otherType with fieldExpr of type ${fieldExpr.dataType}")
    }
  }

  def unapply(g: GetField): Option[(Expression, Expression)] = {
    g match {
      case _: StructGetField => Some((g.child, null))
      case o: OrdinalGetField => Some((o.child, o.ordinal))
      case _ => None
    }
  }

  /**
   * find the ordinal of StructField, report error if no desired field or over one
   * desired fields are found.
   */
  private def findField(fields: Array[StructField], fieldName: String, resolver: Resolver): Int = {
    val checkField = (f: StructField) => resolver(f.name, fieldName)
    val ordinal = fields.indexWhere(checkField)
    if (ordinal == -1) {
      throw new AnalysisException(
        s"No such struct field $fieldName in ${fields.map(_.name).mkString(", ")}")
    } else if (fields.indexWhere(checkField, ordinal + 1) != -1) {
      throw new AnalysisException(
        s"Ambiguous reference to fields ${fields.filter(checkField).mkString(", ")}")
    } else {
      ordinal
    }
  }
}

trait GetField extends UnaryExpression {
  self: Product =>

  type EvaluatedType = Any
}

abstract class StructGetField extends GetField {
  self: Product =>

  def field: StructField

  override def foldable: Boolean = child.foldable
  override def toString: String = s"$child.${field.name}"
}

abstract class OrdinalGetField extends GetField {
  self: Product =>

  def ordinal: Expression

  /** `Null` is returned for invalid ordinals. */
  override def nullable: Boolean = true
  override def foldable: Boolean = child.foldable && ordinal.foldable
  override def toString: String = s"$child[$ordinal]"
  override def children: Seq[Expression] = child :: ordinal :: Nil

  override def eval(input: Row): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      val o = ordinal.eval(input)
      if (o == null) {
        null
      } else {
        evalNotNull(value, o)
      }
    }
  }

  protected def evalNotNull(value: Any, ordinal: Any): Any
}

/**
 * Returns the value of fields in the Struct `child`.
 */
case class SimpleStructGetField(child: Expression, field: StructField, ordinal: Int)
  extends StructGetField {

  override def dataType: DataType = field.dataType
  override def nullable: Boolean = child.nullable || field.nullable

  override def eval(input: Row): Any = {
    val baseValue = child.eval(input).asInstanceOf[Row]
    if (baseValue == null) null else baseValue(ordinal)
  }
}

/**
 * Returns the array of value of fields in the Array of Struct `child`.
 */
case class ArrayStructGetField(
    child: Expression,
    field: StructField,
    ordinal: Int,
    containsNull: Boolean) extends StructGetField {

  override def dataType: DataType = ArrayType(field.dataType, containsNull)
  override def nullable: Boolean = child.nullable

  override def eval(input: Row): Any = {
    val baseValue = child.eval(input).asInstanceOf[Seq[Row]]
    if (baseValue == null) null else {
      baseValue.map { row =>
        if (row == null) null else row(ordinal)
      }
    }
  }
}

/**
 * Returns the field at `ordinal` in the Array `child`
 */
case class ArrayOrdinalGetField(child: Expression, ordinal: Expression)
  extends OrdinalGetField {

  override def dataType: DataType = child.dataType.asInstanceOf[ArrayType].elementType

  override lazy val resolved = childrenResolved &&
    child.dataType.isInstanceOf[ArrayType] && ordinal.dataType.isInstanceOf[IntegralType]

  protected def evalNotNull(value: Any, ordinal: Any) = {
    // TODO: consider using Array[_] for ArrayType child to avoid
    // boxing of primitives
    val baseValue = value.asInstanceOf[Seq[_]]
    val index = ordinal.asInstanceOf[Int]
    if (index >= baseValue.size || index < 0) {
      null
    } else {
      baseValue(index)
    }
  }
}

/**
 * Returns the value of key `ordinal` in Map `child`
 */
case class MapOrdinalGetField(child: Expression, ordinal: Expression)
  extends OrdinalGetField {

  override def dataType: DataType = child.dataType.asInstanceOf[MapType].valueType

  override lazy val resolved = childrenResolved && child.dataType.isInstanceOf[MapType]

  protected def evalNotNull(value: Any, ordinal: Any) = {
    val baseValue = value.asInstanceOf[Map[Any, _]]
    baseValue.get(ordinal).orNull
  }
}
