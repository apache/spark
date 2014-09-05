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
 * Returns the value of fields in the `child`.
 * The type of `child` can be struct, or array of struct,
 * or array of array of struct, or array of array ... of struct.
 */
case class GetField(child: Expression, fieldName: String) extends UnaryExpression {
  type EvaluatedType = Any

  lazy val dataType = {
    structType
    buildDataType(field.dataType)
  }

  override def nullable = child.nullable || field.nullable
  override def foldable = child.foldable

  private var _buildDataType = identity[DataType] _
  private lazy val buildDataType = {
    structType
    _buildDataType
  }

  private var _nestedArrayCount = 0
  private lazy val nestedArrayCount = {
    structType
    _nestedArrayCount
  }

  private def getStructType(t: DataType): StructType = t match {
    case ArrayType(elementType, containsNull) =>
      _buildDataType = {(t: DataType) => ArrayType(t, containsNull)} andThen _buildDataType
      _nestedArrayCount += 1
      getStructType(elementType)
    case s: StructType => s
    case otherType => sys.error(s"GetField is not valid on fields of type $otherType")
  }

  protected lazy val structType: StructType = {
    child match {
      case n: GetField =>
        this._buildDataType = n._buildDataType
        this._nestedArrayCount = n._nestedArrayCount
        getStructType(n.field.dataType)
      case _ => getStructType(child.dataType)
    }
  }

  lazy val field =
    structType.fields
        .find(_.name == fieldName)
        .getOrElse(sys.error(s"No such field $fieldName in ${child.dataType}"))

  lazy val ordinal = structType.fields.indexOf(field)

  override lazy val resolved = childrenResolved

  override def eval(input: Row): Any = {
    val baseValue = child.eval(input)
    evaluateValue(baseValue, nestedArrayCount)
  }

  private def evaluateValue(v: Any, count: Int): Any = {
    if (v == null) {
      null
    } else if (count > 0) {
      v.asInstanceOf[Seq[_]].map(r => evaluateValue(r, count - 1))
    } else {
      v.asInstanceOf[Row](ordinal)
    }
  }

  override def toString = s"$child.$fieldName"
}
