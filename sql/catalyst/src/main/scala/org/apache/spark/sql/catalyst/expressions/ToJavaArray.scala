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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._

/**
 * This expression converts data of `ArrayData` to an array of java type.
 *
 * NOTE: When the data type of expression is `ArrayType`, and the expression is foldable,
 * the `ConstantFolding` can do constant folding optimization automatically,
 * (avoiding frequent calls to `ArrayData.to{XXX}Array()`).
 */
case class ToJavaArray(array: Expression)
  extends UnaryExpression
  with NullIntolerant
  with RuntimeReplaceable
  with QueryErrorsBase {

  override def checkInputDataTypes(): TypeCheckResult = array.dataType match {
    case ArrayType(_, _) =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(ArrayType),
          "inputSql" -> toSQLExpr(array),
          "inputType" -> toSQLType(array.dataType))
      )
  }

  override def foldable: Boolean = array.foldable

  override def child: Expression = array
  override def prettyName: String = "to_java_array"

  @transient lazy val elementType: DataType =
    array.dataType.asInstanceOf[ArrayType].elementType
  private def resultArrayElementNullable: Boolean =
    array.dataType.asInstanceOf[ArrayType].containsNull
  private def isPrimitiveType: Boolean = CodeGenerator.isPrimitiveType(elementType)
  private def canPerformFast: Boolean = isPrimitiveType && !resultArrayElementNullable

  @transient private lazy val elementObjectType = ObjectType(classOf[DataType])
  @transient private lazy val dataTypeFunctionNameArgumentsInputTypes:
    (DataType, String, Seq[Expression], Seq[AbstractDataType]) = {
    if (canPerformFast) {
      elementType match {
        case BooleanType => (ObjectType(classOf[Array[Boolean]]), "toBooleanArray",
          Seq(array), Seq(array.dataType))
        case ByteType => (ObjectType(classOf[Array[Byte]]), "toByteArray",
          Seq(array), Seq(array.dataType))
        case ShortType => (ObjectType(classOf[Array[Short]]), "toShortArray",
          Seq(array), Seq(array.dataType))
        case IntegerType => (ObjectType(classOf[Array[Int]]), "toIntArray",
          Seq(array), Seq(array.dataType))
        case LongType => (ObjectType(classOf[Array[Long]]), "toLongArray",
          Seq(array), Seq(array.dataType))
        case FloatType => (ObjectType(classOf[Array[Float]]), "toFloatArray",
          Seq(array), Seq(array.dataType))
        case DoubleType => (ObjectType(classOf[Array[Double]]), "toDoubleArray",
          Seq(array), Seq(array.dataType))
      }
    } else if (isPrimitiveType) {
      elementType match {
        case BooleanType => (ObjectType(classOf[Array[java.lang.Boolean]]), "toBoxedBooleanArray",
          Seq(array), Seq(array.dataType))
        case ByteType => (ObjectType(classOf[Array[java.lang.Byte]]), "toBoxedByteArray",
          Seq(array), Seq(array.dataType))
        case ShortType => (ObjectType(classOf[Array[java.lang.Short]]), "toBoxedShortArray",
          Seq(array), Seq(array.dataType))
        case IntegerType => (ObjectType(classOf[Array[java.lang.Integer]]), "toBoxedIntArray",
          Seq(array), Seq(array.dataType))
        case LongType => (ObjectType(classOf[Array[java.lang.Long]]), "toBoxedLongArray",
          Seq(array), Seq(array.dataType))
        case FloatType => (ObjectType(classOf[Array[java.lang.Float]]), "toBoxedFloatArray",
          Seq(array), Seq(array.dataType))
        case DoubleType => (ObjectType(classOf[Array[java.lang.Double]]), "toBoxedDoubleArray",
          Seq(array), Seq(array.dataType))
      }
    } else {
      (ObjectType(classOf[Array[Object]]), "toObjectArray",
        Seq(array, Literal(elementType, elementObjectType)), Seq(array.dataType, elementObjectType))
    }
  }

  override def dataType: DataType = dataTypeFunctionNameArgumentsInputTypes._1

  override def replacement: Expression = {
    StaticInvoke(
      classOf[ToJavaArrayUtils],
      dataTypeFunctionNameArgumentsInputTypes._1,
      dataTypeFunctionNameArgumentsInputTypes._2,
      dataTypeFunctionNameArgumentsInputTypes._3,
      dataTypeFunctionNameArgumentsInputTypes._4)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(array = newChild)
}
