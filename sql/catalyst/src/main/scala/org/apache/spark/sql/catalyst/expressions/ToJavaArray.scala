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

import java.lang.reflect.{Array => JArray}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

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

  private def resultArrayElementNullable: Boolean =
    array.dataType.asInstanceOf[ArrayType].containsNull
  private def isPrimitiveType: Boolean = CodeGenerator.isPrimitiveType(elementType)
  private def canPerformFast: Boolean = isPrimitiveType && !resultArrayElementNullable

  @transient lazy val elementType: DataType =
    array.dataType.asInstanceOf[ArrayType].elementType
  @transient private lazy val elementObjectType = ObjectType(classOf[DataType])
  @transient private lazy val elementCls: Class[_] = {
    if (canPerformFast) {
      CodeGenerator.javaClass(elementType)
    } else if (isPrimitiveType) {
      Utils.classForName(s"java.lang.${CodeGenerator.boxedType(elementType)}")
    } else {
      classOf[Object]
    }
  }
  @transient private lazy val returnCls = JArray.newInstance(elementCls, 0).getClass

  override def dataType: DataType = ObjectType(returnCls)

  override def replacement: Expression = {
    if (isPrimitiveType) {
      val funcNamePrefix = if (resultArrayElementNullable) "toBoxed" else "to"
      val funcName = s"$funcNamePrefix${CodeGenerator.boxedType(elementType)}Array"
      StaticInvoke(
        classOf[ToJavaArrayUtils],
        dataType,
        funcName,
        Seq(array),
        Seq(array.dataType))
    } else {
      Invoke(
        array,
        "toObjectArray",
        dataType,
        Seq(Literal(elementType, elementObjectType)),
        Seq(elementObjectType))
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(array = newChild)
}
