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

package org.apache.spark.sql.catalyst.expressions.ml

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._

/**
 * Converts an MLlib vector to an array of doubles or floats. This expression accepts a vector's
 * SQL struct representation or a UDT with the same underlying SQL type, including field and
 * array element nullability. Null inputs are rejected.
 */
case class VectorToArray(child: Expression, elementType: DataType) extends UnaryExpression {
  require(elementType == DoubleType || elementType == FloatType,
    "VectorToArray requires a double or float element type.")

  override def prettyName: String = "vector_to_array"

  override def dataType: DataType = ArrayType(elementType, containsNull = false)

  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    val inputType = child.dataType match {
      case udt: UserDefinedType[_] => udt.sqlType
      case other => other
    }
    if (inputType == VectorScaleShift.vectorSqlType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(VectorScaleShift.vectorSqlType),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType)))
    }
  }

  override def eval(input: InternalRow): Any = {
    MLExpressionUtils.vectorToArray(
      child.eval(input).asInstanceOf[InternalRow], elementType == FloatType)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val input = child.genCode(ctx)
    val utils = classOf[MLExpressionUtils].getName
    val toFloat = elementType == FloatType
    ev.copy(code = code"""
      ${input.code}
      ArrayData ${ev.value} = $utils.vectorToArray(
        ${input.isNull} ? null : ${input.value}, $toFloat);
    """, isNull = FalseLiteral)
  }

  override protected def withNewChildInternal(newChild: Expression): VectorToArray = {
    copy(child = newChild)
  }
}
