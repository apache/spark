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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.TypeUtils.ordinalNumber
import org.apache.spark.sql.types.{DataType, UserDefinedType}

/**
 * Wrap a column with a UDT whose underlying SQL type matches the column data type.
 *
 * @see [[UnwrapUDT]] for converting a UDT column to its underlying SQL type.
 */
case class WrapUDT(child: Expression, udt: UserDefinedType[_])
  extends UnaryExpression with NonSQLExpression {

  def this(child: Expression, udt: Expression) = {
    this(child, WrapUDT.parseUDT(udt))
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (DataTypeUtils.sameType(child.dataType, udt.sqlType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(udt.sqlType),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType)))
    }
  }

  override def dataType: DataType = udt

  override def nullSafeEval(input: Any): Any = input

  override def prettyName: String = "wrap_udt"

  override protected def withNewChildInternal(newChild: Expression): WrapUDT = {
    copy(child = newChild)
  }
}

object WrapUDT {
  private def parseUDT(expression: Expression): UserDefinedType[_] = {
    ExprUtils.evalTypeExpr(expression) match {
      case udt: UserDefinedType[_] => udt
      case dataType =>
        throw new AnalysisException(
          errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "sqlExpr" -> toSQLExpr(expression),
            "paramIndex" -> ordinalNumber(1),
            "requiredType" -> toSQLType("UserDefinedType"),
            "inputSql" -> toSQLExpr(expression),
            "inputType" -> toSQLType(dataType)))
    }
  }
}
