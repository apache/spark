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
package org.apache.spark.sql.catalyst.expressions.postgreSQL

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.postgreSQL.StringUtils
import org.apache.spark.sql.types.{BooleanType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class PostgreCastStringToBoolean(child: Expression)
  extends UnaryExpression with NullIntolerant {

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType == StringType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"The expression ${getClass.getSimpleName} only accepts string input data type")
    }
  }

  override def nullSafeEval(input: Any): Any = {
    val s = input.asInstanceOf[UTF8String].trim().toLowerCase()
    if (StringUtils.isTrueString(s)) {
      true
    } else if (StringUtils.isFalseString(s)) {
      false
    } else {
      null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val stringUtils = inline"${StringUtils.getClass.getName.stripSuffix("$")}"
    val eval = child.genCode(ctx)
    val javaType = JavaCode.javaType(dataType)
    val preprocessedString = ctx.freshName("preprocessedString")
    val castCode =
      code"""
        boolean ${ev.isNull} = ${eval.isNull};
        $javaType ${ev.value} = false;
        if (!${eval.isNull}) {
          UTF8String $preprocessedString = ${eval.value}.trim().toLowerCase();
          if ($stringUtils.isTrueString($preprocessedString)) {
            ${ev.value} = true;
          } else if ($stringUtils.isFalseString($preprocessedString)) {
            ${ev.value} = false;
          } else {
            ${ev.isNull} = true;
          }
        }
      """
    ev.copy(code = eval.code + castCode)
  }

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = true

  override def toString: String = s"PostgreCastStringToBoolean($child as ${dataType.simpleString})"

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}
