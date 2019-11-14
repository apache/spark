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
import org.apache.spark.sql.catalyst.expressions.{CastBase, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, NumericType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

case class PostgreCastToDouble(child: Expression, timeZoneId: Option[String])
  extends CastBase {

  override protected def ansiEnabled =
    throw new UnsupportedOperationException("PostgreSQL dialect doesn't support ansi mode")

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case TimestampType | BooleanType | DateType =>
      TypeCheckResult.TypeCheckFailure(s"cannot cast type ${child.dataType} to double")
    case _ =>
      TypeCheckResult.TypeCheckSuccess
  }

  override def castToDouble(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => {
        val doubleStr = s.toString
        try doubleStr.toDouble catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"invalid input syntax for type double: $doubleStr")
        }
      })
    case NumericType() =>
      super.castToDouble(from)
  }

  override def castToDoubleCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case StringType =>
      val doubleStr = ctx.freshVariable("doubleStr", StringType)
      (c, evPrim, _) =>
        code"""
          final String $doubleStr = $c.toString();
          try {
            $evPrim = Double.valueOf($doubleStr);
          } catch (java.lang.NumberFormatException e) {
            throw new IllegalArgumentException("invalid input syntax for type double: $c
          }
        """
    case NumericType() =>
      super.castToDoubleCode(from, ctx)
  }

  override def dataType: DataType = DoubleType

  override def nullable: Boolean = child.nullable

  override def toString: String = s"PostgreCastToDouble($child as ${dataType.simpleString})"

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}

