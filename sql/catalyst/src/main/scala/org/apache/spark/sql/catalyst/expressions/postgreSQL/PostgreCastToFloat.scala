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

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, CastBase, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class PostgreCastToFloat(child: Expression, timeZoneId: Option[String])
  extends CastBase {

  override def dataType: DataType = FloatType

  override def toString: String = s"PostgreCastToFloat($child as ${dataType.simpleString})"

  override def nullable: Boolean = child.nullable

  override protected def ansiEnabled =
    throw new UnsupportedOperationException("PostgreSQL dialect doesn't support ansi mode")

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case StringType | LongType | IntegerType | NullType |
         ShortType | DoubleType | ByteType =>
      TypeCheckResult.TypeCheckSuccess
    case _: DecimalType => TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(s"cannot cast type ${child.dataType} to float")
  }

  def processFloatingPointSpecialLiterals(v: String, isFloat: Boolean): Any = {
    v.trim.toLowerCase(Locale.ROOT) match {
      case "inf" | "+inf" | "infinity" | "+infinity" =>
        if (isFloat) Float.PositiveInfinity else Double.PositiveInfinity
      case "-inf" | "-infinity" =>
        if (isFloat) Float.NegativeInfinity else Double.NegativeInfinity
      case "nan" =>
        if (isFloat) Float.NaN else Double.NaN
      case _ => throw new AnalysisException(s"invalid input syntax for type double precision: $v")
    }
  }

  override def castToFloat(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => {
        val floatStr = s.toString
        try floatStr.toFloat catch {
          case _: NumberFormatException =>
            processFloatingPointSpecialLiterals(floatStr, true)
        }
      })
    case x: NumericType =>
      super.castToFloat(from)
  }

  override def castToFloatCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case StringType =>
      val floatStr = ctx.freshVariable("floatStr", StringType)
      (c, evPrim, evNull) =>
        code"""
          final String $floatStr = $c.toString();
          try {
            $evPrim = Float.valueOf($floatStr);
          } catch (java.lang.NumberFormatException e) {
            final Float f = (Float) processFloatingPointSpecialLiterals($floatStr, true);
            if (f == null) {
              throw new AnalysisException("invalid input syntax for type double precision: $c")
            } else {
              $evPrim = f.floatValue();
            }
          }
        """
    case DecimalType() =>
      super.castToFloatCode(from, ctx)
    case x: NumericType =>
      super.castToFloatCode(from, ctx)
  }
  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}
