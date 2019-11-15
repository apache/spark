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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{CastBase, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.types.UTF8String.IntWrapper

case class PostgreCastToShort(child: Expression, timeZoneId: Option[String])
  extends CastBase {

  override protected def ansiEnabled =
    throw new UnsupportedOperationException("PostgreSQL dialect doesn't support ansi mode")

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case StringType | IntegerType | LongType | ByteType | FloatType | DoubleType | NullType =>
      TypeCheckResult.TypeCheckSuccess
    case _: DecimalType => TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(s"cannot cast type ${child.dataType} to short")
  }

  override def castToShort(from: DataType): Any => Any = from match {
    case StringType =>
      val result = new IntWrapper()
      buildCast[UTF8String](_, s => if (s.toShort(result)) {
        result.value.toShort
      } else {
        throw new AnalysisException(s"invalid input syntax for integer: $s")
      })
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toShort
  }

  def castDecimalToIntegralTypeCode(ctx: CodegenContext, integralType: String): CastFunction = {
      (c, evPrim, evNull) => code"$evPrim = $c.to${integralType.capitalize}();"
  }

  override def castToShortCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case StringType =>
      val wrapper = ctx.freshVariable("intWrapper", classOf[UTF8String.IntWrapper])
      (c, evPrim, evNull) =>
        code"""
          UTF8String.IntWrapper $wrapper = new UTF8String.IntWrapper();
          if ($c.toShort($wrapper)) {
            $evPrim = (short) $wrapper.value;
          } else {
            throw new AnalysisException("invalid input syntax for integer: $c")
          }
          $wrapper = null;
        """

    case DecimalType() => castDecimalToIntegralTypeCode(ctx, "short")
    case _: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (short) $c;"
  }

  override def dataType: DataType = ShortType

  override def nullable: Boolean = child.nullable

  override def toString: String = s"PostgreCastToShort($child as ${dataType.simpleString})"

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}
