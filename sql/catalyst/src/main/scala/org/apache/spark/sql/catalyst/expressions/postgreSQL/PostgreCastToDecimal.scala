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

import java.math.{BigDecimal => JavaBigDecimal}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, CastBase, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class PostgreCastToDecimal(child: Expression, timeZoneId: Option[String])
  extends CastBase {

  override def dataType: DataType = DecimalType.defaultConcreteType

  override def toString: String = s"PostgreCastToDecimal($child as ${dataType.simpleString})"

  override def nullable: Boolean = child.nullable

  override protected def ansiEnabled =
    throw new UnsupportedOperationException("PostgreSQL dialect doesn't support ansi mode")

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case StringType | LongType | IntegerType | NullType | FloatType | ShortType |
         DoubleType | ByteType =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(s"cannot cast type ${child.dataType} to decimal")
  }

  override def castToDecimal(from: DataType, target: DecimalType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try {
        changePrecision(Decimal(new JavaBigDecimal(s.toString)), target)
      } catch {
        case _: NumberFormatException =>
          throw new AnalysisException(s"invalid input syntax for type numeric: $s")
      })
    case t: IntegralType =>
      super.castToDecimal(from, target)

    case x: FractionalType =>
      b => try {
        changePrecision(Decimal(x.fractional.asInstanceOf[Fractional[Any]].toDouble(b)), target)
      } catch {
        case _: NumberFormatException =>
          throw new AnalysisException(s"invalid input syntax for type numeric: $x")
      }
  }

  override def castToDecimalCode(from: DataType, target: DecimalType,
                                 ctx: CodegenContext): CastFunction = {
    val tmp = ctx.freshVariable("tmpDecimal", classOf[Decimal])
    val canNullSafeCast = Cast.canNullSafeCastToDecimal(from, target)
    from match {
      case StringType =>
        (c, evPrim, evNull) =>
          code"""
            try {
              Decimal $tmp = Decimal.apply(new java.math.BigDecimal($c.toString()));
              ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast)}
            } catch (java.lang.NumberFormatException e) {
              throw new AnalysisException("invalid input syntax for type numeric: $c")
            }
          """
      case t: IntegralType =>
        super.castToDecimalCode(from, target, ctx)

      case x: FractionalType =>
        // All other numeric types can be represented precisely as Doubles
        (c, evPrim, evNull) =>
          code"""
            try {
              Decimal $tmp = Decimal.apply(scala.math.BigDecimal.valueOf((double) $c));
              ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast)}
            } catch (java.lang.NumberFormatException e) {
              throw new AnalysisException("invalid input syntax for type numeric: $c")
            }
          """
    }
  }

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}
