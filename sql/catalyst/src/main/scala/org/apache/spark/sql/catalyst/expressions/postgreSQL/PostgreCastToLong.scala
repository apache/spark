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
import org.apache.spark.unsafe.types.UTF8String.LongWrapper

case class PostgreCastToLong(child: Expression, timeZoneId: Option[String])
  extends CastBase {
  override def dataType: DataType = LongType

  override protected def ansiEnabled: Boolean =
    throw new AnalysisException("")

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case DateType | TimestampType | NullType =>
      TypeCheckResult.TypeCheckFailure(s"cannot cast type ${child.dataType} to long")
    case _ =>
      TypeCheckResult.TypeCheckSuccess
  }
  /** Returns a copy of this expression with the specified timeZoneId. */
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def castToLong(from: DataType): Any => Any = from match {
    case StringType =>
      val result = new LongWrapper()
      buildCast[UTF8String](_, s => if (s.toLong(result)) result.value
      else throw new AnalysisException(s"invalid input syntax for type long: $s"))
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0L)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  override def castToLongCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
      case StringType =>
        val wrapper = ctx.freshVariable("longWrapper", classOf[UTF8String.LongWrapper])
        (c, evPrim, _) =>
          code"""
          UTF8String.LongWrapper $wrapper = new UTF8String.LongWrapper();
          if ($c.toLong($wrapper)) {
            $evPrim = $wrapper.value;
          } else {
            throw new AnalysisException(s"invalid input syntax for type long: $c");
          }
          $wrapper = null;
        """
      case BooleanType =>
        (c, evPrim, _) => code"$evPrim = $c ? 1L : 0L;"
      case DecimalType() =>
        (c, evPrim, _) => code"$evPrim = $c.to${"long".capitalize}();"
      case NumericType() =>
        (c, evPrim, _) => code"$evPrim = (long) $c;"
    }
}
