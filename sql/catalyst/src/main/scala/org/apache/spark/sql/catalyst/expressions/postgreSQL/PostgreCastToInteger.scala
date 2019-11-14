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

case class PostgreCastToInteger(child: Expression, timeZoneId: Option[String])
  extends CastBase{
  override def dataType: DataType = IntegerType

  override protected def ansiEnabled: Boolean =
    throw new AnalysisException("PostgreSQL dialect doesn't support ansi mode")

  override def nullable: Boolean = true

  /** Returns a copy of this expression with the specified timeZoneId. */
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case ByteType | TimestampType | DateType =>
      TypeCheckResult.TypeCheckFailure(s"Cannot cast type ${child.dataType} to int")
    case _ => TypeCheckResult.TypeCheckSuccess
  }

  override def castToInt(from: DataType): Any => Any = from match {
    case StringType =>
      val result = new IntWrapper()
      buildCast[UTF8String](_, s => if (s.toInt(result)) {
        result.value
      } else {
        throw new AnalysisException(s"invalid input syntax for type numeric: $s")
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1 else 0)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b)
  }

  override def castToIntCode(
      from: DataType,
      ctx: CodegenContext): CastFunction = from match {
    case StringType =>
      val wrapper = ctx.freshVariable("intWrapper", classOf[UTF8String.IntWrapper])
      (c, evPrim, evNull) =>
        code"""
          UTF8String.IntWrapper $wrapper = new UTF8String.IntWrapper();
          if ($c.toInt($wrapper)) {
            $evPrim = $wrapper.value;
          } else {
            $evNull = throw new AnalysisException(s"invalid input syntax for type numeric: $c;
          }
          $wrapper = null;
        """
    case BooleanType =>
      (c, evPrim, _) => code"$evPrim = $c ? 1 : 0;"
    case _: NumericType =>
      (c, evPrim, _) => code"$evPrim = (int) $c;"
  }

  override def toString: String = s"PostgreCastToInt($child as ${dataType.simpleString})"

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}
