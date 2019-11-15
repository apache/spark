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

import java.time.ZoneId

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{CastBase, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, JavaCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.postgreSQL.StringUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class PostgreCastBase(toType: DataType) extends CastBase {

  def fromTypes: TypeCollection

  override def dataType: DataType = toType

  override protected def ansiEnabled: Boolean =
    throw new UnsupportedOperationException("PostgreSQL dialect doesn't support ansi mode")

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!fromTypes.acceptsType(child.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"cannot cast type ${child.dataType.simpleString} to ${toType.simpleString}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def nullable: Boolean = child.nullable

  override def sql: String = s"CAST(${child.sql} AS ${toType.sql})"

  override def toString: String =
    s"PostgreCastTo${toType.simpleString}($child as ${toType.simpleString})"
}

case class PostgreCastToBoolean(child: Expression, timeZoneId: Option[String])
  extends PostgreCastBase(BooleanType) {

  override def fromTypes: TypeCollection = TypeCollection(StringType, IntegerType, NullType)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def castToBoolean(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, str => {
        val s = str.trim().toLowerCase()
        if (StringUtils.isTrueString(s)) {
          true
        } else if (StringUtils.isFalseString(s)) {
          false
        } else {
          throw new IllegalArgumentException(s"invalid input syntax for type boolean: $s")
        }
      })
    case IntegerType =>
      super.castToBoolean(from)
  }

  override def castToBooleanCode(from: DataType): CastFunction = from match {
    case StringType =>
      val stringUtils = inline"${StringUtils.getClass.getName.stripSuffix("$")}"
      (c, evPrim, _) =>
        code"""
          if ($stringUtils.isTrueString($c.trim().toLowerCase())) {
            $evPrim = true;
          } else if ($stringUtils.isFalseString($c.trim().toLowerCase())) {
            $evPrim = false;
          } else {
            throw new IllegalArgumentException("invalid input syntax for type boolean: $c");
          }
        """
    case IntegerType =>
      super.castToBooleanCode(from)
  }
}

case class PostgreCastToTimestamp(child: Expression, timeZoneId: Option[String])
  extends PostgreCastBase(TimestampType) {

  override def fromTypes: TypeCollection = TypeCollection(StringType, DateType, NullType)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def castToTimestamp(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, utfs => DateTimeUtils.stringToTimestamp(utfs, zoneId)
        .getOrElse(throw new AnalysisException(s"invalid input syntax for type timestamp:$utfs")))
    case DateType =>
      super.castToTimestamp(from)
  }

  override def castToTimestampCode(
      from: DataType,
      ctx: CodegenContext): CastFunction = from match {
    case StringType =>
      val zoneIdClass = classOf[ZoneId]
      val zid = JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
      val longOpt = ctx.freshVariable("longOpt", classOf[Option[Long]])
      (c, evPrim, _) =>
        code"""
          scala.Option<Long> $longOpt =
            org.apache.spark.sql.catalyst.util.DateTimeUtils.stringToTimestamp($c, $zid);
          if ($longOpt.isDefined()) {
            $evPrim = ((Long) $longOpt.get()).longValue();
          } else {
            throw new AnalysisException(s"invalid input syntax for type timestamp:$c");
          }
         """
    case DateType =>
      super.castToTimestampCode(from, ctx)
  }
}
