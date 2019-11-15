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
import org.apache.spark.sql.catalyst.util.postgreSQL.StringUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class PostgreCastToBoolean(child: Expression, timeZoneId: Option[String])
  extends CastBase {

  override protected def ansiEnabled =
    throw new UnsupportedOperationException("PostgreSQL dialect doesn't support ansi mode")

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case StringType | IntegerType | NullType =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(s"cannot cast type ${child.dataType} to boolean")
  }

  override def castToBoolean(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, str => {
        val s = str.trim().toLowerCase()
        if (StringUtils.isTrueString(s)) {
          true
        } else if (StringUtils.isFalseString(s)) {
          false
        } else {
          throw new AnalysisException(s"invalid input syntax for type boolean: $s")
        }
      })
    case IntegerType =>
      super.castToBoolean(from)
  }

  override def castToBooleanCode(from: DataType): CastFunction = from match {
    case StringType =>
      val stringUtils = inline"${StringUtils.getClass.getName.stripSuffix("$")}"
      (c, evPrim, evNull) =>
        code"""
          if ($stringUtils.isTrueString($c.trim().toLowerCase())) {
            $evPrim = true;
          } else if ($stringUtils.isFalseString($c.trim().toLowerCase())) {
            $evPrim = false;
          } else {
            throw new AnalysisException("invalid input syntax for type boolean: $c");
          }
        """

    case IntegerType =>
      super.castToBooleanCode(from)
  }

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = child.nullable

  override def toString: String = s"PostgreCastToBoolean($child as ${dataType.simpleString})"

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}
