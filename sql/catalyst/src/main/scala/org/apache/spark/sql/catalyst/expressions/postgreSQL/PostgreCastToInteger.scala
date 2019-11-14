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
import org.apache.spark.sql.catalyst.expressions.{CastBase, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, JavaCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

case class PostgreCastToInteger(child: Expression, timeZoneId: Option[String])
  extends CastBase{
  override def dataType: DataType = IntegerType

  override protected def ansiEnabled: Boolean = SQLConf.get.ansiEnabled

  override def nullable: Boolean = true

  /** Returns a copy of this expression with the specified timeZoneId. */
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def castToInt(from: DataType): Any => Any = from match {
    case ByteType | TimestampType | DateType =>
      throw new AnalysisException(
        s"Cannot cast type $from to Integer.")
    case _ =>
      super.castToInt(from)
  }

  private[this] def castToIntCode(
      from: DataType,
      ctx: CodegenContext): CastFunction = from match {
    case ByteType | TimestampType | DateType =>
      (_, _, _) =>
        val fromType = JavaCode.javaType(from)
        code"""throw new AnalysisException("Cannot cast type $fromType to Integer.");"""
    case _ => super.castToIntCode(from, ctx)
  }

  override def toString: String = s"PostgreCastToInt($child as ${dataType.simpleString})"

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}
