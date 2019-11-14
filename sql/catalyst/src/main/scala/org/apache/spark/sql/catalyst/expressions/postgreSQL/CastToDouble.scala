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

import org.apache.spark.sql.catalyst.expressions.{CastBase, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{DataType, DoubleType}

case class CastToDouble(child: Expression, timeZoneId: Option[String]) extends CastBase{
  override def dataType: DataType = DoubleType

  override protected def ansiEnabled: Boolean =
    throw new UnsupportedOperationException("PostgreSQL dialect doesn't support ansi mode")

  /** Returns a copy of this expression with the specified timeZoneId. */
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def castToDouble(from: DataType): Any => Any = super.castToDouble(from)

  override def castToDoubleCode(from: DataType, ctx: CodegenContext): CastFunction =
    super.castToDoubleCode(from, ctx)

  override def nullable: Boolean = child.nullable

  override def toString: String = s"PostgreCastToDouble($child as ${dataType.simpleString})"

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}
