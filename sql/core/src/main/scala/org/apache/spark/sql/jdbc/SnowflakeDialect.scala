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

package org.apache.spark.sql.jdbc

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.{BooleanType, DataType, FractionalType, TimestampType}

private case class SnowflakeDialect() extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:snowflake")

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType =>
      // By default, BOOLEAN is mapped to BIT(1).
      // but Snowflake does not have a BIT type. It uses BOOLEAN instead.
      Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case TimestampType =>
      Option(JdbcType("TIMESTAMP_TZ", java.sql.Types.TIMESTAMP))
    case _ => JdbcUtils.getCommonJDBCType(dt)
  }

  override def compileExpression(expr: Expression): Option[String] = {
    val snowflakeSQLBuilder = new SnowflakeSQLBuilder()
    try {
      Some(snowflakeSQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression to snowflake", e)
        None
    }
  }

  private class SnowflakeSQLBuilder extends JDBCSQLBuilder {
    override def visitCast(expr: String, exprDataType: DataType, dataType: DataType): String = {
      if (exprDataType.isInstanceOf[FractionalType]) {
        if (dataType.isInstanceOf[TimestampType]) {
          throw new UnsupportedOperationException("Cannot cast fractional types to timestamp type")
        }
        if (dataType.isInstanceOf[BooleanType]) {
          // Cast from floating point types fails only when column is referenced,
          // but succeed for literal values
          throw new UnsupportedOperationException("Cannot cast fractional types to boolean type")
        }
      }
      super.visitCast(expr, exprDataType, dataType)
    }
  }
}
