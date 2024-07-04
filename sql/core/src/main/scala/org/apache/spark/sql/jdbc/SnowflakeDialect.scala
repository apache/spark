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

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.{BooleanType, DataType}

private case class SnowflakeDialect() extends JdbcDialect with NoLegacyJDBCError {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:snowflake")

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType =>
      // By default, BOOLEAN is mapped to BIT(1).
      // but Snowflake does not have a BIT type. It uses BOOLEAN instead.
      Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case _ => JdbcUtils.getCommonJDBCType(dt)
  }
}
