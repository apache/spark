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

import java.sql.Connection

import scala.collection.mutable.ArrayBuilder

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.types._

private case class DatabricksDialect() extends JdbcDialect with NoLegacyJDBCError {

  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:databricks")
  }

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case java.sql.Types.TINYINT => Some(ByteType)
      case java.sql.Types.SMALLINT => Some(ShortType)
      case java.sql.Types.REAL => Some(FloatType)
      case _ => None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case DoubleType => Some(JdbcType("DOUBLE", java.sql.Types.DOUBLE))
    case _: StringType => Some(JdbcType("STRING", java.sql.Types.VARCHAR))
    case BinaryType => Some(JdbcType("BINARY", java.sql.Types.BINARY))
    case _ => None
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }

  override def supportsLimit: Boolean = true

  override def supportsOffset: Boolean = true

  override def supportsTableSample: Boolean = true

  override def getTableSample(sample: TableSampleInfo): String = {
    s"TABLESAMPLE (${(sample.upperBound - sample.lowerBound) * 100}) REPEATABLE (${sample.seed})"
  }

  // Override listSchemas to run "show schemas" as a PreparedStatement instead of
  // invoking getMetaData.getSchemas as it may not work correctly in older versions of the driver.
  override def schemasExists(conn: Connection, options: JDBCOptions, schema: String): Boolean = {
    val stmt = conn.prepareStatement("SHOW SCHEMAS")
    val rs = stmt.executeQuery()
    while (rs.next()) {
      if (rs.getString(1) == schema) {
        return true
      }
    }
    false
  }

  // Override listSchemas to run "show schemas" as a PreparedStatement instead of
  // invoking getMetaData.getSchemas as it may not work correctly in older versions of the driver.
  override def listSchemas(conn: Connection, options: JDBCOptions): Array[Array[String]] = {
    val schemaBuilder = ArrayBuilder.make[Array[String]]
    val stmt = conn.prepareStatement("SHOW SCHEMAS")
    val rs = stmt.executeQuery()
    while (rs.next()) {
      schemaBuilder += Array(rs.getString(1))
    }
    schemaBuilder.result()
  }
}
