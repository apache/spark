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

import java.sql.Types
import java.util.Locale

import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types._


private case class DerbyDialect() extends JdbcDialect with NoLegacyJDBCError {

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:derby")

  // See https://db.apache.org/derby/docs/10.15/ref/index.html
  private val supportedAggregateFunctions = Set("MAX", "MIN", "SUM", "COUNT", "AVG",
    "VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP")
  private val supportedFunctions = supportedAggregateFunctions

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.REAL) Option(FloatType) else None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case _: StringType => Option(JdbcType("CLOB", java.sql.Types.CLOB))
    case ByteType => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case ShortType => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case BooleanType => Option(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    // 31 is the maximum precision
    // https://db.apache.org/derby/docs/10.13/ref/rrefsqlj15260.html
    case t: DecimalType =>
      val (p, s) = if (t.precision > 31) {
        (31, math.max(t.scale - (t.precision - 31), 0))
      } else {
        (t.precision, t.scale)
      }
      Option(JdbcType(s"DECIMAL($p,$s)", java.sql.Types.DECIMAL))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  // See https://db.apache.org/derby/docs/10.15/ref/rrefsqljrenametablestatement.html
  override def renameTable(oldTable: Identifier, newTable: Identifier): String = {
    if (!oldTable.namespace().sameElements(newTable.namespace())) {
      throw QueryCompilationErrors.cannotRenameTableAcrossSchemaError()
    }
    // New table name restriction:
    // https://db.apache.org/derby/docs/10.2/ref/rrefnewtablename.html#rrefnewtablename
    s"RENAME TABLE ${getFullyQualifiedQuotedTableName(oldTable)} TO ${newTable.name()}"
  }

  // Derby currently doesn't support comment on table. Here is the ticket to add the support
  // https://issues.apache.org/jira/browse/DERBY-7008
  override def getTableCommentQuery(table: String, comment: String): String = {
    throw QueryExecutionErrors.commentOnTableUnsupportedError()
  }

  // Derby Support 2 types of clauses for nullability constraint alteration
  //   columnName { SET | DROP } NOT NULL
  //   columnName [ NOT ] NULL
  // Here we use the 2nd one
  // For more information, https://db.apache.org/derby/docs/10.16/ref/rrefsqlj81859.html
  override def getUpdateColumnNullabilityQuery(
      tableName: String,
      columnName: String,
      isNullable: Boolean): String = {
    val nullable = if (isNullable) "NULL" else "NOT NULL"
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} $nullable"
  }

  override def getLimitClause(limit: Integer): String = {
    ""
  }
}
