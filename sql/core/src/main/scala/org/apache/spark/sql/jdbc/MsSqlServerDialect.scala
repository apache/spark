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

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


private object MsSqlServerDialect extends JdbcDialect {

  // Special JDBC types in Microsoft SQL Server.
  // https://github.com/microsoft/mssql-jdbc/blob/v8.2.2/src/main/java/microsoft/sql/Types.java
  private object SpecificTypes {
    val GEOMETRY = -157
    val GEOGRAPHY = -158
  }

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:sqlserver")

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (typeName.contains("datetimeoffset")) {
      // String is recommend by Microsoft SQL Server for datetimeoffset types in non-MS clients
      Option(StringType)
    } else {
      if (SQLConf.get.legacyMsSqlServerNumericMappingEnabled) {
        None
      } else {
        sqlType match {
          case java.sql.Types.SMALLINT => Some(ShortType)
          case java.sql.Types.REAL => Some(FloatType)
          case SpecificTypes.GEOMETRY | SpecificTypes.GEOGRAPHY => Some(BinaryType)
          case _ => None
        }
      }
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case TimestampType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
    case StringType => Some(JdbcType("NVARCHAR(MAX)", java.sql.Types.NVARCHAR))
    case BooleanType => Some(JdbcType("BIT", java.sql.Types.BIT))
    case BinaryType => Some(JdbcType("VARBINARY(MAX)", java.sql.Types.VARBINARY))
    case ShortType if !SQLConf.get.legacyMsSqlServerNumericMappingEnabled =>
      Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  // scalastyle:off line.size.limit
  // See https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-rename-transact-sql?view=sql-server-ver15
  // scalastyle:on line.size.limit
  override def renameTable(oldTable: String, newTable: String): String = {
    s"EXEC sp_rename $oldTable, $newTable"
  }

  // scalastyle:off line.size.limit
  // see https://docs.microsoft.com/en-us/sql/relational-databases/tables/add-columns-to-a-table-database-engine?view=sql-server-ver15
  // scalastyle:on line.size.limit
  override def getAddColumnQuery(
      tableName: String,
      columnName: String,
      dataType: String): String = {
    s"ALTER TABLE $tableName ADD ${quoteIdentifier(columnName)} $dataType"
  }

  // scalastyle:off line.size.limit
  // See https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-rename-transact-sql?view=sql-server-ver15
  // scalastyle:on line.size.limit
  override def getRenameColumnQuery(
      tableName: String,
      columnName: String,
      newName: String,
      dbMajorVersion: Int): String = {
    s"EXEC sp_rename '$tableName.${quoteIdentifier(columnName)}'," +
      s" ${quoteIdentifier(newName)}, 'COLUMN'"
  }

  // scalastyle:off line.size.limit
  // see https://docs.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-ver15
  // scalastyle:on line.size.limit
  // require to have column data type to change the column nullability
  // ALTER TABLE tbl_name ALTER COLUMN col_name datatype [NULL | NOT NULL]
  // column_definition:
  //    data_type [NOT NULL | NULL]
  // We don't have column data type here, so we throw Exception for now
  override def getUpdateColumnNullabilityQuery(
      tableName: String,
      columnName: String,
      isNullable: Boolean): String = {
    throw QueryExecutionErrors.unsupportedUpdateColumnNullabilityError()
  }

  // scalastyle:off line.size.limit
  // https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-addextendedproperty-transact-sql?redirectedfrom=MSDN&view=sql-server-ver15
  // scalastyle:on line.size.limit
  // need to use the stored procedure called sp_addextendedproperty to add comments to tables
  override def getTableCommentQuery(table: String, comment: String): String = {
    throw QueryExecutionErrors.commentOnTableUnsupportedError()
  }

  override def getLimitClause(limit: Integer): String = {
    ""
  }
}
