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

import java.sql.SQLException
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.{Expression, NullOrdering, SortDirection}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.MsSqlServerDialect.{GEOGRAPHY, GEOMETRY}
import org.apache.spark.sql.types._


private case class MsSqlServerDialect() extends JdbcDialect with NoLegacyJDBCError {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:sqlserver")

  // Microsoft SQL Server does not have the boolean type.
  // Compile the boolean value to the bit data type instead.
  // scalastyle:off line.size.limit
  // See https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15
  // scalastyle:on line.size.limit
  override def compileValue(value: Any): Any = value match {
    case booleanValue: Boolean => if (booleanValue) 1 else 0
    case other => super.compileValue(other)
  }

  // scalastyle:off line.size.limit
  // See https://docs.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql?view=sql-server-ver15
  // scalastyle:on line.size.limit
  private val supportedAggregateFunctions = Set("MAX", "MIN", "SUM", "COUNT", "AVG",
    "VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP")
  private val supportedFunctions = supportedAggregateFunctions

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  class MsSqlServerSQLBuilder extends JDBCSQLBuilder {
    override protected def predicateToIntSQL(input: String): String =
      "IIF(" + input + ", 1, 0)"
    override def visitSortOrder(
        sortKey: String, sortDirection: SortDirection, nullOrdering: NullOrdering): String = {
      (sortDirection, nullOrdering) match {
        case (SortDirection.ASCENDING, NullOrdering.NULLS_FIRST) =>
          s"$sortKey $sortDirection"
        case (SortDirection.ASCENDING, NullOrdering.NULLS_LAST) =>
          s"CASE WHEN $sortKey IS NULL THEN 1 ELSE 0 END, $sortKey $sortDirection"
        case (SortDirection.DESCENDING, NullOrdering.NULLS_FIRST) =>
          s"CASE WHEN $sortKey IS NULL THEN 0 ELSE 1 END, $sortKey $sortDirection"
        case (SortDirection.DESCENDING, NullOrdering.NULLS_LAST) =>
          s"$sortKey $sortDirection"
      }
    }

    override def dialectFunctionName(funcName: String): String = funcName match {
      case "VAR_POP" => "VARP"
      case "VAR_SAMP" => "VAR"
      case "STDDEV_POP" => "STDEVP"
      case "STDDEV_SAMP" => "STDEV"
      case _ => super.dialectFunctionName(funcName)
    }

    override def build(expr: Expression): String = {
      // MsSqlServer does not support boolean comparison using standard comparison operators
      // We shouldn't propagate these queries to MsSqlServer
      expr match {
        case e: Predicate => e.name() match {
          case "=" | "<>" | "<=>" | "<" | "<=" | ">" | ">=" =>
            val Array(l, r) = e.children().map(inputToSQLNoBool)
            visitBinaryComparison(e.name(), l, r)
          case "CASE_WHEN" =>
            // Since MsSqlServer cannot handle boolean expressions inside
            // a CASE WHEN, it is necessary to convert those to another
            // CASE WHEN expression that will return 1 or 0 depending on
            // the result.
            // Example:
            // In:  ... CASE WHEN a = b THEN c = d ... END
            // Out: ... CASE WHEN a = b THEN CASE WHEN c = d THEN 1 ELSE 0 END ... END = 1
            val stringArray = e.children().grouped(2).flatMap {
              case Array(whenExpression, thenExpression) =>
                Array(inputToSQL(whenExpression), inputToSQLNoBool(thenExpression))
              case Array(elseExpression) =>
                Array(inputToSQLNoBool(elseExpression))
            }.toArray

            visitCaseWhen(stringArray) + " = 1"
          case _ => super.build(expr)
        }
        case _ => super.build(expr)
      }
    }
  }

  override def compileExpression(expr: Expression): Option[String] = {
    val msSqlServerSQLBuilder = new MsSqlServerSQLBuilder()
    try {
      Some(msSqlServerSQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression", e)
        None
    }
  }

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case _ if typeName.contains("datetimeoffset") =>
        if (SQLConf.get.legacyMsSqlServerDatetimeOffsetMappingEnabled) {
          Some(StringType)
        } else {
          Some(TimestampType)
        }
      case java.sql.Types.SMALLINT | java.sql.Types.TINYINT
          if !SQLConf.get.legacyMsSqlServerNumericMappingEnabled =>
        // Data range of TINYINT is 0-255 so it needs to be stored in ShortType.
        // Reference doc: https://learn.microsoft.com/en-us/sql/t-sql/data-types
        Some(ShortType)
      case java.sql.Types.REAL if !SQLConf.get.legacyMsSqlServerNumericMappingEnabled =>
        Some(FloatType)
      case GEOMETRY | GEOGRAPHY => Some(BinaryType)
      case _ => None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case TimestampType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
    case TimestampNTZType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
    case StringType => Some(JdbcType("NVARCHAR(MAX)", java.sql.Types.NVARCHAR))
    case BooleanType => Some(JdbcType("BIT", java.sql.Types.BIT))
    case BinaryType => Some(JdbcType("VARBINARY(MAX)", java.sql.Types.VARBINARY))
    case ShortType if !SQLConf.get.legacyMsSqlServerNumericMappingEnabled =>
      Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case ByteType => Some(JdbcType("SMALLINT", java.sql.Types.TINYINT))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  // scalastyle:off line.size.limit
  // See https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-rename-transact-sql?view=sql-server-ver15
  // scalastyle:on line.size.limit
  override def renameTable(oldTable: Identifier, newTable: Identifier): String = {
    s"EXEC sp_rename ${getFullyQualifiedQuotedTableName(oldTable)}, " +
      s"${getFullyQualifiedQuotedTableName(newTable)}"
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
    if (limit > 0) s"TOP ($limit)" else ""
  }

  override def classifyException(
      e: Throwable,
      errorClass: String,
      messageParameters: Map[String, String],
      description: String,
      isRuntime: Boolean): Throwable with SparkThrowable = {
    e match {
      case sqlException: SQLException =>
        sqlException.getErrorCode match {
          case 3729 =>
            throw NonEmptyNamespaceException(
              namespace = messageParameters.get("namespace").toArray,
              details = sqlException.getMessage,
              cause = Some(e))
           case 15335 if errorClass == "FAILED_JDBC.RENAME_TABLE" =>
             val newTable = messageParameters("newName")
             throw QueryCompilationErrors.tableAlreadyExistsError(newTable)
          case _ =>
            super.classifyException(e, errorClass, messageParameters, description, isRuntime)
        }
      case _ => super.classifyException(e, errorClass, messageParameters, description, isRuntime)
    }
  }

  class MsSqlServerSQLQueryBuilder(dialect: JdbcDialect, options: JDBCOptions)
    extends JdbcSQLQueryBuilder(dialect, options) {

    override def build(): String = {
      val limitClause = dialect.getLimitClause(limit)

      options.prepareQuery +
        s"SELECT $limitClause $columnList FROM ${options.tableOrQuery} $tableSampleClause" +
        s" $whereClause $groupByClause $orderByClause"
    }
  }

  override def getJdbcSQLQueryBuilder(options: JDBCOptions): JdbcSQLQueryBuilder =
    new MsSqlServerSQLQueryBuilder(this, options)

  override def supportsLimit: Boolean = true
}

private object MsSqlServerDialect {
  // Special JDBC types in Microsoft SQL Server.
  // https://github.com/microsoft/mssql-jdbc/blob/v9.4.1/src/main/java/microsoft/sql/Types.java
  final val GEOMETRY = -157
  final val GEOGRAPHY = -158
}
