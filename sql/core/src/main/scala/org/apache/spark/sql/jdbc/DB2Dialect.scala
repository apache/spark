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

import java.sql.{SQLException, Types}
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.{SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types._

private case class DB2Dialect() extends JdbcDialect with SQLConfHelper with NoLegacyJDBCError {

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:db2")

  private val distinctUnsupportedAggregateFunctions =
    Set("COVAR_POP", "COVAR_SAMP", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXY")

  // See https://www.ibm.com/docs/en/db2/11.5?topic=functions-aggregate
  private val supportedAggregateFunctions = Set("MAX", "MIN", "SUM", "COUNT", "AVG",
    "VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP") ++ distinctUnsupportedAggregateFunctions
  private val supportedFunctions = supportedAggregateFunctions

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  class DB2SQLBuilder extends JDBCSQLBuilder {
    override def visitAggregateFunction(
        funcName: String, isDistinct: Boolean, inputs: Array[String]): String =
      if (isDistinct && distinctUnsupportedAggregateFunctions.contains(funcName)) {
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3184",
          messageParameters = Map(
            "class" -> this.getClass.getSimpleName,
            "funcName" -> funcName))
      } else {
        super.visitAggregateFunction(funcName, isDistinct, inputs)
      }

    override def dialectFunctionName(funcName: String): String = funcName match {
      case "VAR_POP" => "VARIANCE"
      case "VAR_SAMP" => "VARIANCE_SAMP"
      case "STDDEV_POP" => "STDDEV"
      case "STDDEV_SAMP" => "STDDEV_SAMP"
      case "COVAR_POP" => "COVARIANCE"
      case "COVAR_SAMP" => "COVARIANCE_SAMP"
      case _ => super.dialectFunctionName(funcName)
    }
  }

  override def compileExpression(expr: Expression): Option[String] = {
    val db2SQLBuilder = new DB2SQLBuilder()
    try {
      Some(db2SQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression", e)
        None
    }
  }

  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder): Option[DataType] = sqlType match {
    case Types.SMALLINT if !conf.legacyDB2numericMappingEnabled =>
      Option(ShortType)
    case Types.REAL => Option(FloatType)
    case Types.OTHER =>
      typeName match {
        case "DECFLOAT" => Option(DecimalType(38, 18))
        case "XML" => Option(StringType)
        case t if (t.startsWith("TIMESTAMP")) => Option(TimestampType) // TIMESTAMP WITH TIMEZONE
        case _ => None
      }
    case _ => None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Option(JdbcType("CLOB", java.sql.Types.CLOB))
    case BooleanType if conf.legacyDB2BooleanMappingEnabled =>
      Option(JdbcType("CHAR(1)", java.sql.Types.CHAR))
    case BooleanType => Option(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case ShortType | ByteType => Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  // scalastyle:off line.size.limit
  // See https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0053474.html
  // scalastyle:on line.size.limit
  override def getTruncateQuery(
      table: String,
      cascade: Option[Boolean] = isCascadingTruncateTable()): String = {
    s"TRUNCATE TABLE $table IMMEDIATE"
  }

  // scalastyle:off line.size.limit
  // See https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0000980.html
  // scalastyle:on line.size.limit
  override def renameTable(oldTable: Identifier, newTable: Identifier): String = {
    s"RENAME TABLE ${getFullyQualifiedQuotedTableName(oldTable)} TO " +
      s"${getFullyQualifiedQuotedTableName(newTable)}"
  }

  // scalastyle:off line.size.limit
  // See https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0000888.html
  // scalastyle:on line.size.limit
  override def getUpdateColumnTypeQuery(
      tableName: String,
      columnName: String,
      newDataType: String): String =
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)}" +
      s" SET DATA TYPE $newDataType"

  // scalastyle:off line.size.limit
  // See https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0000888.html
  // scalastyle:on line.size.limit
  override def getUpdateColumnNullabilityQuery(
      tableName: String,
      columnName: String,
      isNullable: Boolean): String = {
    val nullable = if (isNullable) "DROP NOT NULL" else "SET NOT NULL"
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} $nullable"
  }

  override def removeSchemaCommentQuery(schema: String): String = {
    s"COMMENT ON SCHEMA ${quoteIdentifier(schema)} IS ''"
  }


  override def classifyException(
      e: Throwable,
      errorClass: String,
      messageParameters: Map[String, String],
      description: String,
      isRuntime: Boolean): Throwable with SparkThrowable = {
    e match {
      case sqlException: SQLException =>
        sqlException.getSQLState match {
          // https://www.ibm.com/docs/en/db2/11.5?topic=messages-sqlstate
          case "42893" =>
            throw NonEmptyNamespaceException(
              namespace = messageParameters.get("namespace").toArray,
              details = sqlException.getMessage,
              cause = Some(e))
          case "42710" if errorClass == "FAILED_JDBC.RENAME_TABLE" =>
            val newTable = messageParameters("newName")
            throw QueryCompilationErrors.tableAlreadyExistsError(newTable)
          case _ =>
            super.classifyException(e, errorClass, messageParameters, description, isRuntime)
        }
      case _ => super.classifyException(e, errorClass, messageParameters, description, isRuntime)
    }
  }

  override def dropSchema(schema: String, cascade: Boolean): String = {
    if (cascade) {
      s"DROP SCHEMA ${quoteIdentifier(schema)} CASCADE"
    } else {
      s"DROP SCHEMA ${quoteIdentifier(schema)} RESTRICT"
    }
  }

  override def getLimitClause(limit: Integer): String = {
    if (limit > 0) s"FETCH FIRST $limit ROWS ONLY" else ""
  }

  override def getOffsetClause(offset: Integer): String = {
    if (offset > 0) s"OFFSET $offset ROWS" else ""
  }

  class DB2SQLQueryBuilder(dialect: JdbcDialect, options: JDBCOptions)
    extends JdbcSQLQueryBuilder(dialect, options) {

    override def build(): String = {
      val limitClause = dialect.getLimitClause(limit)
      val offsetClause = dialect.getOffsetClause(offset)

      options.prepareQuery +
        s"SELECT $columnList FROM ${options.tableOrQuery} $tableSampleClause" +
        s" $whereClause $groupByClause $orderByClause $offsetClause $limitClause"
    }
  }

  override def getJdbcSQLQueryBuilder(options: JDBCOptions): JdbcSQLQueryBuilder =
    new DB2SQLQueryBuilder(this, options)

  override def supportsLimit: Boolean = true

  override def supportsOffset: Boolean = true
}
