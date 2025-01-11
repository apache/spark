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

import java.sql.{Date, SQLException, Timestamp, Types}
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.{SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.OracleDialect._
import org.apache.spark.sql.types._


private case class OracleDialect() extends JdbcDialect with SQLConfHelper with NoLegacyJDBCError {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:oracle")

  private val distinctUnsupportedAggregateFunctions =
    Set("VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP", "COVAR_POP", "COVAR_SAMP", "CORR",
      "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXY")

  // scalastyle:off line.size.limit
  // https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Aggregate-Functions.html#GUID-62BE676B-AF18-4E63-BD14-25206FEA0848
  // scalastyle:on line.size.limit
  private val supportedAggregateFunctions =
    Set("MAX", "MIN", "SUM", "COUNT", "AVG") ++ distinctUnsupportedAggregateFunctions
  private val supportedFunctions = supportedAggregateFunctions

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  class OracleSQLBuilder extends JDBCSQLBuilder {
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
  }

  override def compileExpression(expr: Expression): Option[String] = {
    val oracleSQLBuilder = new OracleSQLBuilder()
    try {
      Some(oracleSQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression", e)
        None
    }
  }

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case Types.NUMERIC =>
        val scale = if (null != md) md.build().getLong("scale") else 0L
        size match {
          // Handle NUMBER fields that have no precision/scale in special way
          // because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
          // For more details, please see
          // https://github.com/apache/spark/pull/8780#issuecomment-145598968
          // and
          // https://github.com/apache/spark/pull/8780#issuecomment-144541760
          case 0 => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
          // Handle FLOAT fields in a special way because JDBC ResultSetMetaData converts
          // this to NUMERIC with -127 scale
          // Not sure if there is a more robust way to identify the field as a float (or other
          // numeric types that do not specify a scale.
          case _ if scale == -127L => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
          case _ => None
        }
      case TIMESTAMP_TZ | TIMESTAMP_LTZ =>
        // TIMESTAMP WITH TIME ZONE and TIMESTAMP WITH LOCAL TIME ZONE types can be represented
        // as standard java.sql.Timestamp type.
        // The byte representation of TIMESTAMP WITH TIME ZONE and TIMESTAMP WITH LOCAL TIME ZONE
        // types to java.sql.Timestamp is straight forward.
        // This is because the internal format of TIMESTAMP WITH TIME ZONE and TIMESTAMP WITH LOCAL
        // TIME ZONE data types is GMT, and java.sql.Timestamp type objects internally use a
        // milliseconds time value that is the number of milliseconds since EPOCH.
        Some(TimestampType)
      case BINARY_FLOAT => Some(FloatType) // Value for OracleTypes.BINARY_FLOAT
      case BINARY_DOUBLE => Some(DoubleType) // Value for OracleTypes.BINARY_DOUBLE
      case INTERVAL_YM => Some(YearMonthIntervalType())
      case INTERVAL_DS => Some(DayTimeIntervalType())
      case _ => None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // For more details, please see
    // https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/
    case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.BOOLEAN))
    case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
    case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
    case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
    case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
    case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
    case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
    case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
    case VarcharType(n) => Some(JdbcType(s"VARCHAR2($n)", java.sql.Types.VARCHAR))
    case TimestampType if !conf.legacyOracleTimestampMappingEnabled =>
      Some(JdbcType("TIMESTAMP WITH LOCAL TIME ZONE", TIMESTAMP_LTZ))
    case _ => None
  }

  override def compileValue(value: Any): Any = value match {
    // The JDBC drivers support date literals in SQL statements written in the
    // format: {d 'yyyy-mm-dd'} and timestamp literals in SQL statements written
    // in the format: {ts 'yyyy-mm-dd hh:mm:ss.f...'}. For details, see
    // 'Oracle Database JDBC Developer’s Guide and Reference, 11g Release 1 (11.1)'
    // Appendix A Reference Information.
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "{ts '" + timestampValue + "'}"
    case dateValue: Date => "{d '" + dateValue + "'}"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  /**
   * The SQL query used to truncate a table.
   * @param table The table to truncate
   * @param cascade Whether or not to cascade the truncation. Default value is the
   *                value of isCascadingTruncateTable()
   * @return The SQL query to use for truncating a table
   */
  override def getTruncateQuery(
      table: String,
      cascade: Option[Boolean] = isCascadingTruncateTable()): String = {
    cascade match {
      case Some(true) => s"TRUNCATE TABLE $table CASCADE"
      case _ => s"TRUNCATE TABLE $table"
    }
  }

  // see https://docs.oracle.com/cd/B28359_01/server.111/b28286/statements_3001.htm#SQLRF01001
  override def getAddColumnQuery(
      tableName: String,
      columnName: String,
      dataType: String): String =
    s"ALTER TABLE $tableName ADD ${quoteIdentifier(columnName)} $dataType"

  // see https://docs.oracle.com/cd/B28359_01/server.111/b28286/statements_3001.htm#SQLRF01001
  override def getUpdateColumnTypeQuery(
    tableName: String,
    columnName: String,
    newDataType: String): String =
    s"ALTER TABLE $tableName MODIFY ${quoteIdentifier(columnName)} $newDataType"

  override def getUpdateColumnNullabilityQuery(
    tableName: String,
    columnName: String,
    isNullable: Boolean): String = {
    val nullable = if (isNullable) "NULL" else "NOT NULL"
    s"ALTER TABLE $tableName MODIFY ${quoteIdentifier(columnName)} $nullable"
  }

  override def getLimitClause(limit: Integer): String = {
    // Oracle doesn't support LIMIT clause.
    // We can use rownum <= n to limit the number of rows in the result set.
    if (limit > 0) s"WHERE rownum <= $limit" else ""
  }

  override def getOffsetClause(offset: Integer): String = {
    // Oracle doesn't support OFFSET clause.
    // We can use rownum > n to skip some rows in the result set.
    // Note: rn is an alias of rownum.
    if (offset > 0) s"WHERE rn > $offset" else ""
  }

  class OracleSQLQueryBuilder(dialect: JdbcDialect, options: JDBCOptions)
    extends JdbcSQLQueryBuilder(dialect, options) {

    override def build(): String = {
      val selectStmt = s"SELECT $columnList FROM ${options.tableOrQuery} $tableSampleClause" +
        s" $whereClause $groupByClause $orderByClause"
      val finalSelectStmt = if (limit > 0) {
        if (offset > 0) {
          // Because the rownum is calculated when the value is returned,
          // if we not give an alias for rownum and using it directly, e.g.
          // SELECT $columnList FROM ($selectStmt) tab WHERE rownum > $offset AND
          // rownum <= ${limit + offset}. The result is incorrect.
          s"SELECT $columnList FROM (SELECT tab.*, rownum rn FROM ($selectStmt) tab)" +
            s" WHERE rn > $offset AND rn <= ${limit + offset}"
        } else {
          val limitClause = dialect.getLimitClause(limit)
          s"SELECT tab.* FROM ($selectStmt) tab $limitClause"
        }
      } else if (offset > 0) {
        val offsetClause = dialect.getOffsetClause(offset)
        // Using rownum directly will lead to incorrect result too.
        s"SELECT $columnList FROM (SELECT tab.*, rownum rn FROM ($selectStmt) tab) $offsetClause"
      } else {
        selectStmt
      }

      options.prepareQuery + finalSelectStmt
    }
  }

  override def getJdbcSQLQueryBuilder(options: JDBCOptions): JdbcSQLQueryBuilder =
    new OracleSQLQueryBuilder(this, options)

  override def supportsLimit: Boolean = true

  override def supportsOffset: Boolean = true

  override def classifyException(
      e: Throwable,
      errorClass: String,
      messageParameters: Map[String, String],
      description: String,
      isRuntime: Boolean): Throwable with SparkThrowable = {
    e match {
      case sqlException: SQLException =>
        sqlException.getErrorCode match {
          case 955 if errorClass == "FAILED_JDBC.RENAME_TABLE" =>
            val newTable = messageParameters("newName")
            throw QueryCompilationErrors.tableAlreadyExistsError(newTable)
          case _ =>
            super.classifyException(e, errorClass, messageParameters, description, isRuntime)
        }
      case _ => super.classifyException(e, errorClass, messageParameters, description, isRuntime)
    }
  }
}

private[jdbc] object OracleDialect {
  final val BINARY_FLOAT = 100
  final val BINARY_DOUBLE = 101
  final val TIMESTAMP_TZ = -101
  // oracle.jdbc.OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE
  final val TIMESTAMP_LTZ = -102
  // INTERVAL YEAR [(year_precision)] TO MONTH
  // Stores a period of time in years and months, where year_precision is the number of digits in
  // the YEAR datetime field. Accepted values are 0 to 9. The default is 2.
  // The size is fixed at 5 bytes.
  final val INTERVAL_YM = -103
  // INTERVAL DAY [(day_precision)] TO SECOND [(fractional_seconds_precision)]
  // Stores a period of time in days, hours, minutes, and seconds, where
  // - day_precision is the maximum number of digits in the DAY datetime field.
  //   Accepted values are 0 to 9. The default is 2.
  // - fractional_seconds_precision is the number of digits in the fractional part
  //   of the SECOND field. Accepted values are 0 to 9. The default is 6.
  final val INTERVAL_DS = -104
}
