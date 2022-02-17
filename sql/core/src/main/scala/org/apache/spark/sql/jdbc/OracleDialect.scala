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

import java.sql.{Date, Timestamp, Types}
import java.util.{Locale, TimeZone}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, GeneralAggregateFunc}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


private case object OracleDialect extends JdbcDialect {
  private[jdbc] val BINARY_FLOAT = 100
  private[jdbc] val BINARY_DOUBLE = 101
  private[jdbc] val TIMESTAMPTZ = -101

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:oracle")

  override def compileAggregate(aggFunction: AggregateFunc): Option[String] = {
    super.compileAggregate(aggFunction).orElse(
      aggFunction match {
        case f: GeneralAggregateFunc if f.name() == "VAR_POP" =>
          assert(f.inputs().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"VAR_POP($distinct${f.inputs().head})")
        case f: GeneralAggregateFunc if f.name() == "VAR_SAMP" =>
          assert(f.inputs().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"VAR_SAMP($distinct${f.inputs().head})")
        case f: GeneralAggregateFunc if f.name() == "STDDEV_POP" =>
          assert(f.inputs().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"STDDEV_POP($distinct${f.inputs().head})")
        case f: GeneralAggregateFunc if f.name() == "STDDEV_SAMP" =>
          assert(f.inputs().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"STDDEV_SAMP($distinct${f.inputs().head})")
        case f: GeneralAggregateFunc if f.name() == "COVAR_POP" =>
          assert(f.inputs().length == 2)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"COVAR_POP($distinct${f.inputs().head}, ${f.inputs().last})")
        case f: GeneralAggregateFunc if f.name() == "COVAR_SAMP" =>
          assert(f.inputs().length == 2)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"COVAR_SAMP($distinct${f.inputs().head}, ${f.inputs().last})")
        case f: GeneralAggregateFunc if f.name() == "CORR" =>
          assert(f.inputs().length == 2)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"CORR($distinct${f.inputs().head}, ${f.inputs().last})")
        case _ => None
      }
    )
  }

  private def supportTimeZoneTypes: Boolean = {
    val timeZone = DateTimeUtils.getTimeZone(SQLConf.get.sessionLocalTimeZone)
    // TODO: support timezone types when users are not using the JVM timezone, which
    // is the default value of SESSION_LOCAL_TIMEZONE
    timeZone == TimeZone.getDefault
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
      case TIMESTAMPTZ if supportTimeZoneTypes
        => Some(TimestampType) // Value for Timestamp with Time Zone in Oracle
      case BINARY_FLOAT => Some(FloatType) // Value for OracleTypes.BINARY_FLOAT
      case BINARY_DOUBLE => Some(DoubleType) // Value for OracleTypes.BINARY_DOUBLE
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
      cascade: Option[Boolean] = isCascadingTruncateTable): String = {
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
}
