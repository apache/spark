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

import java.sql.{Connection, Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util

import scala.collection.mutable.ArrayBuilder

import org.apache.commons.lang3.StringUtils

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.catalog.index.TableIndex
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Count, CountStar, Max, Min, Sum}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * :: DeveloperApi ::
 * A database type definition coupled with the jdbc type needed to send null
 * values to the database.
 * @param databaseTypeDefinition The database type definition
 * @param jdbcNullType The jdbc type (as defined in java.sql.Types) used to
 *                     send a null value to the database.
 */
@DeveloperApi
case class JdbcType(databaseTypeDefinition : String, jdbcNullType : Int)

/**
 * :: DeveloperApi ::
 * Encapsulates everything (extensions, workarounds, quirks) to handle the
 * SQL dialect of a certain database or jdbc driver.
 * Lots of databases define types that aren't explicitly supported
 * by the JDBC spec.  Some JDBC drivers also report inaccurate
 * information---for instance, BIT(n{@literal >}1) being reported as a BIT type is quite
 * common, even though BIT in JDBC is meant for single-bit values. Also, there
 * does not appear to be a standard name for an unbounded string or binary
 * type; we use BLOB and CLOB by default but override with database-specific
 * alternatives when these are absent or do not behave correctly.
 *
 * Currently, the only thing done by the dialect is type mapping.
 * `getCatalystType` is used when reading from a JDBC table and `getJDBCType`
 * is used when writing to a JDBC table.  If `getCatalystType` returns `null`,
 * the default type handling is used for the given JDBC type.  Similarly,
 * if `getJDBCType` returns `(null, None)`, the default type handling is used
 * for the given Catalyst type.
 */
@DeveloperApi
abstract class JdbcDialect extends Serializable with Logging{
  /**
   * Check if this dialect instance can handle a certain jdbc url.
   * @param url the jdbc url.
   * @return True if the dialect can be applied on the given jdbc url.
   * @throws NullPointerException if the url is null.
   */
  def canHandle(url : String): Boolean

  /**
   * Get the custom datatype mapping for the given jdbc meta information.
   * @param sqlType The sql type (see java.sql.Types)
   * @param typeName The sql type name (e.g. "BIGINT UNSIGNED")
   * @param size The size of the type.
   * @param md Result metadata associated with this type.
   * @return The actual DataType (subclasses of [[org.apache.spark.sql.types.DataType]])
   *         or null if the default type mapping should be used.
   */
  def getCatalystType(
    sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = None

  /**
   * Retrieve the jdbc / sql type for a given datatype.
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The new JdbcType if there is an override for this DataType
   */
  def getJDBCType(dt: DataType): Option[JdbcType] = None

  /**
   * Quotes the identifier. This is used to put quotes around the identifier in case the column
   * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
   */
  def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }

  /**
   * Get the SQL query that should be used to find if the given table exists. Dialects can
   * override this method to return a query that works best in a particular database.
   * @param table  The name of the table.
   * @return The SQL query to use for checking the table.
   */
  def getTableExistsQuery(table: String): String = {
    s"SELECT * FROM $table WHERE 1=0"
  }

  /**
   * The SQL query that should be used to discover the schema of a table. It only needs to
   * ensure that the result set has the same schema as the table, such as by calling
   * "SELECT * ...". Dialects can override this method to return a query that works best in a
   * particular database.
   * @param table The name of the table.
   * @return The SQL query to use for discovering the schema.
   */
  @Since("2.1.0")
  def getSchemaQuery(table: String): String = {
    s"SELECT * FROM $table WHERE 1=0"
  }

  /**
   * The SQL query that should be used to truncate a table. Dialects can override this method to
   * return a query that is suitable for a particular database. For PostgreSQL, for instance,
   * a different query is used to prevent "TRUNCATE" affecting other tables.
   * @param table The table to truncate
   * @return The SQL query to use for truncating a table
   */
  @Since("2.3.0")
  def getTruncateQuery(table: String): String = {
    getTruncateQuery(table, isCascadingTruncateTable)
  }

  /**
   * The SQL query that should be used to truncate a table. Dialects can override this method to
   * return a query that is suitable for a particular database. For PostgreSQL, for instance,
   * a different query is used to prevent "TRUNCATE" affecting other tables.
   * @param table The table to truncate
   * @param cascade Whether or not to cascade the truncation
   * @return The SQL query to use for truncating a table
   */
  @Since("2.4.0")
  def getTruncateQuery(
    table: String,
    cascade: Option[Boolean] = isCascadingTruncateTable): String = {
      s"TRUNCATE TABLE $table"
  }

  /**
   * Override connection specific properties to run before a select is made.  This is in place to
   * allow dialects that need special treatment to optimize behavior.
   * @param connection The connection object
   * @param properties The connection properties.  This is passed through from the relation.
   */
  def beforeFetch(connection: Connection, properties: Map[String, String]): Unit = {
  }

  /**
   * Escape special characters in SQL string literals.
   * @param value The string to be escaped.
   * @return Escaped string.
   */
  @Since("2.3.0")
  protected[jdbc] def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  /**
   * Converts value to SQL expression.
   * @param value The value to be converted.
   * @return Converted value.
   */
  @Since("2.3.0")
  def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case timestampValue: Instant =>
      val timestampFormatter = TimestampFormatter.getFractionFormatter(
        DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
      s"'${timestampFormatter.format(timestampValue)}'"
    case dateValue: Date => "'" + dateValue + "'"
    case dateValue: LocalDate => s"'${DateFormatter().format(dateValue)}'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  /**
   * Converts aggregate function to String representing a SQL expression.
   * @param aggFunction The aggregate function to be converted.
   * @return Converted value.
   */
  @Since("3.3.0")
  def compileAggregate(aggFunction: AggregateFunc): Option[String] = {
    aggFunction match {
      case min: Min =>
        if (min.column.fieldNames.length != 1) return None
        Some(s"MIN(${quoteIdentifier(min.column.fieldNames.head)})")
      case max: Max =>
        if (max.column.fieldNames.length != 1) return None
        Some(s"MAX(${quoteIdentifier(max.column.fieldNames.head)})")
      case count: Count =>
        if (count.column.fieldNames.length != 1) return None
        val distinct = if (count.isDistinct) "DISTINCT " else ""
        val column = quoteIdentifier(count.column.fieldNames.head)
        Some(s"COUNT($distinct$column)")
      case sum: Sum =>
        if (sum.column.fieldNames.length != 1) return None
        val distinct = if (sum.isDistinct) "DISTINCT " else ""
        val column = quoteIdentifier(sum.column.fieldNames.head)
        Some(s"SUM($distinct$column)")
      case _: CountStar =>
        Some(s"COUNT(*)")
      case _ => None
    }
  }

  /**
   * Return Some[true] iff `TRUNCATE TABLE` causes cascading default.
   * Some[true] : TRUNCATE TABLE causes cascading.
   * Some[false] : TRUNCATE TABLE does not cause cascading.
   * None: The behavior of TRUNCATE TABLE is unknown (default).
   */
  def isCascadingTruncateTable(): Option[Boolean] = None

  /**
   * Rename an existing table.
   *
   * @param oldTable The existing table.
   * @param newTable New name of the table.
   * @return The SQL statement to use for renaming the table.
   */
  def renameTable(oldTable: String, newTable: String): String = {
    s"ALTER TABLE $oldTable RENAME TO $newTable"
  }

  /**
   * Alter an existing table.
   *
   * @param tableName The name of the table to be altered.
   * @param changes Changes to apply to the table.
   * @return The SQL statements to use for altering the table.
   */
  def alterTable(
      tableName: String,
      changes: Seq[TableChange],
      dbMajorVersion: Int): Array[String] = {
    val updateClause = ArrayBuilder.make[String]
    for (change <- changes) {
      change match {
        case add: AddColumn if add.fieldNames.length == 1 =>
          val dataType = JdbcUtils.getJdbcType(add.dataType(), this).databaseTypeDefinition
          val name = add.fieldNames
          updateClause += getAddColumnQuery(tableName, name(0), dataType)
        case rename: RenameColumn if rename.fieldNames.length == 1 =>
          val name = rename.fieldNames
          updateClause += getRenameColumnQuery(tableName, name(0), rename.newName, dbMajorVersion)
        case delete: DeleteColumn if delete.fieldNames.length == 1 =>
          val name = delete.fieldNames
          updateClause += getDeleteColumnQuery(tableName, name(0))
        case updateColumnType: UpdateColumnType if updateColumnType.fieldNames.length == 1 =>
          val name = updateColumnType.fieldNames
          val dataType = JdbcUtils.getJdbcType(updateColumnType.newDataType(), this)
            .databaseTypeDefinition
          updateClause += getUpdateColumnTypeQuery(tableName, name(0), dataType)
        case updateNull: UpdateColumnNullability if updateNull.fieldNames.length == 1 =>
          val name = updateNull.fieldNames
          updateClause += getUpdateColumnNullabilityQuery(tableName, name(0), updateNull.nullable())
        case _ =>
          throw QueryCompilationErrors.unsupportedTableChangeInJDBCCatalogError(change)
      }
    }
    updateClause.result()
  }

  def getAddColumnQuery(tableName: String, columnName: String, dataType: String): String =
    s"ALTER TABLE $tableName ADD COLUMN ${quoteIdentifier(columnName)} $dataType"

  def getRenameColumnQuery(
      tableName: String,
      columnName: String,
      newName: String,
      dbMajorVersion: Int): String =
    s"ALTER TABLE $tableName RENAME COLUMN ${quoteIdentifier(columnName)} TO" +
      s" ${quoteIdentifier(newName)}"

  def getDeleteColumnQuery(tableName: String, columnName: String): String =
    s"ALTER TABLE $tableName DROP COLUMN ${quoteIdentifier(columnName)}"

  def getUpdateColumnTypeQuery(
      tableName: String,
      columnName: String,
      newDataType: String): String =
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} $newDataType"

  def getUpdateColumnNullabilityQuery(
      tableName: String,
      columnName: String,
      isNullable: Boolean): String = {
    val nullable = if (isNullable) "NULL" else "NOT NULL"
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} SET $nullable"
  }

  def getTableCommentQuery(table: String, comment: String): String = {
    s"COMMENT ON TABLE $table IS '$comment'"
  }

  def getSchemaCommentQuery(schema: String, comment: String): String = {
    s"COMMENT ON SCHEMA ${quoteIdentifier(schema)} IS '$comment'"
  }

  def removeSchemaCommentQuery(schema: String): String = {
    s"COMMENT ON SCHEMA ${quoteIdentifier(schema)} IS NULL"
  }

  /**
   * Build a create index SQL statement.
   *
   * @param indexName         the name of the index to be created
   * @param tableName         the table on which index to be created
   * @param columns           the columns on which index to be created
   * @param columnsProperties the properties of the columns on which index to be created
   * @param properties        the properties of the index to be created
   * @return                  the SQL statement to use for creating the index.
   */
  def createIndex(
      indexName: String,
      tableName: String,
      columns: Array[NamedReference],
      columnsProperties: util.Map[NamedReference, util.Map[String, String]],
      properties: util.Map[String, String]): String = {
    throw new UnsupportedOperationException("createIndex is not supported")
  }

  /**
   * Checks whether an index exists
   *
   * @param indexName the name of the index
   * @param tableName the table name on which index to be checked
   * @param options JDBCOptions of the table
   * @return true if the index with `indexName` exists in the table with `tableName`,
   *         false otherwise
   */
  def indexExists(
      conn: Connection,
      indexName: String,
      tableName: String,
      options: JDBCOptions): Boolean = {
    throw new UnsupportedOperationException("indexExists is not supported")
  }

  /**
   * Build a drop index SQL statement.
   *
   * @param indexName the name of the index to be dropped.
   * @param tableName the table name on which index to be dropped.
  * @return the SQL statement to use for dropping the index.
   */
  def dropIndex(indexName: String, tableName: String): String = {
    throw new UnsupportedOperationException("dropIndex is not supported")
  }

  /**
   * Lists all the indexes in this table.
   */
  def listIndexes(
      conn: Connection,
      tableName: String,
      options: JDBCOptions): Array[TableIndex] = {
    throw new UnsupportedOperationException("listIndexes is not supported")
  }

  /**
   * Gets a dialect exception, classifies it and wraps it by `AnalysisException`.
   * @param message The error message to be placed to the returned exception.
   * @param e The dialect specific exception.
   * @return `AnalysisException` or its sub-class.
   */
  def classifyException(message: String, e: Throwable): AnalysisException = {
    new AnalysisException(message, cause = Some(e))
  }

  /**
   * returns the LIMIT clause for the SELECT statement
   */
  def getLimitClause(limit: Integer): String = {
    if (limit > 0 ) s"LIMIT $limit" else ""
  }

  def supportsTableSample: Boolean = false

  def getTableSample(sample: TableSampleInfo): String =
    throw new UnsupportedOperationException("TableSample is not supported by this data source")
}

/**
 * :: DeveloperApi ::
 * Registry of dialects that apply to every new jdbc `org.apache.spark.sql.DataFrame`.
 *
 * If multiple matching dialects are registered then all matching ones will be
 * tried in reverse order. A user-added dialect will thus be applied first,
 * overwriting the defaults.
 *
 * @note All new dialects are applied to new jdbc DataFrames only. Make
 * sure to register your dialects first.
 */
@DeveloperApi
object JdbcDialects {

  /**
   * Register a dialect for use on all new matching jdbc `org.apache.spark.sql.DataFrame`.
   * Reading an existing dialect will cause a move-to-front.
   *
   * @param dialect The new dialect.
   */
  def registerDialect(dialect: JdbcDialect) : Unit = {
    dialects = dialect :: dialects.filterNot(_ == dialect)
  }

  /**
   * Unregister a dialect. Does nothing if the dialect is not registered.
   *
   * @param dialect The jdbc dialect.
   */
  def unregisterDialect(dialect : JdbcDialect) : Unit = {
    dialects = dialects.filterNot(_ == dialect)
  }

  private[this] var dialects = List[JdbcDialect]()

  registerDialect(MySQLDialect)
  registerDialect(PostgresDialect)
  registerDialect(DB2Dialect)
  registerDialect(MsSqlServerDialect)
  registerDialect(DerbyDialect)
  registerDialect(OracleDialect)
  registerDialect(TeradataDialect)
  registerDialect(H2Dialect)

  /**
   * Fetch the JdbcDialect class corresponding to a given database url.
   */
  def get(url: String): JdbcDialect = {
    val matchingDialects = dialects.filter(_.canHandle(url))
    matchingDialects.length match {
      case 0 => NoopDialect
      case 1 => matchingDialects.head
      case _ => new AggregatedDialect(matchingDialects)
    }
  }
}

/**
 * NOOP dialect object, always returning the neutral element.
 */
private object NoopDialect extends JdbcDialect {
  override def canHandle(url : String): Boolean = true
}
