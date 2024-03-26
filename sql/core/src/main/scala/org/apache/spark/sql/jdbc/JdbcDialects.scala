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

import java.sql.{Connection, Date, Driver, Statement, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime}
import java.util
import java.util.ServiceLoader

import scala.collection.mutable.ArrayBuilder
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{localDateTimeToMicros, toJavaTimestampNoRebase}
import org.apache.spark.sql.connector.catalog.{Identifier, TableChange}
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog.index.TableIndex
import org.apache.spark.sql.connector.expressions.{Expression, Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.execution.datasources.jdbc.connection.ConnectionProvider
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

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
abstract class JdbcDialect extends Serializable with Logging {
  /**
   * Check if this dialect instance can handle a certain jdbc url.
   * @param url the jdbc url.
   * @return True if the dialect can be applied on the given jdbc url.
   * @throws NullPointerException if the url is null.
   */
  def canHandle(url : String): Boolean

  /**
   * Get the custom datatype mapping for the given jdbc meta information.
   *
   * Guidelines for mapping database defined timestamps to Spark SQL timestamps:
   * <ul>
   *   <li>
   *     TIMESTAMP WITHOUT TIME ZONE if preferTimestampNTZ ->
   *     [[org.apache.spark.sql.types.TimestampNTZType]]
   *   </li>
   *   <li>
   *     TIMESTAMP WITHOUT TIME ZONE if !preferTimestampNTZ ->
   *     [[org.apache.spark.sql.types.TimestampType]](LTZ)
   *   </li>
   *   <li>TIMESTAMP WITH TIME ZONE -> [[org.apache.spark.sql.types.TimestampType]](LTZ)</li>
   *   <li>TIMESTAMP WITH LOCAL TIME ZONE -> [[org.apache.spark.sql.types.TimestampType]](LTZ)</li>
   *   <li>
   *     If the TIMESTAMP cannot be distinguished by `sqlType` and `typeName`, preferTimestampNTZ
   *     is respected for now, but we may need to add another option in the future if necessary.
   *   </li>
   * </ul>
   *
   * @param sqlType Refers to [[java.sql.Types]] constants, or other constants defined by the
   *                target database, e.g. `-101` is Oracle's TIMESTAMP WITH TIME ZONE type.
   *                This value is returned by [[java.sql.ResultSetMetaData#getColumnType]].
   * @param typeName The column type name used by the database (e.g. "BIGINT UNSIGNED"). This is
   *                 sometimes used to determine the target data type when `sqlType` is not
   *                 sufficient if multiple database types are conflated into a single id.
   *                 This value is returned by [[java.sql.ResultSetMetaData#getColumnTypeName]].
   * @param size The size of the type, e.g. the maximum precision for numeric types, length for
   *             character string, etc.
   *             This value is returned by [[java.sql.ResultSetMetaData#getPrecision]].
   * @param md Result metadata associated with this type. This contains additional information
   *           from [[java.sql.ResultSetMetaData]] or user specified options.
   *           <ul>
   *             <li>
   *               `isTimestampNTZ`: Whether read a TIMESTAMP WITHOUT TIME ZONE value as
   *               [[org.apache.spark.sql.types.TimestampNTZType]] or not. This is configured by
   *               `JDBCOptions.preferTimestampNTZ`.
   *             </li>
   *             <li>
   *               `scale`: The length of fractional part [[java.sql.ResultSetMetaData#getScale]]
   *             </li>
   *            </ul>
   * @return An option the actual DataType (subclasses of [[org.apache.spark.sql.types.DataType]])
   *         or None if the default type mapping should be used.
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
   * Converts an instance of `java.sql.Timestamp` to a custom `java.sql.Timestamp` value.
   * @param t represents a specific instant in time based on
   *          the hybrid calendar which combines Julian and
   *          Gregorian calendars.
   * @return the timestamp value to convert to
   * @throws IllegalArgumentException if t is null
   */
  @Since("3.5.0")
  def convertJavaTimestampToTimestamp(t: Timestamp): Timestamp = t

  /**
   * Converts an instance of `java.sql.Date` to a custom `java.sql.Date` value.
   * @param d the date value returned from JDBC ResultSet getDate method.
   * @return the date value after conversion
   */
  @Since("4.0.0")
  def convertJavaDateToDate(d: Date): Date = d

  /**
   * Convert java.sql.Timestamp to a LocalDateTime representing the same wall-clock time as the
   * value stored in a remote database.
   * JDBC dialects should override this function to provide implementations that suit their
   * JDBC drivers.
   * @param t Timestamp returned from JDBC driver getTimestamp method.
   * @return A LocalDateTime representing the same wall clock time as the timestamp in database.
   */
  @Since("3.5.0")
  def convertJavaTimestampToTimestampNTZ(t: Timestamp): LocalDateTime = {
    DateTimeUtils.microsToLocalDateTime(DateTimeUtils.fromJavaTimestampNoRebase(t))
  }

  /**
   * Converts a LocalDateTime representing a TimestampNTZ type to an
   * instance of `java.sql.Timestamp`.
   * @param ldt representing a TimestampNTZType.
   * @return A Java Timestamp representing this LocalDateTime.
   */
  @Since("3.5.0")
  def convertTimestampNTZToJavaTimestamp(ldt: LocalDateTime): Timestamp = {
    val micros = localDateTimeToMicros(ldt)
    toJavaTimestampNoRebase(micros)
  }

  /**
   * Returns a factory for creating connections to the given JDBC URL.
   * In general, creating a connection has nothing to do with JDBC partition id.
   * But sometimes it is needed, such as a database with multiple shard nodes.
   * @param options - JDBC options that contains url, table and other information.
   * @return The factory method for creating JDBC connections with the RDD partition ID. -1 means
             the connection is being created at the driver side.
   * @throws IllegalArgumentException if the driver could not open a JDBC connection.
   */
  @Since("3.3.0")
  def createConnectionFactory(options: JDBCOptions): Int => Connection = {
    val driverClass: String = options.driverClass
    (partitionId: Int) => {
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverRegistry.get(driverClass)
      val connection =
        ConnectionProvider.create(driver, options.parameters, options.connectionProviderName)
      require(connection != null,
        s"The driver could not open a JDBC connection. Check the URL: ${options.getRedactUrl()}")
      connection
    }
  }

  /**
   * Quotes the identifier. This is used to put quotes around the identifier in case the column
   * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
   */
  def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }

  /**
   * Create the table if the table does not exist.
   * To allow certain options to append when create a new table, which can be
   * table_options or partition_options.
   * E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
   *
   * @param statement The Statement object used to execute SQL statements.
   * @param tableName The name of the table to be created.
   * @param strSchema The schema of the table to be created.
   * @param options The JDBC options. It contains the create table option, which can be
   *                table_options or partition_options.
   */
  def createTable(
      statement: Statement,
      tableName: String,
      strSchema: String,
      options: JdbcOptionsInWrite): Unit = {
    val createTableOptions = options.createTableOptions
    statement.executeUpdate(s"CREATE TABLE $tableName ($strSchema) $createTableOptions")
  }

  /**
   * Returns an Insert SQL statement template for inserting a row into the target table via JDBC
   * conn. Use "?" as placeholder for each value to be inserted.
   * E.g. `INSERT INTO t ("name", "age", "gender") VALUES (?, ?, ?)`
   *
   * @param table The name of the table.
   * @param fields The fields of the row that will be inserted.
   * @return The SQL query to use for insert data into table.
   */
  @Since("4.0.0")
  def insertIntoTable(
      table: String,
      fields: Array[StructField]): String = {
    val placeholders = fields.map(_ => "?").mkString(",")
    val columns = fields.map(x => quoteIdentifier(x.name)).mkString(",")
    s"INSERT INTO $table ($columns) VALUES ($placeholders)"
  }

  /**
   * Get the SQL query that should be used to find if the given table exists. Dialects can
   * override this method to return a query that works best in a particular database.
   * @param table  The name of the table.
   * @return The SQL query to use for checking the table.
   */
  def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table WHERE 1=0"
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
    getTruncateQuery(table, isCascadingTruncateTable())
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
    cascade: Option[Boolean] = isCascadingTruncateTable()): String = {
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
    case timestampValue: LocalDateTime =>
      val timestampFormatter = TimestampFormatter.getFractionFormatter(
        DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
      s"'${timestampFormatter.format(timestampValue)}'"
    case timestampValue: Instant =>
      val timestampFormatter = TimestampFormatter.getFractionFormatter(
        DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
      s"'${timestampFormatter.format(timestampValue)}'"
    case dateValue: Date => "'" + dateValue + "'"
    case dateValue: LocalDate => s"'${DateFormatter().format(dateValue)}'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  private[jdbc] class JDBCSQLBuilder extends V2ExpressionSQLBuilder {
    override def visitLiteral(literal: Literal[_]): String = {
      Option(literal.value()).map(v =>
        compileValue(CatalystTypeConverters.convertToScala(v, literal.dataType())).toString)
        .getOrElse(super.visitLiteral(literal))
    }

    override def visitNamedReference(namedRef: NamedReference): String = {
      if (namedRef.fieldNames().length > 1) {
        throw QueryCompilationErrors.commandNotSupportNestedColumnError(
          "Filter push down", namedRef.toString)
      }
      quoteIdentifier(namedRef.fieldNames.head)
    }

    override def visitCast(l: String, dataType: DataType): String = {
      val databaseTypeDefinition =
        getJDBCType(dataType).map(_.databaseTypeDefinition).getOrElse(dataType.typeName)
      s"CAST($l AS $databaseTypeDefinition)"
    }

    override def visitSQLFunction(funcName: String, inputs: Array[String]): String = {
      if (isSupportedFunction(funcName)) {
        s"""${dialectFunctionName(funcName)}(${inputs.mkString(", ")})"""
      } else {
        // The framework will catch the error and give up the push-down.
        // Please see `JdbcDialect.compileExpression(expr: Expression)` for more details.
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3177",
          messageParameters = Map(
            "class" -> this.getClass.getSimpleName,
            "funcName" -> funcName))
      }
    }

    override def visitAggregateFunction(
        funcName: String, isDistinct: Boolean, inputs: Array[String]): String = {
      if (isSupportedFunction(funcName)) {
        super.visitAggregateFunction(dialectFunctionName(funcName), isDistinct, inputs)
      } else {
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3177",
          messageParameters = Map(
            "class" -> this.getClass.getSimpleName,
            "funcName" -> funcName))
      }
    }

    override def visitInverseDistributionFunction(
        funcName: String,
        isDistinct: Boolean,
        inputs: Array[String],
        orderingWithinGroups: Array[String]): String = {
      if (isSupportedFunction(funcName)) {
        super.visitInverseDistributionFunction(
          dialectFunctionName(funcName), isDistinct, inputs, orderingWithinGroups)
      } else {
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3178",
          messageParameters = Map(
            "class" -> this.getClass.getSimpleName,
            "funcName" -> funcName))
      }
    }

    protected def dialectFunctionName(funcName: String): String = funcName

    override def visitOverlay(inputs: Array[String]): String = {
      if (isSupportedFunction("OVERLAY")) {
        super.visitOverlay(inputs)
      } else {
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3177",
          messageParameters = Map(
            "class" -> this.getClass.getSimpleName,
            "funcName" -> "OVERLAY"))
      }
    }

    override def visitTrim(direction: String, inputs: Array[String]): String = {
      if (isSupportedFunction("TRIM")) {
        super.visitTrim(direction, inputs)
      } else {
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3177",
          messageParameters = Map(
            "class" -> this.getClass.getSimpleName,
            "funcName" -> "TRIM"))
      }
    }
  }

  /**
   * Returns whether the database supports function.
   * @param funcName Upper-cased function name
   * @return True if the database supports function.
   */
  @Since("3.3.0")
  def isSupportedFunction(funcName: String): Boolean = false

  /**
   * Converts V2 expression to String representing a SQL expression.
   * @param expr The V2 expression to be converted.
   * @return Converted value.
   */
  @Since("3.3.0")
  def compileExpression(expr: Expression): Option[String] = {
    val jdbcSQLBuilder = new JDBCSQLBuilder()
    try {
      Some(jdbcSQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression", e)
        None
    }
  }

  /**
   * Converts aggregate function to String representing a SQL expression.
   * @param aggFunction The aggregate function to be converted.
   * @return Converted value.
   */
  @Since("3.3.0")
  @deprecated("use org.apache.spark.sql.jdbc.JdbcDialect.compileExpression instead.", "3.4.0")
  def compileAggregate(aggFunction: AggregateFunc): Option[String] = compileExpression(aggFunction)

  /**
   * List the user-defined functions in jdbc dialect.
   * @return a sequence of tuple from function name to user-defined function.
   */
  def functions: Seq[(String, UnboundFunction)] = Nil

  /**
   * Create schema with an optional comment. Empty string means no comment.
   */
  def createSchema(statement: Statement, schema: String, comment: String): Unit = {
    val schemaCommentQuery = if (comment.nonEmpty) {
      // We generate comment query here so that it can fail earlier without creating the schema.
      getSchemaCommentQuery(schema, comment)
    } else {
      comment
    }
    statement.executeUpdate(s"CREATE SCHEMA ${quoteIdentifier(schema)}")
    if (comment.nonEmpty) {
      statement.executeUpdate(schemaCommentQuery)
    }
  }

  /**
   * Check schema exists or not.
   */
  def schemasExists(conn: Connection, options: JDBCOptions, schema: String): Boolean = {
    val rs = conn.getMetaData.getSchemas(null, schema)
    while (rs.next()) {
      if (rs.getString(1) == schema) return true;
    }
    false
  }

  /**
   * Lists all the schemas in this table.
   */
  def listSchemas(conn: Connection, options: JDBCOptions): Array[Array[String]] = {
    val schemaBuilder = ArrayBuilder.make[Array[String]]
    val rs = conn.getMetaData.getSchemas()
    while (rs.next()) {
      schemaBuilder += Array(rs.getString(1))
    }
    schemaBuilder.result()
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
  @deprecated("Please override renameTable method with identifiers", "3.5.0")
  def renameTable(oldTable: String, newTable: String): String = {
    s"ALTER TABLE $oldTable RENAME TO $newTable"
  }

  /**
   * Rename an existing table.
   *
   * @param oldTable The existing table.
   * @param newTable New name of the table.
   * @return The SQL statement to use for renaming the table.
   */
  @Since("3.5.0")
  def renameTable(oldTable: Identifier, newTable: Identifier): String = {
    s"ALTER TABLE ${getFullyQualifiedQuotedTableName(oldTable)} RENAME TO " +
      s"${getFullyQualifiedQuotedTableName(newTable)}"
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

  def dropSchema(schema: String, cascade: Boolean): String = {
    if (cascade) {
      s"DROP SCHEMA ${quoteIdentifier(schema)} CASCADE"
    } else {
      s"DROP SCHEMA ${quoteIdentifier(schema)}"
    }
  }

  /**
   * Build a SQL statement to drop the given table.
   *
   * @param table the table name
   * @return The SQL statement to use for drop the table.
   */
  @Since("4.0.0")
  def dropTable(table: String): String = {
    s"DROP TABLE $table"
  }

  /**
   * Build a create index SQL statement.
   *
   * @param indexName         the name of the index to be created
   * @param tableIdent        the table on which index to be created
   * @param columns           the columns on which index to be created
   * @param columnsProperties the properties of the columns on which index to be created
   * @param properties        the properties of the index to be created
   * @return                  the SQL statement to use for creating the index.
   */
  def createIndex(
      indexName: String,
      tableIdent: Identifier,
      columns: Array[NamedReference],
      columnsProperties: util.Map[NamedReference, util.Map[String, String]],
      properties: util.Map[String, String]): String = {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3179")
  }

  /**
   * Checks whether an index exists
   *
   * @param indexName the name of the index
   * @param tableIdent the table on which index to be checked
   * @param options JDBCOptions of the table
   * @return true if the index with `indexName` exists in the table with `tableName`,
   *         false otherwise
   */
  def indexExists(
      conn: Connection,
      indexName: String,
      tableIdent: Identifier,
      options: JDBCOptions): Boolean = {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3180")
  }

  /**
   * Build a drop index SQL statement.
   *
   * @param indexName the name of the index to be dropped.
   * @param tableIdent the table on which index to be dropped.
   * @return the SQL statement to use for dropping the index.
   */
  def dropIndex(indexName: String, tableIdent: Identifier): String = {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3181")
  }

  /**
   * Lists all the indexes in this table.
   */
  def listIndexes(
      conn: Connection,
      tableIdent: Identifier,
      options: JDBCOptions): Array[TableIndex] = {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3182")
  }

  /**
   * Gets a dialect exception, classifies it and wraps it by `AnalysisException`.
   * @param e The dialect specific exception.
   * @param errorClass The error class assigned in the case of an unclassified `e`
   * @param messageParameters The message parameters of `errorClass`
   * @param description The error description
   * @return `AnalysisException` or its sub-class.
   */
  def classifyException(
      e: Throwable,
      errorClass: String,
      messageParameters: Map[String, String],
      description: String): AnalysisException = {
    classifyException(description, e)
  }

  /**
   * Gets a dialect exception, classifies it and wraps it by `AnalysisException`.
   * @param message The error message to be placed to the returned exception.
   * @param e The dialect specific exception.
   * @return `AnalysisException` or its sub-class.
   */
  @deprecated("Please override the classifyException method with an error class", "4.0.0")
  def classifyException(message: String, e: Throwable): AnalysisException = {
    new AnalysisException(
      errorClass = "FAILED_JDBC.UNCLASSIFIED",
      messageParameters = Map(
        "url" -> "jdbc:",
        "message" -> message),
      cause = Some(e))
  }

  /**
   * Returns the LIMIT clause for the SELECT statement
   */
  def getLimitClause(limit: Integer): String = {
    if (limit > 0) s"LIMIT $limit" else ""
  }

  /**
   * Returns the OFFSET clause for the SELECT statement
   */
  def getOffsetClause(offset: Integer): String = {
    if (offset > 0) s"OFFSET $offset" else ""
  }

  /**
   * Returns the SQL builder for the SELECT statement.
   */
  def getJdbcSQLQueryBuilder(options: JDBCOptions): JdbcSQLQueryBuilder =
    new JdbcSQLQueryBuilder(this, options)

  /**
   * Returns ture if dialect supports LIMIT clause.
   *
   * Note: Some build-in dialect supports LIMIT clause with some trick, please see:
   * {@link OracleDialect.OracleSQLQueryBuilder} and
   * {@link MsSqlServerDialect.MsSqlServerSQLQueryBuilder}.
   */
  def supportsLimit: Boolean = false

  /**
   * Returns ture if dialect supports OFFSET clause.
   *
   * Note: Some build-in dialect supports OFFSET clause with some trick, please see:
   * {@link OracleDialect.OracleSQLQueryBuilder} and
   * {@link MySQLDialect.MySQLSQLQueryBuilder}.
   */
  def supportsOffset: Boolean = false

  def supportsTableSample: Boolean = false

  def getTableSample(sample: TableSampleInfo): String =
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3183")

  /**
   * Return the DB-specific quoted and fully qualified table name
   */
  @Since("3.5.0")
  def getFullyQualifiedQuotedTableName(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(quoteIdentifier).mkString(".")
  }

  /**
   * Return TimestampType/TimestampNTZType based on the metadata.
   */
  protected final def getTimestampType(md: Metadata): DataType = {
    JdbcUtils.getTimestampType(md.getBoolean("isTimestampNTZ"))
  }
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

  private def registerDialects(): Unit = {
    val loader = ServiceLoader.load(classOf[JdbcDialect], Utils.getContextOrSparkClassLoader)
    val iter = loader.iterator()
    while (iter.hasNext) {
      registerDialect(iter.next())
    }
  }
  registerDialects()

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
private[spark] object NoopDialect extends JdbcDialect {
  override def canHandle(url : String): Boolean = true
}
