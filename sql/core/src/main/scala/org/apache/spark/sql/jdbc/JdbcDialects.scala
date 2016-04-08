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

import org.apache.spark.annotation.DeveloperApi
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
 * information---for instance, BIT(n>1) being reported as a BIT type is quite
 * common, even though BIT in JDBC is meant for single-bit values.  Also, there
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
abstract class JdbcDialect extends Serializable {
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
   * Override connection specific properties to run before a select is made.  This is in place to
   * allow dialects that need special treatment to optimize behavior.
   * @param connection The connection object
   * @param properties The connection properties.  This is passed through from the relation.
   */
  def beforeFetch(connection: Connection, properties: Map[String, String]): Unit = {
  }

}

/**
 * :: DeveloperApi ::
 * Registry of dialects that apply to every new jdbc [[org.apache.spark.sql.DataFrame]].
 *
 * If multiple matching dialects are registered then all matching ones will be
 * tried in reverse order. A user-added dialect will thus be applied first,
 * overwriting the defaults.
 *
 * Note that all new dialects are applied to new jdbc DataFrames only. Make
 * sure to register your dialects first.
 */
@DeveloperApi
object JdbcDialects {

  /**
   * Register a dialect for use on all new matching jdbc [[org.apache.spark.sql.DataFrame]].
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

  /**
   * Fetch the JdbcDialect class corresponding to a given database url.
   */
  private[sql] def get(url: String): JdbcDialect = {
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
