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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, SQLException}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

/**
 * Util functions for JDBC tables.
 */
object JdbcUtils extends Logging {

  // the property names are case sensitive
  val JDBC_BATCH_FETCH_SIZE = "fetchsize"
  val JDBC_BATCH_INSERT_SIZE = "batchsize"
  val JDBC_TXN_ISOLATION_LEVEL = "isolationLevel"

  /**
   * Returns a factory for creating connections to the given JDBC URL.
   *
   * @param url the JDBC url to connect to.
   * @param properties JDBC connection properties.
   */
  def createConnectionFactory(url: String, properties: Properties): () => Connection = {
    val userSpecifiedDriverClass = Option(properties.getProperty("driver"))
    userSpecifiedDriverClass.foreach(DriverRegistry.register)
    // Performing this part of the logic on the driver guards against the corner-case where the
    // driver returned for a URL is different on the driver and executors due to classpath
    // differences.
    val driverClass: String = userSpecifiedDriverClass.getOrElse {
      DriverManager.getDriver(url).getClass.getCanonicalName
    }
    () => {
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
        case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
        case d if d.getClass.getCanonicalName == driverClass => d
      }.getOrElse {
        throw new IllegalStateException(
          s"Did not find registered driver with class $driverClass")
      }
      driver.connect(url, properties)
    }
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(conn: Connection, url: String, table: String): Boolean = {
    val dialect = JdbcDialects.get(url)

    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems using JDBC meta data calls, considering "table" could also include
    // the database name. Query used to find table exists can be overridden by the dialects.
    Try {
      val statement = conn.prepareStatement(dialect.getTableExistsQuery(table))
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /**
   * Drops a table from the JDBC database.
   */
  def dropTable(conn: Connection, table: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(s"DROP TABLE $table")
    } finally {
      statement.close()
    }
  }

  /**
   * Truncates a table from the JDBC database.
   */
  def truncateTable(conn: Connection, table: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(s"TRUNCATE TABLE $table")
    } finally {
      statement.close()
    }
  }

  def isCascadingTruncateTable(url: String): Option[Boolean] = {
    JdbcDialects.get(url).isCascadingTruncateTable()
  }

  /**
   * Returns a PreparedStatement that inserts a row into table via conn.
   */
  def insertStatement(conn: Connection, table: String, rddSchema: StructType, dialect: JdbcDialect)
      : PreparedStatement = {
    val columns = rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders)"
    conn.prepareStatement(sql)
  }

  /**
   * Retrieve standard jdbc types.
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The default JdbcType for this DataType
   */
  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(
        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  // A `JDBCValueSetter` is responsible for setting a value from `Row` into a field for
  // `PreparedStatement`. The last argument `Int` means the index for the value to be set
  // in the SQL statement and also used for the value in `Row`.
  private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

  private def makeSetter(
      conn: Connection,
      dialect: JdbcDialect,
      dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setString(pos + 1, row.getString(pos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase.split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](pos).toArray)
        stmt.setArray(pos + 1, array)

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  /**
   * Saves a partition of a DataFrame to the JDBC database.  This is done in
   * a single database transaction (unless isolation level is "NONE")
   * in order to avoid repeatedly inserting data as much as possible.
   *
   * It is still theoretically possible for rows in a DataFrame to be
   * inserted into the database more than once if a stage somehow fails after
   * the commit occurs but before the stage can return successfully.
   *
   * This is not a closure inside saveTable() because apparently cosmetic
   * implementation changes elsewhere might easily render such a closure
   * non-Serializable.  Instead, we explicitly close over all variables that
   * are used.
   */
  def savePartition(
      getConnection: () => Connection,
      table: String,
      iterator: Iterator[Row],
      rddSchema: StructType,
      nullTypes: Array[Int],
      batchSize: Int,
      dialect: JdbcDialect,
      isolationLevel: Int): Iterator[Byte] = {
    require(batchSize >= 1,
      s"Invalid value `${batchSize.toString}` for parameter " +
      s"`${JdbcUtils.JDBC_BATCH_INSERT_SIZE}`. The minimum value is 1.")

    val conn = getConnection()
    var committed = false

    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel))  {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(s"Requested isolation level $isolationLevel is not supported; " +
                s"falling back to default isolation level $defaultIsolation")
          }
        } else {
          logWarning(s"Requested isolation level $isolationLevel, but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt = insertStatement(conn, table, rddSchema, dialect)
      val setters: Array[JDBCValueSetter] = rddSchema.fields.map(_.dataType)
        .map(makeSetter(conn, dialect, _)).toArray

      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              setters(i).apply(stmt, row, i)
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (e.getCause != cause) {
          if (e.getCause == null) {
            e.initCause(cause)
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }

  /**
   * Compute the schema string for this RDD.
   */
  def schemaString(df: DataFrame, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    df.schema.fields foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ: String = getJdbcType(field.dataType, dialect).databaseTypeDefinition
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   * Saves the content of the [[DataFrame]] to an external database table in a single transaction.
   *
   * @param mode Specifies the behavior when data or table already exists.
   * @param parameters Specifies the JDBC database connection arguments
   * @param df Specifies the dataframe
   */
  def saveTable(mode: SaveMode, parameters: Map[String, String], df: DataFrame): Unit = {
    val jdbcOptions = new JDBCOptions(parameters)
    val url = parameters("url")
    val table = parameters("dbtable")

    val properties = new Properties()
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    val conn = JdbcUtils.createConnectionFactory(url, properties)()
    try {
      var tableExists = JdbcUtils.tableExists(conn, url, table)

      if (tableExists) {
        mode match {
          case SaveMode.Ignore => return
          case SaveMode.ErrorIfExists => sys.error(s"Table $table already exists.")
          case SaveMode.Overwrite =>
            if (jdbcOptions.isTruncate &&
              JdbcUtils.isCascadingTruncateTable(url) == Option(false)) {
              JdbcUtils.truncateTable(conn, table)
            } else {
              JdbcUtils.dropTable(conn, table)
              tableExists = false
            }
          case SaveMode.Append =>
        }
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val schema = JdbcUtils.schemaString(df, url)
        // To allow certain options to append when create a new table, which can be
        // table_options or partition_options.
        // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
        val createtblOptions = jdbcOptions.createTableOptions
        val sql = s"CREATE TABLE $table ($schema) $createtblOptions"
        val statement = conn.createStatement
        try {
          statement.executeUpdate(sql)
        } finally {
          statement.close()
        }
      }
    } finally {
      conn.close()
    }

    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      getJdbcType(field.dataType, dialect).jdbcNullType
    }

    val rddSchema = df.schema
    val getConnection: () => Connection = createConnectionFactory(url, properties)
    val batchSize = properties.getProperty(JDBC_BATCH_INSERT_SIZE, "1000").toInt
    val isolationLevel =
      properties.getProperty(JDBC_TXN_ISOLATION_LEVEL, "READ_UNCOMMITTED") match {
        case "NONE" => Connection.TRANSACTION_NONE
        case "READ_UNCOMMITTED" => Connection.TRANSACTION_READ_UNCOMMITTED
        case "READ_COMMITTED" => Connection.TRANSACTION_READ_COMMITTED
        case "REPEATABLE_READ" => Connection.TRANSACTION_REPEATABLE_READ
        case "SERIALIZABLE" => Connection.TRANSACTION_SERIALIZABLE
      }
    df.foreachPartition(iterator => savePartition(
      getConnection, table, iterator, rddSchema, nullTypes, batchSize, dialect, isolationLevel)
    )
  }
}
