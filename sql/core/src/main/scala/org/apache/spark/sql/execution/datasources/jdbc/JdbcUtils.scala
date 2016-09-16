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

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.TaskContext
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{MutableRow, SpecificMutableRow}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator

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
   *
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

  /**
   * Maps a JDBC type to a Catalyst type.  This function is called only when
   * the JdbcDialect class corresponding to your database driver returns null.
   *
   * @param sqlType - A field of java.sql.Types
   * @return The Catalyst type corresponding to sqlType.
   */
  private def getCatalystType(
      sqlType: Int,
      precision: Int,
      scale: Int,
      signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.CLOB          => StringType
      case java.sql.Types.DATALINK      => null
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)
      case java.sql.Types.DECIMAL       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT      => null
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT   => null
      case java.sql.Types.LONGNVARCHAR  => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR   => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NCLOB         => StringType
      case java.sql.Types.NULL          => null
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.OTHER         => null
      case java.sql.Types.REAL          => DoubleType
      case java.sql.Types.REF           => StringType
      case java.sql.Types.ROWID         => LongType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.SQLXML        => StringType
      case java.sql.Types.STRUCT        => StringType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TINYINT       => IntegerType
      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR       => StringType
      case _                            => null
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }

  /**
   * Takes a [[ResultSet]] and returns its Catalyst schema.
   *
   * @return A [[StructType]] giving the Catalyst schema.
   * @throws SQLException if the schema contains an unsupported type.
   */
  def getSchema(resultSet: ResultSet, dialect: JdbcDialect): StructType = {
    val rsmd = resultSet.getMetaData
    val ncols = rsmd.getColumnCount
    val fields = new Array[StructField](ncols)
    var i = 0
    while (i < ncols) {
      val columnName = rsmd.getColumnLabel(i + 1)
      val dataType = rsmd.getColumnType(i + 1)
      val typeName = rsmd.getColumnTypeName(i + 1)
      val fieldSize = rsmd.getPrecision(i + 1)
      val fieldScale = rsmd.getScale(i + 1)
      val isSigned = {
        try {
          rsmd.isSigned(i + 1)
        } catch {
          // Workaround for HIVE-14684:
          case e: SQLException if
          e.getMessage == "Method not supported" &&
            rsmd.getClass.getName == "org.apache.hive.jdbc.HiveResultSetMetaData" => true
        }
      }
      val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      val metadata = new MetadataBuilder()
        .putString("name", columnName)
        .putLong("scale", fieldScale)
      val columnType =
        dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
          getCatalystType(dataType, fieldSize, fieldScale, isSigned))
      fields(i) = StructField(columnName, columnType, nullable, metadata.build())
      i = i + 1
    }
    new StructType(fields)
  }

  /**
   * Convert a [[ResultSet]] into an iterator of Catalyst Rows.
   */
  def resultSetToRows(resultSet: ResultSet, schema: StructType): Iterator[Row] = {
    val inputMetrics =
      Option(TaskContext.get()).map(_.taskMetrics().inputMetrics).getOrElse(new InputMetrics)
    val encoder = RowEncoder(schema).resolveAndBind()
    val internalRows = resultSetToSparkInternalRows(resultSet, schema, inputMetrics)
    internalRows.map(encoder.fromRow)
  }

  private[spark] def resultSetToSparkInternalRows(
      resultSet: ResultSet,
      schema: StructType,
      inputMetrics: InputMetrics): Iterator[InternalRow] = {
    new NextIterator[InternalRow] {
      private[this] val rs = resultSet
      private[this] val getters: Array[JDBCValueGetter] = makeGetters(schema)
      private[this] val mutableRow = new SpecificMutableRow(schema.fields.map(x => x.dataType))

      override protected def close(): Unit = {
        try {
          rs.close()
        } catch {
          case e: Exception => logWarning("Exception closing resultset", e)
        }
      }

      override protected def getNext(): InternalRow = {
        if (rs.next()) {
          inputMetrics.incRecordsRead(1)
          var i = 0
          while (i < getters.length) {
            getters(i).apply(rs, mutableRow, i)
            if (rs.wasNull) mutableRow.setNullAt(i)
            i = i + 1
          }
          mutableRow
        } else {
          finished = true
          null.asInstanceOf[InternalRow]
        }
      }
    }
  }

  // A `JDBCValueGetter` is responsible for getting a value from `ResultSet` into a field
  // for `MutableRow`. The last argument `Int` means the index for the value to be set in
  // the row and also used for the value in `ResultSet`.
  private type JDBCValueGetter = (ResultSet, MutableRow, Int) => Unit

  /**
   * Creates `JDBCValueGetter`s according to [[StructType]], which can set
   * each value from `ResultSet` to each field of [[MutableRow]] correctly.
   */
  private def makeGetters(schema: StructType): Array[JDBCValueGetter] =
    schema.fields.map(sf => makeGetter(sf.dataType, sf.metadata))

  private def makeGetter(dt: DataType, metadata: Metadata): JDBCValueGetter = dt match {
    case BooleanType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        row.setBoolean(pos, rs.getBoolean(pos + 1))

    case DateType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
        val dateVal = rs.getDate(pos + 1)
        if (dateVal != null) {
          row.setInt(pos, DateTimeUtils.fromJavaDate(dateVal))
        } else {
          row.update(pos, null)
        }

    // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
    // object returned by ResultSet.getBigDecimal is not correctly matched to the table
    // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
    // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
    // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
    // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
    // retrieve it, you will get wrong result 199.99.
    // So it is needed to set precision and scale for Decimal based on JDBC metadata.
    case DecimalType.Fixed(p, s) =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        val decimal =
          nullSafeConvert[java.math.BigDecimal](rs.getBigDecimal(pos + 1), d => Decimal(d, p, s))
        row.update(pos, decimal)

    case DoubleType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        row.setDouble(pos, rs.getDouble(pos + 1))

    case FloatType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        row.setFloat(pos, rs.getFloat(pos + 1))

    case IntegerType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        row.setInt(pos, rs.getInt(pos + 1))

    case LongType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        val bytes = rs.getBytes(pos + 1)
        var ans = 0L
        var j = 0
        while (j < bytes.size) {
          ans = 256 * ans + (255 & bytes(j))
          j = j + 1
        }
        row.setLong(pos, ans)

    case LongType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        row.setLong(pos, rs.getLong(pos + 1))

    case ShortType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        row.setShort(pos, rs.getShort(pos + 1))

    case StringType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

    case TimestampType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, DateTimeUtils.fromJavaTimestamp(t))
        } else {
          row.update(pos, null)
        }

    case BinaryType =>
      (rs: ResultSet, row: MutableRow, pos: Int) =>
        row.update(pos, rs.getBytes(pos + 1))

    case ArrayType(et, _) =>
      val elementConversion = et match {
        case TimestampType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
              nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
            }

        case StringType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.lang.String]]
              .map(UTF8String.fromString)

        case DateType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Date]].map { date =>
              nullSafeConvert(date, DateTimeUtils.fromJavaDate)
            }

        case dt: DecimalType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
              nullSafeConvert[java.math.BigDecimal](
                decimal, d => Decimal(d, dt.precision, dt.scale))
            }

        case LongType if metadata.contains("binarylong") =>
          throw new IllegalArgumentException(s"Unsupported array element " +
            s"type ${dt.simpleString} based on binary")

        case ArrayType(_, _) =>
          throw new IllegalArgumentException("Nested arrays unsupported")

        case _ => (array: Object) => array.asInstanceOf[Array[Any]]
      }

      (rs: ResultSet, row: MutableRow, pos: Int) =>
        val array = nullSafeConvert[Object](
          rs.getArray(pos + 1).getArray,
          array => new GenericArrayData(elementConversion.apply(array)))
        row.update(pos, array)

    case _ => throw new IllegalArgumentException(s"Unsupported type ${dt.simpleString}")
  }

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
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
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
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
      Iterator.empty
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
   * Saves the RDD to the database in a single transaction.
   */
  def saveTable(
      df: DataFrame,
      url: String,
      table: String,
      properties: Properties) {
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
