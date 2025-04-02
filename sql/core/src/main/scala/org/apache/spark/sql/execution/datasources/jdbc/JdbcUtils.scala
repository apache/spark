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

import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, Date, JDBCType, PreparedStatement, ResultSet, ResultSetMetaData, SQLException, Time, Timestamp}
import java.time.{Instant, LocalDate}
import java.util

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.{SparkThrowable, SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{DEFAULT_ISOLATION_LEVEL, ISOLATION_LEVEL}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{DecimalPrecisionTypeCoercion, Resolver}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils, GenericArrayData}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_MILLIS
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.connector.catalog.{Identifier, TableChange}
import org.apache.spark.sql.connector.catalog.index.{SupportsIndex, TableIndex}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType, NoopDialect}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.NextIterator

/**
 * Util functions for JDBC tables.
 */
object JdbcUtils extends Logging with SQLConfHelper {

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(conn: Connection, options: JdbcOptionsInWrite): Boolean = {
    val dialect = JdbcDialects.get(options.url)

    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems using JDBC meta data calls, considering "table" could also include
    // the database name. Query used to find table exists can be overridden by the dialects.
    Try {
      val statement = conn.prepareStatement(dialect.getTableExistsQuery(options.table))
      try {
        statement.setQueryTimeout(options.queryTimeout)
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /**
   * Drops a table from the JDBC database.
   */
  def dropTable(conn: Connection, table: String, options: JDBCOptions): Unit = {
    val dialect = JdbcDialects.get(options.url)
    executeStatement(conn, options, dialect.dropTable(table))
  }

  /**
   * Truncates a table from the JDBC database without side effects.
   */
  def truncateTable(conn: Connection, options: JdbcOptionsInWrite): Unit = {
    val dialect = JdbcDialects.get(options.url)
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      val truncateQuery = if (options.isCascadeTruncate.isDefined) {
        dialect.getTruncateQuery(options.table, options.isCascadeTruncate)
      } else {
        dialect.getTruncateQuery(options.table)
      }
      statement.executeUpdate(truncateQuery)
    } finally {
      statement.close()
    }
  }

  def isCascadingTruncateTable(url: String): Option[Boolean] = {
    JdbcDialects.get(url).isCascadingTruncateTable()
  }

  /**
   * Returns an Insert SQL statement for inserting a row into the target table via JDBC conn.
   */
  def getInsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      isCaseSensitive: Boolean,
      dialect: JdbcDialect): String = {
    val columns = if (tableSchema.isEmpty) {
      rddSchema.fields
    } else {
      // The generated insert statement needs to follow rddSchema's column sequence and
      // tableSchema's column names. When appending data into some case-sensitive DBMSs like
      // PostgreSQL/Oracle, we need to respect the existing case-sensitive column names instead of
      // RDD column names for user convenience.
      rddSchema.fields.map { col =>
        tableSchema.get.find(f => conf.resolver(f.name, col.name)).getOrElse {
          throw QueryCompilationErrors.columnNotFoundInSchemaError(
            col.dataType, col.name, table, rddSchema.fieldNames)
        }
      }
    }
    dialect.insertIntoTable(table, columns)
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
      case CharType(n) => Option(JdbcType(s"CHAR($n)", java.sql.Types.CHAR))
      case VarcharType(n) => Option(JdbcType(s"VARCHAR($n)", java.sql.Types.VARCHAR))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      // This is a common case of timestamp without time zone. Most of the databases either only
      // support TIMESTAMP type or use TIMESTAMP as an alias for TIMESTAMP WITHOUT TIME ZONE.
      // Note that some dialects override this setting, e.g. as SQL Server.
      case TimestampNTZType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(
        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw QueryExecutionErrors.cannotGetJdbcTypeError(dt))
  }

  def getTimestampType(isTimestampNTZ: Boolean): DataType = {
    if (isTimestampNTZ) TimestampNTZType else TimestampType
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
      typeName: String,
      precision: Int,
      scale: Int,
      signed: Boolean,
      isTimestampNTZ: Boolean): DataType = sqlType match {
    case java.sql.Types.BIGINT => if (signed) LongType else DecimalType(20, 0)
    case java.sql.Types.BINARY => BinaryType
    case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
    case java.sql.Types.BLOB => BinaryType
    case java.sql.Types.BOOLEAN => BooleanType
    case java.sql.Types.CHAR if conf.charVarcharAsString => StringType
    case java.sql.Types.CHAR => CharType(precision)
    case java.sql.Types.CLOB => StringType
    case java.sql.Types.DATE => DateType
    case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC if precision == 0 && scale == 0 =>
      DecimalType.SYSTEM_DEFAULT
    case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC if scale < 0 =>
      DecimalType.bounded(precision - scale, 0)
    case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC =>
      DecimalPrecisionTypeCoercion.bounded(precision, scale)
    case java.sql.Types.DOUBLE => DoubleType
    case java.sql.Types.FLOAT => FloatType
    case java.sql.Types.INTEGER => if (signed) IntegerType else LongType
    case java.sql.Types.LONGNVARCHAR => StringType
    case java.sql.Types.LONGVARBINARY => BinaryType
    case java.sql.Types.LONGVARCHAR => StringType
    case java.sql.Types.NCHAR => StringType
    case java.sql.Types.NCLOB => StringType
    case java.sql.Types.NVARCHAR => StringType
    case java.sql.Types.REAL => DoubleType
    case java.sql.Types.REF => StringType
    case java.sql.Types.ROWID => StringType
    case java.sql.Types.SMALLINT => IntegerType
    case java.sql.Types.SQLXML => StringType
    case java.sql.Types.STRUCT => StringType
    case java.sql.Types.TIME => getTimestampType(isTimestampNTZ)
    case java.sql.Types.TIMESTAMP => getTimestampType(isTimestampNTZ)
    case java.sql.Types.TINYINT => IntegerType
    case java.sql.Types.VARBINARY => BinaryType
    case java.sql.Types.VARCHAR if conf.charVarcharAsString => StringType
    case java.sql.Types.VARCHAR => VarcharType(precision)
    case java.sql.Types.NULL => NullType
    case _ =>
      // For unmatched types:
      // including java.sql.Types.ARRAY,DATALINK,DISTINCT,JAVA_OBJECT,OTHER,REF_CURSOR,
      // TIME_WITH_TIMEZONE,TIMESTAMP_WITH_TIMEZONE, and among others.
      val jdbcType = classOf[JDBCType].getEnumConstants()
        .find(_.getVendorTypeNumber == sqlType)
        .map(_.getName)
        .getOrElse(sqlType.toString)
      throw QueryExecutionErrors.unrecognizedSqlTypeError(jdbcType, typeName)
  }

  /**
   * Returns the schema if the table already exists in the JDBC database.
   */
  def getSchemaOption(conn: Connection, options: JDBCOptions): Option[StructType] = {
    val dialect = JdbcDialects.get(options.url)

    try {
      val statement =
        conn.prepareStatement(options.prepareQuery + dialect.getSchemaQuery(options.tableOrQuery))
      try {
        statement.setQueryTimeout(options.queryTimeout)
        Some(getSchema(conn, statement.executeQuery(), dialect,
          isTimestampNTZ = options.preferTimestampNTZ))
      } catch {
        case _: SQLException => None
      } finally {
        statement.close()
      }
    } catch {
      case _: SQLException => None
    }
  }

  /**
   * Takes a [[ResultSet]] and returns its Catalyst schema.
   *
   * @param alwaysNullable If true, all the columns are nullable.
   * @param isTimestampNTZ If true, all timestamp columns are interpreted as TIMESTAMP_NTZ.
   * @return A [[StructType]] giving the Catalyst schema.
   * @throws SQLException if the schema contains an unsupported type.
   */
  def getSchema(
      conn: Connection,
      resultSet: ResultSet,
      dialect: JdbcDialect,
      alwaysNullable: Boolean = false,
      isTimestampNTZ: Boolean = false): StructType = {
    val rsmd = resultSet.getMetaData
    val ncols = rsmd.getColumnCount
    val fields = new Array[StructField](ncols)
    var i = 0
    while (i < ncols) {
      val metadata = new MetadataBuilder()
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
      val nullable = if (alwaysNullable) {
        true
      } else {
        rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      }

      dataType match {
        case java.sql.Types.TIME =>
          // SPARK-33888
          // - include TIME type metadata
          // - always build the metadata
          metadata.putBoolean("logical_time_type", true)
        case java.sql.Types.ROWID =>
          metadata.putBoolean("rowid", true)
        case _ =>
      }
      metadata.putBoolean("isSigned", isSigned)
      metadata.putBoolean("isTimestampNTZ", isTimestampNTZ)
      metadata.putLong("scale", fieldScale)
      dialect.updateExtraColumnMeta(conn, rsmd, i + 1, metadata)

      val columnType =
        dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
          getCatalystType(dataType, typeName, fieldSize, fieldScale, isSigned, isTimestampNTZ))
      fields(i) = StructField(columnName, columnType, nullable, metadata.build())
      i = i + 1
    }
    CharVarcharUtils.replaceCharVarcharWithStringInSchema(new StructType(fields))
  }

  /**
   * Convert a [[ResultSet]] into an iterator of Catalyst Rows.
   */
  def resultSetToRows(
      resultSet: ResultSet,
      schema: StructType): Iterator[Row] = {
    resultSetToRows(resultSet, schema, NoopDialect)
  }

  def resultSetToRows(
      resultSet: ResultSet,
      schema: StructType,
      dialect: JdbcDialect): Iterator[Row] = {
    val inputMetrics =
      Option(TaskContext.get()).map(_.taskMetrics().inputMetrics).getOrElse(new InputMetrics)
    val fromRow = ExpressionEncoder(schema).resolveAndBind().createDeserializer()
    val internalRows = resultSetToSparkInternalRows(resultSet, dialect, schema, inputMetrics)
    internalRows.map(fromRow)
  }

  private[spark] def resultSetToSparkInternalRows(
      resultSet: ResultSet,
      dialect: JdbcDialect,
      schema: StructType,
      inputMetrics: InputMetrics): Iterator[InternalRow] = {
    new NextIterator[InternalRow] {
      private[this] val rs = resultSet
      private[this] val getters: Array[JDBCValueGetter] = makeGetters(dialect, schema)
      private[this] val mutableRow =
        new SpecificInternalRow(schema.fields.map(x => x.dataType).toImmutableArraySeq)

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
  private type JDBCValueGetter = (ResultSet, InternalRow, Int) => Unit

  /**
   * Creates `JDBCValueGetter`s according to [[StructType]], which can set
   * each value from `ResultSet` to each field of [[InternalRow]] correctly.
   */
  private def makeGetters(
      dialect: JdbcDialect,
      schema: StructType): Array[JDBCValueGetter] = {
    val replaced = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
    replaced.fields.map(sf => makeGetter(sf.dataType, dialect, sf.metadata))
  }

  private def makeGetter(
      dt: DataType,
      dialect: JdbcDialect,
      metadata: Metadata): JDBCValueGetter = dt match {
    case BooleanType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setBoolean(pos, rs.getBoolean(pos + 1))

    case DateType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
        val dateVal = rs.getDate(pos + 1)
        if (dateVal != null) {
          row.setInt(pos, fromJavaDate(dialect.convertJavaDateToDate(dateVal)))
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
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val decimal =
          nullSafeConvert[JBigDecimal](rs.getBigDecimal(pos + 1), d => Decimal(d, p, s))
        row.update(pos, decimal)

    case DoubleType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setDouble(pos, rs.getDouble(pos + 1))

    case FloatType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setFloat(pos, rs.getFloat(pos + 1))

    case IntegerType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setInt(pos, rs.getInt(pos + 1))

    case LongType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val l = nullSafeConvert[Array[Byte]](rs.getBytes(pos + 1), bytes => {
          var ans = 0L
          var j = 0
          while (j < bytes.length) {
            ans = 256 * ans + (255 & bytes(j))
            j = j + 1
          }
          ans
        })
        row.update(pos, l)

    case LongType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setLong(pos, rs.getLong(pos + 1))

    case ShortType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setShort(pos, rs.getShort(pos + 1))

    case ByteType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setByte(pos, rs.getByte(pos + 1))

    case StringType if metadata.contains("rowid") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val rawRowId = rs.getRowId(pos + 1)
        if (rawRowId == null) {
          row.update(pos, null)
        } else {
          row.update(pos, UTF8String.fromString(rawRowId.toString))
        }

    case StringType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

    // SPARK-34357 - sql TIME type represents as zero epoch timestamp.
    // It is mapped as Spark TimestampType but fixed at 1970-01-01 for day,
    // time portion is time of day, with no reference to a particular calendar,
    // time zone or date, with a precision till microseconds.
    // It stores the number of milliseconds after midnight, 00:00:00.000000
    case TimestampType if metadata.contains("logical_time_type") =>
      (rs: ResultSet, row: InternalRow, pos: Int) => {
        row.update(pos, nullSafeConvert[Time](
          rs.getTime(pos + 1), t => Math.multiplyExact(t.getTime, MICROS_PER_MILLIS)))
      }

    case TimestampType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, fromJavaTimestamp(dialect.convertJavaTimestampToTimestamp(t)))
        } else {
          row.update(pos, null)
        }

    case TimestampNTZType if metadata.contains("logical_time_type") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val micros = nullSafeConvert[Time](rs.getTime(pos + 1), t => {
          val time = dialect.convertJavaTimestampToTimestampNTZ(new Timestamp(t.getTime))
          localDateTimeToMicros(time)
        })
        row.update(pos, micros)

    case TimestampNTZType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, localDateTimeToMicros(dialect.convertJavaTimestampToTimestampNTZ(t)))
        } else {
          row.update(pos, null)
        }

    case BinaryType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val bytes = rs.getBytes(pos + 1)
        if (bytes != null) {
          val binary = bytes.flatMap(Integer.toBinaryString(_).getBytes(StandardCharsets.US_ASCII))
          row.update(pos, binary)
        } else {
          row.update(pos, null)
        }

    case BinaryType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos, rs.getBytes(pos + 1))

    case _: YearMonthIntervalType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos,
          nullSafeConvert(rs.getString(pos + 1), dialect.getYearMonthIntervalAsMonths))

    case _: DayTimeIntervalType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos,
          nullSafeConvert(rs.getString(pos + 1), dialect.getDayTimeIntervalAsMicros))

    case _: ArrayType if metadata.contains("pg_bit_array_type") =>
      // SPARK-47628: Handle PostgreSQL bit(n>1) array type ahead. As in the pgjdbc driver,
      // bit(n>1)[] is not distinguishable from bit(1)[], and they are all recognized as boolen[].
      // This is wrong for bit(n>1)[], so we need to handle it first as byte array.
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val fieldString = rs.getString(pos + 1)
        if (fieldString != null) {
          val strArray = fieldString.substring(1, fieldString.length - 1).split(",")
          // Charset is picked from the pgjdbc driver for consistency.
          val bytesArray = strArray.map(_.getBytes(StandardCharsets.US_ASCII))
          row.update(pos, new GenericArrayData(bytesArray))
        } else {
          row.update(pos, null)
        }

    case ArrayType(et, _) =>
      def elementConversion(et: DataType): AnyRef => Any = et match {
        case TimestampType => arrayConverter[Timestamp] {
          (t: Timestamp) => fromJavaTimestamp(dialect.convertJavaTimestampToTimestamp(t))
        }

        case TimestampNTZType =>
          arrayConverter[Timestamp] {
            (t: Timestamp) => localDateTimeToMicros(dialect.convertJavaTimestampToTimestampNTZ(t))
          }

        case StringType =>
          arrayConverter[Object]((obj: Object) => UTF8String.fromString(obj.toString))

        case DateType => arrayConverter[Date] {
          (d: Date) => fromJavaDate(dialect.convertJavaDateToDate(d))
        }

        case dt: DecimalType =>
            arrayConverter[java.math.BigDecimal](d => Decimal(d, dt.precision, dt.scale))

        case LongType if metadata.contains("binarylong") =>
          throw QueryExecutionErrors.unsupportedArrayElementTypeBasedOnBinaryError(dt)

        case ArrayType(et0, _) =>
          arrayConverter[Array[Any]] {
            arr => new GenericArrayData(elementConversion(et0)(arr))
          }

        case IntegerType => arrayConverter[Int]((i: Int) => i)
        case FloatType => arrayConverter[Float]((f: Float) => f)
        case DoubleType => arrayConverter[Double]((d: Double) => d)
        case ShortType => arrayConverter[Short]((s: Short) => s)
        case BooleanType => arrayConverter[Boolean]((b: Boolean) => b)
        case LongType => arrayConverter[Long]((l: Long) => l)

        case _ => (array: Object) => array.asInstanceOf[Array[Any]]
      }

      (rs: ResultSet, row: InternalRow, pos: Int) =>
        try {
          val array = nullSafeConvert[java.sql.Array](
            input = rs.getArray(pos + 1),
            array => new GenericArrayData(elementConversion(et)(array.getArray())))
          row.update(pos, array)
        } catch {
          case e: java.lang.ClassCastException =>
            throw QueryExecutionErrors.wrongDatatypeInSomeRows(pos, dt)
        }

    case NullType =>
      (_: ResultSet, row: InternalRow, pos: Int) => row.update(pos, null)

    case _ => throw QueryExecutionErrors.unsupportedJdbcTypeError(dt.catalogString)
  }

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }

  private def arrayConverter[T](elementConvert: T => Any): Any => Any = (array: Any) => {
    array.asInstanceOf[Array[T]].map(e => nullSafeConvert(e, elementConvert))
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
      if (conf.datetimeJava8ApiEnabled) {
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setTimestamp(pos + 1, toJavaTimestamp(instantToMicros(row.getAs[Instant](pos))))
      } else {
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))
      }

    case TimestampNTZType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1,
          dialect.convertTimestampNTZToJavaTimestamp(row.getAs[java.time.LocalDateTime](pos)))

    case DateType =>
      if (conf.datetimeJava8ApiEnabled) {
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setDate(pos + 1, toJavaDate(localDateToDays(row.getAs[LocalDate](pos))))
      } else {
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))
      }

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        et match {
          case TimestampNTZType =>
            val array = row.getSeq[java.time.LocalDateTime](pos)
            val arrayType = conn.createArrayOf(
              getJdbcType(et, dialect).databaseTypeDefinition.split("\\(")(0),
              array.map(dialect.convertTimestampNTZToJavaTimestamp).toArray)
            stmt.setArray(pos + 1, arrayType)
          case _ =>
            @tailrec
            def getElementTypeName(dt: DataType): String = dt match {
              case ArrayType(et0, _) => getElementTypeName(et0)
              case a: AtomicType => getJdbcType(a, dialect).databaseTypeDefinition.split("\\(")(0)
              case _ => throw QueryExecutionErrors.nestedArraysUnsupportedError()
            }

            def toArray(seq: scala.collection.Seq[Any], dt: DataType): Array[Any] = dt match {
              case ArrayType(et0, _) =>
                seq.map(i => toArray(i.asInstanceOf[scala.collection.Seq[Any]], et0)).toArray
              case _ => seq.toArray
            }

            val seq = row.getSeq[AnyRef](pos)
            val arrayType = conn.createArrayOf(
              getElementTypeName(et),
              toArray(seq, et).asInstanceOf[Array[AnyRef]])
            stmt.setArray(pos + 1, arrayType)
        }

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw QueryExecutionErrors.cannotTranslateNonNullValueForFieldError(pos)
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
   *
   * Note that this method records task output metrics. It assumes the method is
   * running in a task. For now, we only records the number of rows being written
   * because there's no good way to measure the total bytes being written. Only
   * effective outputs are taken into account: for example, metric will not be updated
   * if it supports transaction and transaction is rolled back, but metric will be
   * updated even with error if it doesn't support transaction, as there're dirty outputs.
   */
  def savePartition(
      table: String,
      iterator: Iterator[Row],
      rddSchema: StructType,
      insertStmt: String,
      batchSize: Int,
      dialect: JdbcDialect,
      isolationLevel: Int,
      options: JDBCOptions): Unit = {

    if (iterator.isEmpty) {
      return
    }

    val outMetrics = TaskContext.get().taskMetrics().outputMetrics

    val conn = dialect.createConnectionFactory(options)(-1)
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
            logWarning(log"Requested isolation level ${MDC(ISOLATION_LEVEL, isolationLevel)} " +
              log"is not supported; falling back to default isolation level " +
              log"${MDC(DEFAULT_ISOLATION_LEVEL, defaultIsolation)}")
          }
        } else {
          logWarning(log"Requested isolation level ${MDC(ISOLATION_LEVEL, isolationLevel)}, " +
            log"but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE
    var totalRowCount = 0L
    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt = conn.prepareStatement(insertStmt)
      val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
      val nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0

        stmt.setQueryTimeout(options.queryTimeout)

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
          totalRowCount += 1
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
        if (cause != null && e.getCause != cause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(cause)
            }
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
        } else {
          outMetrics.setRecordsWritten(totalRowCount)
        }
        conn.close()
      } else {
        outMetrics.setRecordsWritten(totalRowCount)

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
  def schemaString(
      dialect: JdbcDialect,
      schema: StructType,
      caseSensitive: Boolean,
      createTableColumnTypes: Option[String] = None): String = {
    val sb = new StringBuilder()
    val userSpecifiedColTypesMap = createTableColumnTypes
      .map(parseUserSpecifiedCreateTableColumnTypes(dialect, schema, caseSensitive, _))
      .getOrElse(Map.empty[String, String])
    schema.foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ = userSpecifiedColTypesMap
        .getOrElse(field.name, getJdbcType(field.dataType, dialect).databaseTypeDefinition)
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   * Parses the user specified createTableColumnTypes option value string specified in the same
   * format as create table ddl column types, and returns Map of field name and the data type to
   * use in-place of the default data type.
   */
  private def parseUserSpecifiedCreateTableColumnTypes(
      dialect: JdbcDialect,
      schema: StructType,
      caseSensitive: Boolean,
      createTableColumnTypes: String): Map[String, String] = {
    val userSchema = CatalystSqlParser.parseTableSchema(createTableColumnTypes)

    // checks duplicate columns in the user specified column types.
    SchemaUtils.checkColumnNameDuplication(userSchema.map(_.name), conf.resolver)

    // checks if user specified column names exist in the DataFrame schema
    userSchema.fieldNames.foreach { col =>
      schema.find(f => conf.resolver(f.name, col)).getOrElse {
        throw QueryCompilationErrors.createTableColumnTypesOptionColumnNotFoundInSchemaError(
          col, schema)
      }
    }

    val userSchemaMap = userSchema
      .map(f => f.name -> getJdbcType(f.dataType, dialect).databaseTypeDefinition)
      .toMap
    if (caseSensitive) userSchemaMap else CaseInsensitiveMap(userSchemaMap)
  }

  /**
   * Parses the user specified customSchema option value to DataFrame schema, and
   * returns a schema that is replaced by the custom schema's dataType if column name is matched.
   */
  def getCustomSchema(
      tableSchema: StructType,
      customSchema: String,
      nameEquality: Resolver): StructType = {
    if (null != customSchema && customSchema.nonEmpty) {
      val userSchema = CatalystSqlParser.parseTableSchema(customSchema)

      SchemaUtils.checkSchemaColumnNameDuplication(userSchema, nameEquality)

      // This is resolved by names, use the custom filed dataType to replace the default dataType.
      val newSchema = tableSchema.map { col =>
        userSchema.find(f => nameEquality(f.name, col.name)) match {
          case Some(c) => col.copy(dataType = c.dataType)
          case None => col
        }
      }
      StructType(newSchema)
    } else {
      tableSchema
    }
  }

  /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveTable(
      df: DataFrame,
      tableSchema: Option[StructType],
      isCaseSensitive: Boolean,
      options: JdbcOptionsInWrite): Unit = {
    val url = options.url
    val table = options.table
    val dialect = JdbcDialects.get(url)
    val rddSchema = df.schema
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val insertStmt = getInsertStatement(table, rddSchema, tableSchema, isCaseSensitive, dialect)
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw QueryExecutionErrors.invalidJdbcNumPartitionsError(
        n, JDBCOptions.JDBC_NUM_PARTITIONS)
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    repartitionedDF.foreachPartition { iterator => savePartition(
      table, iterator, rddSchema, insertStmt, batchSize, dialect, isolationLevel, options)
    }
  }

  /**
   * Creates a table with a given schema.
   */
  def createTable(
      conn: Connection,
      tableName: String,
      schema: StructType,
      caseSensitive: Boolean,
      options: JdbcOptionsInWrite): Unit = {
    val statement = conn.createStatement
    val dialect = JdbcDialects.get(options.url)
    val strSchema = schemaString(
      dialect, schema, caseSensitive, options.createTableColumnTypes)
    try {
      statement.setQueryTimeout(options.queryTimeout)
      dialect.createTable(statement, tableName, strSchema, options)
      if (options.tableComment.nonEmpty) {
        try {
          val tableCommentQuery = dialect.getTableCommentQuery(tableName, options.tableComment)
          statement.executeUpdate(tableCommentQuery)
        } catch {
          case e: Exception =>
            logWarning("Cannot create JDBC table comment. The table comment will be ignored.")
        }
      }
    } finally {
      statement.close()
    }
  }

  /**
   * Rename a table from the JDBC database.
   */
  def renameTable(
      conn: Connection,
      oldTable: Identifier,
      newTable: Identifier,
      options: JDBCOptions): Unit = {
    val dialect = JdbcDialects.get(options.url)
    executeStatement(conn, options, dialect.renameTable(oldTable, newTable))
  }

  /**
   * Update a table from the JDBC database.
   */
  def alterTable(
      conn: Connection,
      tableName: String,
      changes: Seq[TableChange],
      options: JDBCOptions): Unit = {
    val dialect = JdbcDialects.get(options.url)
    val metaData = conn.getMetaData
    if (changes.length == 1) {
      executeStatement(conn, options, dialect.alterTable(tableName, changes,
        metaData.getDatabaseMajorVersion)(0))
    } else {
      if (!metaData.supportsTransactions) {
        throw QueryExecutionErrors.multiActionAlterError(tableName)
      } else {
        conn.setAutoCommit(false)
        val statement = conn.createStatement
        try {
          statement.setQueryTimeout(options.queryTimeout)
          for (sql <- dialect.alterTable(tableName, changes, metaData.getDatabaseMajorVersion)) {
            statement.executeUpdate(sql)
          }
          conn.commit()
        } catch {
          case e: Exception =>
            if (conn != null) conn.rollback()
            throw e
        } finally {
          statement.close()
          conn.setAutoCommit(true)
        }
      }
    }
  }

  /**
   * Creates a schema.
   */
  def createSchema(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      comment: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      val dialect = JdbcDialects.get(options.url)
      dialect.createSchema(statement, schema, comment)
    } finally {
      statement.close()
    }
  }

  def schemaExists(conn: Connection, options: JDBCOptions, schema: String): Boolean = {
    val dialect = JdbcDialects.get(options.url)
    dialect.schemasExists(conn, options, schema)
  }

  def listSchemas(conn: Connection, options: JDBCOptions): Array[Array[String]] = {
    val dialect = JdbcDialects.get(options.url)
    dialect.listSchemas(conn, options)
  }

  def alterSchemaComment(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      comment: String): Unit = {
    val dialect = JdbcDialects.get(options.url)
    executeStatement(conn, options, dialect.getSchemaCommentQuery(schema, comment))
  }

  def removeSchemaComment(
      conn: Connection,
      options: JDBCOptions,
      schema: String): Unit = {
    val dialect = JdbcDialects.get(options.url)
    executeStatement(conn, options, dialect.removeSchemaCommentQuery(schema))
  }

  /**
   * Drops a schema from the JDBC database.
   */
  def dropSchema(
      conn: Connection, options: JDBCOptions, schema: String, cascade: Boolean): Unit = {
    val dialect = JdbcDialects.get(options.url)
    executeStatement(conn, options, dialect.dropSchema(schema, cascade))
  }

  /**
   * Create an index.
   */
  def createIndex(
      conn: Connection,
      indexName: String,
      tableIdent: Identifier,
      columns: Array[NamedReference],
      columnsProperties: util.Map[NamedReference, util.Map[String, String]],
      properties: util.Map[String, String],
      options: JDBCOptions): Unit = {
    val dialect = JdbcDialects.get(options.url)
    executeStatement(conn, options,
      dialect.createIndex(indexName, tableIdent, columns, columnsProperties, properties))
  }

  /**
   * Check if an index exists
   */
  def indexExists(
      conn: Connection,
      indexName: String,
      tableIdent: Identifier,
      options: JDBCOptions): Boolean = {
    val dialect = JdbcDialects.get(options.url)
    dialect.indexExists(conn, indexName, tableIdent, options)
  }

  /**
   * Drop an index.
   */
  def dropIndex(
      conn: Connection,
      indexName: String,
      tableIdent: Identifier,
      options: JDBCOptions): Unit = {
    val dialect = JdbcDialects.get(options.url)
    executeStatement(conn, options, dialect.dropIndex(indexName, tableIdent))
  }

  /**
   * List all the indexes in a table.
   */
  def listIndexes(
      conn: Connection,
      tableIdent: Identifier,
      options: JDBCOptions): Array[TableIndex] = {
    val dialect = JdbcDialects.get(options.url)
    dialect.listIndexes(conn, tableIdent, options)
  }

  private def executeStatement(conn: Connection, options: JDBCOptions, sql: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }

  /**
   * Check if index exists in a table
   */
  def checkIfIndexExists(
      conn: Connection,
      sql: String,
      options: JDBCOptions): Boolean = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      val rs = statement.executeQuery(sql)
      rs.next
    } catch {
      case _: Exception =>
        logWarning("Cannot retrieved index info.")
        false
    } finally {
      statement.close()
    }
  }

  /**
   * Process index properties and return tuple of indexType and list of the other index properties.
   */
  def processIndexProperties(
      properties: util.Map[String, String],
      dialectName: String): (String, Array[String]) = {
    var indexType = ""
    val indexPropertyList: ArrayBuffer[String] = ArrayBuffer[String]()
    val supportedIndexTypeList = getSupportedIndexTypeList(dialectName)

    if (!properties.isEmpty) {
      properties.asScala.foreach { case (k, v) =>
        if (k.equals(SupportsIndex.PROP_TYPE)) {
          if (containsIndexTypeIgnoreCase(supportedIndexTypeList, v)) {
            indexType = s"USING $v"
          } else {
            throw new SparkUnsupportedOperationException(
              errorClass = "_LEGACY_ERROR_TEMP_3175",
              messageParameters = Map(
                "v" -> v,
                "supportedIndexTypeList" -> supportedIndexTypeList.mkString(" AND ")))
          }
        } else {
          indexPropertyList.append(s"$k = $v")
        }
      }
    }
    (indexType, indexPropertyList.toArray)
  }

  def containsIndexTypeIgnoreCase(supportedIndexTypeList: Array[String], value: String): Boolean = {
    if (supportedIndexTypeList.isEmpty) {
      throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3173")
    }
    for (indexType <- supportedIndexTypeList) {
      if (value.equalsIgnoreCase(indexType)) return true
    }
    false
  }

  def getSupportedIndexTypeList(dialectName: String): Array[String] = {
    dialectName match {
      case "mysql" => Array("BTREE", "HASH")
      case "postgresql" => Array("BTREE", "HASH", "BRIN")
      case _ => Array.empty
    }
  }

  def executeQuery(conn: Connection, options: JDBCOptions, sql: String)(
    f: ResultSet => Unit): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      val rs = statement.executeQuery(sql)
      try {
        f(rs)
      } finally {
        rs.close()
      }
    } finally {
      statement.close()
    }
  }

  def classifyException[T](
      condition: String,
      messageParameters: Map[String, String],
      dialect: JdbcDialect,
      description: String,
      isRuntime: Boolean)(f: => T): T = {
    try {
      f
    } catch {
      case e: SparkThrowable with Throwable => throw e
      case e: Throwable =>
        throw dialect.classifyException(e, condition, messageParameters, description, isRuntime)
    }
  }

  def withConnection[T](options: JDBCOptions)(f: Connection => T): T = {
    val dialect = JdbcDialects.get(options.url)
    val conn = dialect.createConnectionFactory(options)(-1)
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }
}
