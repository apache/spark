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

import java.sql.{Connection, Date, ResultSet, ResultSetMetaData, SQLException, Timestamp}
import java.util.Properties

import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Data corresponding to one partition of a JDBCRDD.
 */
case class JDBCPartition(whereClause: String, idx: Int) extends Partition {
  override def index: Int = idx
}

object JDBCRDD extends Logging {

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
   * Takes a (schema, table) specification and returns the table's Catalyst
   * schema.
   *
   * @param url - The JDBC url to fetch information from.
   * @param table - The table name of the desired table.  This may also be a
   *   SQL query wrapped in parentheses.
   *
   * @return A StructType giving the table's Catalyst schema.
   * @throws SQLException if the table specification is garbage.
   * @throws SQLException if the table contains an unsupported type.
   */
  def resolveTable(url: String, table: String, properties: Properties): StructType = {
    val dialect = JdbcDialects.get(url)
    val conn: Connection = JdbcUtils.createConnectionFactory(url, properties)()
    try {
      val statement = conn.prepareStatement(s"SELECT * FROM $table WHERE 1=0")
      try {
        val rs = statement.executeQuery()
        try {
          val rsmd = rs.getMetaData
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
          return new StructType(fields)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }

    throw new RuntimeException("This line is unreachable.")
  }

  /**
   * Prune all but the specified columns from the specified Catalyst schema.
   *
   * @param schema - The Catalyst schema of the master table
   * @param columns - The list of desired columns
   *
   * @return A Catalyst schema corresponding to columns in the given order.
   */
  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.metadata.getString("name") -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  /**
   * Converts value to SQL expression.
   */
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  /**
   * Turns a single Filter into a String representing a SQL expression.
   * Returns None for an unhandled filter.
   */
  def compileFilter(f: Filter): Option[String] = {
    Option(f match {
      case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
      case EqualNullSafe(attr, value) =>
        s"(NOT ($attr != ${compileValue(value)} OR $attr IS NULL OR " +
          s"${compileValue(value)} IS NULL) OR ($attr IS NULL AND ${compileValue(value)} IS NULL))"
      case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
      case IsNull(attr) => s"$attr IS NULL"
      case IsNotNull(attr) => s"$attr IS NOT NULL"
      case StringStartsWith(attr, value) => s"${attr} LIKE '${value}%'"
      case StringEndsWith(attr, value) => s"${attr} LIKE '%${value}'"
      case StringContains(attr, value) => s"${attr} LIKE '%${value}%'"
      case In(attr, value) => s"$attr IN (${compileValue(value)})"
      case Not(f) => compileFilter(f).map(p => s"(NOT ($p))").getOrElse(null)
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter(_))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter(_))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }



  /**
   * Build and return JDBCRDD from the given information.
   *
   * @param sc - Your SparkContext.
   * @param schema - The Catalyst schema of the underlying database table.
   * @param url - The JDBC url to connect to.
   * @param fqTable - The fully-qualified table name (or paren'd SQL query) to use.
   * @param requiredColumns - The names of the columns to SELECT.
   * @param filters - The filters to include in all WHERE clauses.
   * @param parts - An array of JDBCPartitions specifying partition ids and
   *    per-partition WHERE clauses.
   *
   * @return An RDD representing "SELECT requiredColumns FROM fqTable".
   */
  def scanTable(
      sc: SparkContext,
      schema: StructType,
      url: String,
      properties: Properties,
      fqTable: String,
      requiredColumns: Array[String],
      filters: Array[Filter],
      parts: Array[Partition]): RDD[InternalRow] = {
    val dialect = JdbcDialects.get(url)
    val quotedColumns = requiredColumns.map(colName => dialect.quoteIdentifier(colName))
    new JDBCRDD(
      sc,
      JdbcUtils.createConnectionFactory(url, properties),
      pruneSchema(schema, requiredColumns),
      fqTable,
      quotedColumns,
      filters,
      parts,
      url,
      properties)
  }
}

/**
 * An RDD representing a table in a database accessed via JDBC.  Both the
 * driver code and the workers must be able to access the database; the driver
 * needs to fetch the schema while the workers need to fetch the data.
 */
private[jdbc] class JDBCRDD(
    sc: SparkContext,
    getConnection: () => Connection,
    schema: StructType,
    fqTable: String,
    columns: Array[String],
    filters: Array[Filter],
    partitions: Array[Partition],
    url: String,
    properties: Properties)
  extends RDD[InternalRow](sc, Nil) {

  /**
   * Retrieve the list of partitions corresponding to this RDD.
   */
  override def getPartitions: Array[Partition] = partitions

  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  private val columnList: String = {
    val sb = new StringBuilder()
    columns.foreach(x => sb.append(",").append(x))
    if (sb.length == 0) "1" else sb.substring(1)
  }

  /**
   * `filters`, but as a WHERE clause suitable for injection into a SQL query.
   */
  private val filterWhereClause: String =
    filters.flatMap(JDBCRDD.compileFilter).map(p => s"($p)").mkString(" AND ")

  /**
   * A WHERE clause representing both `filters`, if any, and the current partition.
   */
  private def getWhereClause(part: JDBCPartition): String = {
    if (part.whereClause != null && filterWhereClause.length > 0) {
      "WHERE " + s"($filterWhereClause)" + " AND " + s"(${part.whereClause})"
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else if (filterWhereClause.length > 0) {
      "WHERE " + filterWhereClause
    } else {
      ""
    }
  }

  // Each JDBC-to-Catalyst conversion corresponds to a tag defined here so that
  // we don't have to potentially poke around in the Metadata once for every
  // row.
  // Is there a better way to do this?  I'd rather be using a type that
  // contains only the tags I define.
  abstract class JDBCConversion
  case object BooleanConversion extends JDBCConversion
  case object DateConversion extends JDBCConversion
  case class  DecimalConversion(precision: Int, scale: Int) extends JDBCConversion
  case object DoubleConversion extends JDBCConversion
  case object FloatConversion extends JDBCConversion
  case object IntegerConversion extends JDBCConversion
  case object LongConversion extends JDBCConversion
  case object BinaryLongConversion extends JDBCConversion
  case object StringConversion extends JDBCConversion
  case object TimestampConversion extends JDBCConversion
  case object BinaryConversion extends JDBCConversion
  case class ArrayConversion(elementConversion: JDBCConversion) extends JDBCConversion

  /**
   * Maps a StructType to a type tag list.
   */
  def getConversions(schema: StructType): Array[JDBCConversion] =
    schema.fields.map(sf => getConversions(sf.dataType, sf.metadata))

  private def getConversions(dt: DataType, metadata: Metadata): JDBCConversion = dt match {
    case BooleanType => BooleanConversion
    case DateType => DateConversion
    case DecimalType.Fixed(p, s) => DecimalConversion(p, s)
    case DoubleType => DoubleConversion
    case FloatType => FloatConversion
    case IntegerType => IntegerConversion
    case LongType => if (metadata.contains("binarylong")) BinaryLongConversion else LongConversion
    case StringType => StringConversion
    case TimestampType => TimestampConversion
    case BinaryType => BinaryConversion
    case ArrayType(et, _) => ArrayConversion(getConversions(et, metadata))
    case _ => throw new IllegalArgumentException(s"Unsupported type ${dt.simpleString}")
  }

  /**
   * Runs the SQL query against the JDBC driver.
   *
   */
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] =
    new Iterator[InternalRow] {
    var closed = false
    var finished = false
    var gotNext = false
    var nextValue: InternalRow = null

    context.addTaskCompletionListener{ context => close() }
    val inputMetrics = context.taskMetrics().inputMetrics
    val part = thePart.asInstanceOf[JDBCPartition]
    val conn = getConnection()
    val dialect = JdbcDialects.get(url)
    import scala.collection.JavaConverters._
    dialect.beforeFetch(conn, properties.asScala.toMap)

    // H2's JDBC driver does not support the setSchema() method.  We pass a
    // fully-qualified table name in the SELECT statement.  I don't know how to
    // talk about a table in a completely portable way.

    val myWhereClause = getWhereClause(part)

    val sqlText = s"SELECT $columnList FROM $fqTable $myWhereClause"
    val stmt = conn.prepareStatement(sqlText,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    val fetchSize = properties.getProperty(JdbcUtils.JDBC_BATCH_FETCH_SIZE, "0").toInt
    require(fetchSize >= 0,
      s"Invalid value `${fetchSize.toString}` for parameter " +
      s"`${JdbcUtils.JDBC_BATCH_FETCH_SIZE}`. The minimum value is 0. When the value is 0, " +
      "the JDBC driver ignores the value and does the estimates.")
    stmt.setFetchSize(fetchSize)
    val rs = stmt.executeQuery()

    val conversions = getConversions(schema)
    val mutableRow = new SpecificMutableRow(schema.fields.map(x => x.dataType))

    def getNext(): InternalRow = {
      if (rs.next()) {
        inputMetrics.incRecordsRead(1)
        var i = 0
        while (i < conversions.length) {
          val pos = i + 1
          conversions(i) match {
            case BooleanConversion => mutableRow.setBoolean(i, rs.getBoolean(pos))
            case DateConversion =>
              // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
              val dateVal = rs.getDate(pos)
              if (dateVal != null) {
                mutableRow.setInt(i, DateTimeUtils.fromJavaDate(dateVal))
              } else {
                mutableRow.update(i, null)
              }
            // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
            // object returned by ResultSet.getBigDecimal is not correctly matched to the table
            // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
            // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
            // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
            // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
            // retrieve it, you will get wrong result 199.99.
            // So it is needed to set precision and scale for Decimal based on JDBC metadata.
            case DecimalConversion(p, s) =>
              val decimalVal = rs.getBigDecimal(pos)
              if (decimalVal == null) {
                mutableRow.update(i, null)
              } else {
                mutableRow.update(i, Decimal(decimalVal, p, s))
              }
            case DoubleConversion => mutableRow.setDouble(i, rs.getDouble(pos))
            case FloatConversion => mutableRow.setFloat(i, rs.getFloat(pos))
            case IntegerConversion => mutableRow.setInt(i, rs.getInt(pos))
            case LongConversion => mutableRow.setLong(i, rs.getLong(pos))
            // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
            case StringConversion => mutableRow.update(i, UTF8String.fromString(rs.getString(pos)))
            case TimestampConversion =>
              val t = rs.getTimestamp(pos)
              if (t != null) {
                mutableRow.setLong(i, DateTimeUtils.fromJavaTimestamp(t))
              } else {
                mutableRow.update(i, null)
              }
            case BinaryConversion => mutableRow.update(i, rs.getBytes(pos))
            case BinaryLongConversion =>
              val bytes = rs.getBytes(pos)
              var ans = 0L
              var j = 0
              while (j < bytes.size) {
                ans = 256 * ans + (255 & bytes(j))
                j = j + 1
              }
              mutableRow.setLong(i, ans)
            case ArrayConversion(elementConversion) =>
              val array = rs.getArray(pos).getArray
              if (array != null) {
                val data = elementConversion match {
                  case TimestampConversion =>
                    array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
                      nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
                    }
                  case StringConversion =>
                    array.asInstanceOf[Array[java.lang.String]]
                      .map(UTF8String.fromString)
                  case DateConversion =>
                    array.asInstanceOf[Array[java.sql.Date]].map { date =>
                      nullSafeConvert(date, DateTimeUtils.fromJavaDate)
                    }
                  case DecimalConversion(p, s) =>
                    array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
                      nullSafeConvert[java.math.BigDecimal](decimal, d => Decimal(d, p, s))
                    }
                  case BinaryLongConversion =>
                    throw new IllegalArgumentException(s"Unsupported array element conversion $i")
                  case _: ArrayConversion =>
                    throw new IllegalArgumentException("Nested arrays unsupported")
                  case _ => array.asInstanceOf[Array[Any]]
                }
                mutableRow.update(i, new GenericArrayData(data))
              } else {
                mutableRow.update(i, null)
              }
          }
          if (rs.wasNull) mutableRow.setNullAt(i)
          i = i + 1
        }
        mutableRow
      } else {
        finished = true
        null.asInstanceOf[InternalRow]
      }
    }

    def close() {
      if (closed) return
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          if (!conn.isClosed && !conn.getAutoCommit) {
            try {
              conn.commit()
            } catch {
              case NonFatal(e) => logWarning("Exception committing transaction", e)
            }
          }
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
      closed = true
    }

    override def hasNext: Boolean = {
      if (!finished) {
        if (!gotNext) {
          nextValue = getNext()
          if (finished) {
            close()
          }
          gotNext = true
        }
      }
      !finished
    }

    override def next(): InternalRow = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      nextValue
    }
  }

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }
}
