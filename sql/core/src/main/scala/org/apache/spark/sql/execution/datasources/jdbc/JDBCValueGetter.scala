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
import java.sql.{Date, ResultSet, Time, Timestamp}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_MILLIS
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A `JDBCValueGetter` is responsible for getting a value from a `ResultSet` into a field of an
 * `InternalRow`. `pos` is the index for the value to be set in the row, and is also used for the
 * value in the `ResultSet`. `JdbcUtils.makeGetter` selects one getter per column.
 */
private[jdbc] sealed trait JDBCValueGetter {
  def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit
}

private[jdbc] object JDBCValueGetter {
  case object BooleanGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.setBoolean(pos, rs.getBoolean(pos + 1))
  }

  final case class DateGetter(dialect: JdbcDialect) extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
      // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
      val dateVal = rs.getDate(pos + 1)
      if (dateVal != null) {
        row.setInt(pos, fromJavaDate(dialect.convertJavaDateToDate(dateVal)))
      } else {
        row.update(pos, null)
      }
    }
  }

  case object TimeGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
      val localTime = rs.getObject(pos + 1, classOf[java.time.LocalTime])
      if (localTime != null) {
        row.setLong(pos, localTime.toNanoOfDay)
      } else {
        row.update(pos, null)
      }
    }
  }

  // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
  // object returned by ResultSet.getBigDecimal is not correctly matched to the table
  // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
  // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
  // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
  // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
  // retrieve it, you will get wrong result 199.99.
  // So it is needed to set precision and scale for Decimal based on JDBC metadata.
  final case class DecimalGetter(precision: Int, scale: Int) extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
      val decimal =
        nullSafeConvert[JBigDecimal](rs.getBigDecimal(pos + 1), d => Decimal(d, precision, scale))
      row.update(pos, decimal)
    }
  }

  case object DoubleGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.setDouble(pos, rs.getDouble(pos + 1))
  }

  case object FloatGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.setFloat(pos, rs.getFloat(pos + 1))
  }

  case object IntGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.setInt(pos, rs.getInt(pos + 1))
  }

  case object BinaryLongGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
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
    }
  }

  case object LongGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.setLong(pos, rs.getLong(pos + 1))
  }

  case object ShortGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.setShort(pos, rs.getShort(pos + 1))
  }

  case object ByteGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.setByte(pos, rs.getByte(pos + 1))
  }

  case object RowIdGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
      val rawRowId = rs.getRowId(pos + 1)
      if (rawRowId == null) {
        row.update(pos, null)
      } else {
        row.update(pos, UTF8String.fromString(rawRowId.toString))
      }
    }
  }

  case object StringGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
      row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))
  }

  // SPARK-34357 - sql TIME type represents as zero epoch timestamp.
  // It is mapped as Spark TimestampType but fixed at 1970-01-01 for day,
  // time portion is time of day, with no reference to a particular calendar,
  // time zone or date, with a precision till microseconds.
  // It stores the number of milliseconds after midnight, 00:00:00.000000
  case object LogicalTimeGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.update(pos, nullSafeConvert[Time](
        rs.getTime(pos + 1), t => Math.multiplyExact(t.getTime, MICROS_PER_MILLIS)))
  }

  final case class TimestampGetter(dialect: JdbcDialect) extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
      val t = rs.getTimestamp(pos + 1)
      if (t != null) {
        row.setLong(pos, fromJavaTimestamp(dialect.convertJavaTimestampToTimestamp(t)))
      } else {
        row.update(pos, null)
      }
    }
  }

  final case class LogicalTimeNTZGetter(dialect: JdbcDialect) extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
      val micros = nullSafeConvert[Time](rs.getTime(pos + 1), t => {
        val time = dialect.convertJavaTimestampToTimestampNTZ(new Timestamp(t.getTime))
        localDateTimeToMicros(time)
      })
      row.update(pos, micros)
    }
  }

  final case class TimestampNTZGetter(dialect: JdbcDialect) extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
      val t = rs.getTimestamp(pos + 1)
      if (t != null) {
        row.setLong(pos, localDateTimeToMicros(dialect.convertJavaTimestampToTimestampNTZ(t)))
      } else {
        row.update(pos, null)
      }
    }
  }

  case object BinaryBitGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
      val bytes = rs.getBytes(pos + 1)
      if (bytes != null) {
        val binary = bytes.flatMap(Integer.toBinaryString(_).getBytes(StandardCharsets.US_ASCII))
        row.update(pos, binary)
      } else {
        row.update(pos, null)
      }
    }
  }

  case object BytesGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.update(pos, rs.getBytes(pos + 1))
  }

  final case class YearMonthIntervalGetter(dialect: JdbcDialect) extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.update(pos,
        nullSafeConvert(rs.getString(pos + 1), dialect.getYearMonthIntervalAsMonths))
  }

  final case class DayTimeIntervalGetter(dialect: JdbcDialect) extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      row.update(pos,
        nullSafeConvert(rs.getString(pos + 1), dialect.getDayTimeIntervalAsMicros))
  }

  // SPARK-47628: Handle PostgreSQL bit(n>1) array type ahead. As in the pgjdbc driver,
  // bit(n>1)[] is not distinguishable from bit(1)[], and they are all recognized as boolean[].
  // This is wrong for bit(n>1)[], so we need to handle it first as byte array.
  case object PostgresBitArrayGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = {
      val fieldString = rs.getString(pos + 1)
      if (fieldString != null) {
        val strArray = fieldString.substring(1, fieldString.length - 1).split(",")
        // Charset is picked from the pgjdbc driver for consistency.
        val bytesArray = strArray.map(_.getBytes(StandardCharsets.US_ASCII))
        row.update(pos, new GenericArrayData(bytesArray))
      } else {
        row.update(pos, null)
      }
    }
  }

  final case class ArrayGetter(arrayType: ArrayType, dialect: JdbcDialect, metadata: Metadata)
      extends JDBCValueGetter {
    private def elementConversion(et: DataType): AnyRef => Any = et match {
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
        throw QueryExecutionErrors.unsupportedArrayElementTypeBasedOnBinaryError(arrayType)

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

    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit =
      try {
        val array = nullSafeConvert[java.sql.Array](
          input = rs.getArray(pos + 1),
          arr => new GenericArrayData(elementConversion(arrayType.elementType)(arr.getArray())))
        row.update(pos, array)
      } catch {
        case _: java.lang.ClassCastException =>
          throw QueryExecutionErrors.wrongDatatypeInSomeRows(pos, arrayType)
      }
  }

  case object NullGetter extends JDBCValueGetter {
    def apply(rs: ResultSet, row: InternalRow, pos: Int): Unit = row.update(pos, null)
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
}
