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

package org.apache.spark.sql.connect.client.jdbc

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql.{Array => JdbcArray, _}
import java.util
import java.util.Calendar

import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.client.SparkResult

class SparkConnectResultSet(
    sparkResult: SparkResult[Row],
    stmt: SparkConnectStatement = null) extends ResultSet {

  private val iterator = sparkResult.destructiveIterator

  private var currentRow: Row = _

  private var _wasNull: Boolean = false

  override def wasNull: Boolean = _wasNull

  override def next(): Boolean = {
    val hasNext = iterator.hasNext
    if (hasNext) {
      currentRow = iterator.next()
    } else {
      currentRow = null
    }
    hasNext
  }

  @volatile protected var closed: Boolean = false

  override def isClosed: Boolean = closed

  override def close(): Unit = synchronized {
    if (!closed) {
      iterator.close()
      sparkResult.close()
      closed = true
    }
  }

  private[jdbc] def checkOpen(): Unit = {
    if (closed) {
      throw new SQLException("JDBC Statement is closed.")
    }
  }

  override def findColumn(columnLabel: String): Int = {
    sparkResult.schema.getFieldIndex(columnLabel) match {
      case Some(i) => i + 1
      case None =>
        throw new SQLException(s"Invalid column label: $columnLabel")
    }
  }

  override def getString(columnIndex: Int): String = {
    if (currentRow.isNullAt(columnIndex - 1)) {
      _wasNull = true
      return null
    }
    _wasNull = false
    String.valueOf(currentRow.get(columnIndex - 1))
  }

  override def getBoolean(columnIndex: Int): Boolean = {
    if (currentRow.isNullAt(columnIndex - 1)) {
      _wasNull = true
      return false
    }
    _wasNull = false
    currentRow.getBoolean(columnIndex - 1)
  }

  override def getByte(columnIndex: Int): Byte = {
    if (currentRow.isNullAt(columnIndex - 1)) {
      _wasNull = true
      return 0.toByte
    }
    _wasNull = false
    currentRow.getByte(columnIndex - 1)
  }

  override def getShort(columnIndex: Int): Short = {
    if (currentRow.isNullAt(columnIndex - 1)) {
      _wasNull = true
      return 0.toShort
    }
    _wasNull = false
    currentRow.getShort(columnIndex - 1)
  }

  override def getInt(columnIndex: Int): Int = {
    if (currentRow.isNullAt(columnIndex - 1)) {
      _wasNull = true
      return 0
    }
    _wasNull = false
    currentRow.getInt(columnIndex - 1)
  }

  override def getLong(columnIndex: Int): Long = {
    if (currentRow.isNullAt(columnIndex - 1)) {
      _wasNull = true
      return 0L
    }
    _wasNull = false
    currentRow.getLong(columnIndex - 1)
  }

  override def getFloat(columnIndex: Int): Float = {
    if (currentRow.isNullAt(columnIndex - 1)) {
      _wasNull = true
      return 0.toFloat
    }
    _wasNull = false
    currentRow.getFloat(columnIndex - 1)
  }

  override def getDouble(columnIndex: Int): Double = {
    if (currentRow.isNullAt(columnIndex - 1)) {
      _wasNull = true
      return 0.toDouble
    }
    _wasNull = false
    currentRow.getDouble(columnIndex - 1)
  }

  override def getBigDecimal(columnIndex: Int, scale: Int): java.math.BigDecimal =
    throw new SQLFeatureNotSupportedException

  override def getBytes(columnIndex: Int): Array[Byte] =
    throw new SQLFeatureNotSupportedException

  override def getDate(columnIndex: Int): Date =
    throw new SQLFeatureNotSupportedException

  override def getTime(columnIndex: Int): Time =
    throw new SQLFeatureNotSupportedException

  override def getTimestamp(columnIndex: Int): Timestamp =
    throw new SQLFeatureNotSupportedException

  override def getAsciiStream(columnIndex: Int): InputStream =
    throw new SQLFeatureNotSupportedException

  override def getUnicodeStream(columnIndex: Int): InputStream =
    throw new SQLFeatureNotSupportedException

  override def getBinaryStream(columnIndex: Int): InputStream =
    throw new SQLFeatureNotSupportedException

  override def getString(columnLabel: String): String =
    getString(findColumn(columnLabel))

  override def getBoolean(columnLabel: String): Boolean =
    getBoolean(findColumn(columnLabel))

  override def getByte(columnLabel: String): Byte =
    getByte(findColumn(columnLabel))

  override def getShort(columnLabel: String): Short =
    getShort(findColumn(columnLabel))

  override def getInt(columnLabel: String): Int =
    getInt(findColumn(columnLabel))

  override def getLong(columnLabel: String): Long =
    getLong(findColumn(columnLabel))

  override def getFloat(columnLabel: String): Float =
    getFloat(findColumn(columnLabel))

  override def getDouble(columnLabel: String): Double =
    getDouble(findColumn(columnLabel))

  override def getBigDecimal(columnLabel: String, scale: Int): java.math.BigDecimal =
    throw new SQLFeatureNotSupportedException

  override def getBytes(columnLabel: String): Array[Byte] =
    throw new SQLFeatureNotSupportedException

  override def getDate(columnLabel: String): Date =
    throw new SQLFeatureNotSupportedException

  override def getTime(columnLabel: String): Time =
    throw new SQLFeatureNotSupportedException

  override def getTimestamp(columnLabel: String): Timestamp =
    throw new SQLFeatureNotSupportedException

  override def getAsciiStream(columnLabel: String): InputStream =
    throw new SQLFeatureNotSupportedException

  override def getUnicodeStream(columnLabel: String): InputStream =
    throw new SQLFeatureNotSupportedException

  override def getBinaryStream(columnLabel: String): InputStream =
    throw new SQLFeatureNotSupportedException

  override def getWarnings: SQLWarning = null

  override def clearWarnings(): Unit = {}

  override def getCursorName: String = throw new SQLFeatureNotSupportedException

  override def getMetaData: ResultSetMetaData = {
    checkOpen()
    new SparkConnectResultSetMetaData(sparkResult.schema)
  }

  override def getObject(columnIndex: Int): AnyRef = {
    if (currentRow.isNullAt(columnIndex - 1)) {
      _wasNull = true
      return null
    }
    _wasNull = false
    currentRow.get(columnIndex - 1).asInstanceOf[AnyRef]
  }

  override def getObject(columnLabel: String): AnyRef =
    getObject(findColumn(columnLabel))

  override def getCharacterStream(columnIndex: Int): Reader =
    throw new SQLFeatureNotSupportedException

  override def getCharacterStream(columnLabel: String): Reader =
    throw new SQLFeatureNotSupportedException

  override def getBigDecimal(columnIndex: Int): java.math.BigDecimal =
    throw new SQLFeatureNotSupportedException

  override def getBigDecimal(columnLabel: String): java.math.BigDecimal =
    throw new SQLFeatureNotSupportedException

  override def isBeforeFirst: Boolean = throw new SQLFeatureNotSupportedException

  override def isAfterLast: Boolean = throw new SQLFeatureNotSupportedException

  override def isFirst: Boolean = throw new SQLFeatureNotSupportedException

  override def isLast: Boolean = throw new SQLFeatureNotSupportedException

  override def beforeFirst(): Unit = throw new SQLFeatureNotSupportedException

  override def afterLast(): Unit = throw new SQLFeatureNotSupportedException

  override def first(): Boolean = throw new SQLFeatureNotSupportedException

  override def last(): Boolean = throw new SQLFeatureNotSupportedException

  override def getRow: Int = throw new SQLFeatureNotSupportedException

  override def absolute(row: Int): Boolean = throw new SQLFeatureNotSupportedException

  override def relative(rows: Int): Boolean = throw new SQLFeatureNotSupportedException

  override def previous(): Boolean = throw new SQLFeatureNotSupportedException

  override def setFetchDirection(direction: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def getFetchDirection: Int =
    throw new SQLFeatureNotSupportedException

  override def setFetchSize(rows: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def getFetchSize: Int =
    throw new SQLFeatureNotSupportedException

  override def getType: Int = {
    checkOpen()
    ResultSet.TYPE_FORWARD_ONLY
  }

  override def getConcurrency: Int = {
    checkOpen()
    ResultSet.CONCUR_READ_ONLY
  }

  override def rowUpdated(): Boolean =
    throw new SQLFeatureNotSupportedException

  override def rowInserted(): Boolean =
    throw new SQLFeatureNotSupportedException

  override def rowDeleted(): Boolean =
    throw new SQLFeatureNotSupportedException

  override def updateNull(columnIndex: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateByte(columnIndex: Int, x: Byte): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateShort(columnIndex: Int, x: Short): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateInt(columnIndex: Int, x: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateLong(columnIndex: Int, x: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateFloat(columnIndex: Int, x: Float): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateDouble(columnIndex: Int, x: Double): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBigDecimal(columnIndex: Int, x: java.math.BigDecimal): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateString(columnIndex: Int, x: String): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBytes(columnIndex: Int, x: scala.Array[Byte]): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateDate(columnIndex: Int, x: Date): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateTime(columnIndex: Int, x: Time): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateObject(columnIndex: Int, x: Any, scaleOrLength: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateObject(columnIndex: Int, x: Any): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNull(columnLabel: String): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBoolean(columnLabel: String, x: Boolean): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateByte(columnLabel: String, x: Byte): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateShort(columnLabel: String, x: Short): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateInt(columnLabel: String, x: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateLong(columnLabel: String, x: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateFloat(columnLabel: String, x: Float): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateDouble(columnLabel: String, x: Double): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBigDecimal(columnLabel: String, x: java.math.BigDecimal): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateString(columnLabel: String, x: String): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBytes(columnLabel: String, x: Array[Byte]): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateDate(columnLabel: String, x: Date): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateTime(columnLabel: String, x: Time): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateTimestamp(columnLabel: String, x: Timestamp): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateObject(columnLabel: String, x: Any, scaleOrLength: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateObject(columnLabel: String, x: Any): Unit =
    throw new SQLFeatureNotSupportedException

  override def insertRow(): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateRow(): Unit =
    throw new SQLFeatureNotSupportedException

  override def deleteRow(): Unit =
    throw new SQLFeatureNotSupportedException

  override def refreshRow(): Unit =
    throw new SQLFeatureNotSupportedException

  override def cancelRowUpdates(): Unit =
    throw new SQLFeatureNotSupportedException

  override def moveToInsertRow(): Unit =
    throw new SQLFeatureNotSupportedException

  override def moveToCurrentRow(): Unit =
    throw new SQLFeatureNotSupportedException

  override def getStatement: Statement = {
    checkOpen()
    stmt
  }

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef =
    throw new SQLFeatureNotSupportedException

  override def getRef(columnIndex: Int): Ref =
    throw new SQLFeatureNotSupportedException

  override def getBlob(columnIndex: Int): Blob =
    throw new SQLFeatureNotSupportedException

  override def getClob(columnIndex: Int): Clob =
    throw new SQLFeatureNotSupportedException

  override def getArray(columnIndex: Int): JdbcArray =
    throw new SQLFeatureNotSupportedException

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef =
    throw new SQLFeatureNotSupportedException

  override def getRef(columnLabel: String): Ref =
    throw new SQLFeatureNotSupportedException

  override def getBlob(columnLabel: String): Blob =
    throw new SQLFeatureNotSupportedException

  override def getClob(columnLabel: String): Clob =
    throw new SQLFeatureNotSupportedException

  override def getArray(columnLabel: String): JdbcArray =
    throw new SQLFeatureNotSupportedException

  override def getDate(columnIndex: Int, cal: Calendar): Date =
    throw new SQLFeatureNotSupportedException

  override def getDate(columnLabel: String, cal: Calendar): Date =
    throw new SQLFeatureNotSupportedException

  override def getTime(columnIndex: Int, cal: Calendar): Time =
    throw new SQLFeatureNotSupportedException

  override def getTime(columnLabel: String, cal: Calendar): Time =
    throw new SQLFeatureNotSupportedException

  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp =
    throw new SQLFeatureNotSupportedException

  override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp =
    throw new SQLFeatureNotSupportedException

  override def getURL(columnIndex: Int): URL =
    throw new SQLFeatureNotSupportedException

  override def getURL(columnLabel: String): URL =
    throw new SQLFeatureNotSupportedException

  override def updateRef(columnIndex: Int, x: Ref): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateRef(columnLabel: String, x: Ref): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBlob(columnIndex: Int, x: Blob): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBlob(columnLabel: String, x: Blob): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateClob(columnIndex: Int, x: Clob): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateClob(columnLabel: String, x: Clob): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateArray(columnIndex: Int, x: JdbcArray): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateArray(columnLabel: String, x: JdbcArray): Unit =
    throw new SQLFeatureNotSupportedException

  override def getRowId(columnIndex: Int): RowId =
    throw new SQLFeatureNotSupportedException

  override def getRowId(columnLabel: String): RowId =
    throw new SQLFeatureNotSupportedException

  override def updateRowId(columnIndex: Int, x: RowId): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateRowId(columnLabel: String, x: RowId): Unit =
    throw new SQLFeatureNotSupportedException

  override def getHoldability: Int = ResultSet.HOLD_CURSORS_OVER_COMMIT

  override def updateNString(columnIndex: Int, nString: String): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNString(columnLabel: String, nString: String): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNClob(columnIndex: Int, nClob: NClob): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNClob(columnLabel: String, nClob: NClob): Unit =
    throw new SQLFeatureNotSupportedException

  override def getNClob(columnIndex: Int): NClob =
    throw new SQLFeatureNotSupportedException

  override def getNClob(columnLabel: String): NClob =
    throw new SQLFeatureNotSupportedException

  override def getSQLXML(columnIndex: Int): SQLXML =
    throw new SQLFeatureNotSupportedException

  override def getSQLXML(columnLabel: String): SQLXML =
    throw new SQLFeatureNotSupportedException

  override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit =
    throw new SQLFeatureNotSupportedException

  override def getNString(columnIndex: Int): String =
    throw new SQLFeatureNotSupportedException

  override def getNString(columnLabel: String): String =
    throw new SQLFeatureNotSupportedException

  override def getNCharacterStream(columnIndex: Int): Reader =
    throw new SQLFeatureNotSupportedException

  override def getNCharacterStream(columnLabel: String): Reader =
    throw new SQLFeatureNotSupportedException

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateClob(columnIndex: Int, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateClob(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNClob(columnIndex: Int, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException

  override def updateNClob(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T =
    throw new SQLFeatureNotSupportedException

  override def getObject[T](columnLabel: String, `type`: Class[T]): T =
    throw new SQLFeatureNotSupportedException

  override def unwrap[T](iface: Class[T]): T = if (isWrapperFor(iface)) {
    iface.asInstanceOf[T]
  } else {
    throw new SQLException(s"${this.getClass.getName} not unwrappable from ${iface.getName}")
  }

  override def isWrapperFor(iface: Class[_]): Boolean = iface.isInstance(this)
}
