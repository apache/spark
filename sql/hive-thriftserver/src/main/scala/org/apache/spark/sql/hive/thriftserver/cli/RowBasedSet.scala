/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.service.cli.thrift._
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.thriftserver.utils.SparkSQLUtils
import org.apache.spark.sql.types._

/**
 * A result set of Spark's [[Row]]s with its [[StructType]] as its schema,
 * with the ability of
 * transform to [[TRowSet]].
 */
case class RowBasedSet(types: StructType,
                       rows: ArrayBuffer[Row], initStartOffset: Long)
  extends RowSet {

  var startOffset: Long = initStartOffset

  override def toTRowSet: TRowSet = new TRowSet(startOffset, toTRows.asJava)

  private[this] def toTRows: Seq[TRow] = {
    if (rows != null) {
      if (types == null || (rows.nonEmpty && rows.head.size != types.size)) {
        throw new IllegalArgumentException("The given schema does't match the given row")
      }
      rows.map(toTRow)
    } else {
      Nil
    }
  }

  private[this] def toTRow(row: Row): TRow = {
    val tRow = new TRow()
    (0 until row.length).map(i => toTColumnValue(i, row)).foreach(tRow.addToColVals)
    tRow
  }

  private[this] def toTColumnValue(ordinal: Int, row: Row): TColumnValue =
    types(ordinal).dataType match {
      case BooleanType =>
        val boolValue = new TBoolValue
        if (!row.isNullAt(ordinal)) boolValue.setValue(row.getBoolean(ordinal))
        TColumnValue.boolVal(boolValue)

      case ByteType =>
        val byteValue = new TByteValue
        if (!row.isNullAt(ordinal)) byteValue.setValue(row.getByte(ordinal))
        TColumnValue.byteVal(byteValue)

      case ShortType =>
        val tI16Value = new TI16Value
        if (!row.isNullAt(ordinal)) tI16Value.setValue(row.getShort(ordinal))
        TColumnValue.i16Val(tI16Value)

      case IntegerType =>
        val tI32Value = new TI32Value
        if (!row.isNullAt(ordinal)) tI32Value.setValue(row.getInt(ordinal))
        TColumnValue.i32Val(tI32Value)

      case LongType =>
        val tI64Value = new TI64Value
        if (!row.isNullAt(ordinal)) tI64Value.setValue(row.getLong(ordinal))
        TColumnValue.i64Val(tI64Value)

      case FloatType =>
        val tDoubleValue = new TDoubleValue
        if (!row.isNullAt(ordinal)) tDoubleValue.setValue(row.getFloat(ordinal))
        TColumnValue.doubleVal(tDoubleValue)

      case DoubleType =>
        val tDoubleValue = new TDoubleValue
        if (!row.isNullAt(ordinal)) tDoubleValue.setValue(row.getDouble(ordinal))
        TColumnValue.doubleVal(tDoubleValue)

      case StringType =>
        val tStringValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStringValue.setValue(row.getString(ordinal))
        TColumnValue.stringVal(tStringValue)

      case DecimalType() =>
        val tStrValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStrValue.setValue(row.getDecimal(ordinal).toString)
        TColumnValue.stringVal(tStrValue)

      case DateType =>
        val tStringValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStringValue.setValue(row.get(ordinal).toString)
        TColumnValue.stringVal(tStringValue)

      case TimestampType =>
        val tStringValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStringValue.setValue(row.get(ordinal).toString)
        TColumnValue.stringVal(tStringValue)

      case BinaryType =>
        val tStringValue = new TStringValue
        if (!row.isNullAt(ordinal)) {
          val bytes = row.getAs[Array[Byte]](ordinal)
          tStringValue.setValue(SparkSQLUtils.toHiveString((bytes, types(ordinal).dataType)))
        }
        TColumnValue.stringVal(tStringValue)

      case _: ArrayType | _: StructType | _: MapType =>
        val tStrValue = new TStringValue
        if (!row.isNullAt(ordinal)) {
          tStrValue.setValue(
            SparkSQLUtils.toHiveString((row.get(ordinal), types(ordinal).dataType)))
        }
        TColumnValue.stringVal(tStrValue)
    }

  override def addRow(row: Row): RowSet = {
    rows.+=(row)
    this
  }

  override def extractSubset(maxRows: Int): RowSet = {
    val numRows = Math.min(rows.size, maxRows)
    val result = new RowBasedSet(types, rows.slice(0, numRows), startOffset)
    rows.remove(0, numRows)
    startOffset += numRows
    return result
  }

  override def numColumns: Int = types.size

  override def numRows: Int = rows.size

  override def getStartOffset: Long = startOffset

  override def setStartOffset(startOffset: Long): Unit = this.startOffset = startOffset

}
