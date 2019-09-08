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

import java.nio.ByteBuffer
import java.util.BitSet

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.service.cli.thrift._
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.thriftserver.utils.SparkSQLUtils
import org.apache.spark.sql.types.{BinaryType, _}

case class ColumnBasedSet(types: StructType,
                          rows: ArrayBuffer[Row],
                          initStartOffset: Long)
  extends RowSet {

  var startOffset: Long = initStartOffset

  import ColumnBasedSet._

  override def toTRowSet: TRowSet = {
    val tRowSet = new TRowSet(startOffset, Seq[TRow]().asJava)
    if (rows != null) {
      (0 until types.length).map(i => {
        toTColumn(i, types(i).dataType)
      }
      ).foreach(tRowSet.addToColumns)
    }
    tRowSet
  }

  private[this] def toTColumn(ordinal: Int, typ: DataType): TColumn = {
    val nulls = new BitSet()
    typ match {
      case BooleanType =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) true else rows(i).getBoolean(ordinal)
        }.map(_.asInstanceOf[java.lang.Boolean]).asJava
        TColumn.boolVal(new TBoolColumn(values, bitSetToBuffer(nulls)))
      case ByteType =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) 0.toByte else rows(i).getByte(ordinal)
        }.map(_.asInstanceOf[java.lang.Byte]).asJava
        TColumn.byteVal(new TByteColumn(values, bitSetToBuffer(nulls)))
      case ShortType =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) 0.toShort else rows(i).getShort(ordinal)
        }.map(_.asInstanceOf[java.lang.Short]).asJava
        TColumn.i16Val(new TI16Column(values, bitSetToBuffer(nulls)))
      case IntegerType =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) 0 else rows(i).getInt(ordinal)
        }.map(_.asInstanceOf[java.lang.Integer]).asJava
        TColumn.i32Val(new TI32Column(values, bitSetToBuffer(nulls)))
      case LongType =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) 0 else rows(i).getLong(ordinal)
        }.map(_.asInstanceOf[java.lang.Long]).asJava
        TColumn.i64Val(new TI64Column(values, bitSetToBuffer(nulls)))
      case FloatType =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) 0 else rows(i).getFloat(ordinal)
        }.map(_.toDouble.asInstanceOf[java.lang.Double]).asJava
        TColumn.doubleVal(new TDoubleColumn(values, bitSetToBuffer(nulls)))
      case DoubleType =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) 0 else rows(i).getDouble(ordinal)
        }.map(_.asInstanceOf[java.lang.Double]).asJava
        TColumn.doubleVal(new TDoubleColumn(values, bitSetToBuffer(nulls)))
      case StringType =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) EMPTY_STRING else rows(i).getString(ordinal)
        }.asJava
        TColumn.stringVal(new TStringColumn(values, bitSetToBuffer(nulls)))
      case BinaryType =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) {
            EMPTY_BINARY
          } else {
            ByteBuffer.wrap(rows(i).getAs[Array[Byte]](ordinal))
          }
        }.asJava
        TColumn.binaryVal(new TBinaryColumn(values, bitSetToBuffer(nulls)))
      case _ =>
        val values = rows.indices.map { i =>
          nulls.set(i, rows(i).isNullAt(ordinal))
          if (rows(i).isNullAt(ordinal)) {
            EMPTY_STRING
          } else {
            SparkSQLUtils.toHiveString((rows(i).get(ordinal), typ))
          }
        }.asJava
        TColumn.stringVal(new TStringColumn(values, bitSetToBuffer(nulls)))
    }
  }

  private[this] def bitSetToBuffer(bitSet: BitSet): ByteBuffer = ByteBuffer.wrap(bitSet.toByteArray)

  override def addRow(row: Row): RowSet = {
    rows.+=(row)
    this
  }

  override def extractSubset(maxRows: Int): RowSet = {
    val numRows = Math.min(rows.size, maxRows)
    val result = new ColumnBasedSet(types, rows.slice(0, numRows), startOffset)
    rows.remove(0, numRows)
    startOffset += numRows
    return result
  }

  override def numColumns: Int = types.size

  override def numRows: Int = rows.size

  override def getStartOffset: Long = startOffset

  override def setStartOffset(startOffset: Long): Unit = this.startOffset = startOffset

}

object ColumnBasedSet {
  private val EMPTY_STRING = ""
  private val EMPTY_BINARY = ByteBuffer.allocate(0)
}
