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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, Decimal, StringType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class FailureSafeParser[IN](
    func: IN => Seq[InternalRow],
    mode: String,
    corruptFieldIndex: Option[Int]) {

  private val toResultRow: (InternalRow, () => UTF8String) => InternalRow = {
    if (corruptFieldIndex.isDefined) {
      val resultRow = new RowWithBadRecord(null, corruptFieldIndex.get, null)
      (row, badRecord) => {
        resultRow.row = row
        resultRow.record = badRecord()
        resultRow
      }
    } else {
      (row, badRecord) => row
    }
  }

  def parse(input: IN): Seq[InternalRow] = {
    try {
      func(input).map(toResultRow(_, () => null))
    } catch {
      case e: BadRecordException if ParseModes.isPermissiveMode(mode) =>
        Seq(toResultRow(e.partialResult(), e.record))
      case _: BadRecordException if ParseModes.isDropMalformedMode(mode) =>
        Nil
      // If the parse mode is FAIL FAST, do not catch the exception.
    }
  }
}

case class BadRecordException(
    record: () => UTF8String,
    partialResult: () => InternalRow,
    cause: Throwable) extends Exception(cause)

class RowWithBadRecord(var row: InternalRow, index: Int, var record: UTF8String)
  extends InternalRow {

  override def numFields: Int = row.numFields + 1

  override def setNullAt(ordinal: Int): Unit = {
    if (ordinal < index) {
      row.setNullAt(ordinal)
    } else if (ordinal == index) {
      record = null
    } else {
      row.setNullAt(ordinal - 1)
    }
  }

  override def update(i: Int, value: Any): Unit = {
    throw new UnsupportedOperationException("update")
  }

  override def copy(): InternalRow = new RowWithBadRecord(row.copy(), index, record)

  override def anyNull: Boolean = row.anyNull || record == null

  override def isNullAt(ordinal: Int): Boolean = {
    if (ordinal < index) {
      row.isNullAt(ordinal)
    } else if (ordinal == index) {
      record == null
    } else {
      row.isNullAt(ordinal - 1)
    }
  }

  private def fail() = {
    throw new IllegalAccessError("This is a string field.")
  }

  override def getBoolean(ordinal: Int): Boolean = {
    if (ordinal < index) {
      row.getBoolean(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getBoolean(ordinal - 1)
    }
  }

  override def getByte(ordinal: Int): Byte = {
    if (ordinal < index) {
      row.getByte(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getByte(ordinal - 1)
    }
  }

  override def getShort(ordinal: Int): Short = {
    if (ordinal < index) {
      row.getShort(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getShort(ordinal - 1)
    }
  }

  override def getInt(ordinal: Int): Int = {
    if (ordinal < index) {
      row.getInt(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getInt(ordinal - 1)
    }
  }

  override def getLong(ordinal: Int): Long = {
    if (ordinal < index) {
      row.getLong(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getLong(ordinal - 1)
    }
  }

  override def getFloat(ordinal: Int): Float = {
    if (ordinal < index) {
      row.getFloat(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getFloat(ordinal - 1)
    }
  }

  override def getDouble(ordinal: Int): Double = {
    if (ordinal < index) {
      row.getDouble(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getDouble(ordinal - 1)
    }
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    if (ordinal < index) {
      row.getDecimal(ordinal, precision, scale)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getDecimal(ordinal - 1, precision, scale)
    }
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    if (ordinal < index) {
      row.getUTF8String(ordinal)
    } else if (ordinal == index) {
      record
    } else {
      row.getUTF8String(ordinal - 1)
    }
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    if (ordinal < index) {
      row.getBinary(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getBinary(ordinal - 1)
    }
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    if (ordinal < index) {
      row.getInterval(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getInterval(ordinal - 1)
    }
  }

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    if (ordinal < index) {
      row.getStruct(ordinal, numFields)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getStruct(ordinal - 1, numFields)
    }
  }

  override def getArray(ordinal: Int): ArrayData = {
    if (ordinal < index) {
      row.getArray(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getArray(ordinal - 1)
    }
  }

  override def getMap(ordinal: Int): MapData = {
    if (ordinal < index) {
      row.getMap(ordinal)
    } else if (ordinal == index) {
      fail()
    } else {
      row.getMap(ordinal - 1)
    }
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    if (ordinal < index) {
      row.get(ordinal, dataType)
    } else if (ordinal == index) {
      if (dataType == StringType) {
        record
      } else {
        fail()
      }
    } else {
      row.get(ordinal - 1, dataType)
    }
  }
}
