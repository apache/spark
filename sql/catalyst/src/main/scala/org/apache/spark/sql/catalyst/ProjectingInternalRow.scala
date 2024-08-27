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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal, StructType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String, VariantVal}

/**
 * An [[InternalRow]] that projects particular columns from another [[InternalRow]] without copying
 * the underlying data.
 */
case class ProjectingInternalRow(schema: StructType,
                                 colOrdinals: IndexedSeq[Int]) extends InternalRow {
  assert(schema.size == colOrdinals.size)

  private var row: InternalRow = _

  override def numFields: Int = colOrdinals.size

  def project(row: InternalRow): Unit = {
    this.row = row
  }

  override def setNullAt(i: Int): Unit = throw SparkUnsupportedOperationException()

  override def update(i: Int, value: Any): Unit = throw SparkUnsupportedOperationException()

  override def copy(): InternalRow = {
    val newRow = if (row != null) row.copy() else null
    val newProjection = ProjectingInternalRow(schema, colOrdinals)
    newProjection.project(newRow)
    newProjection
  }

  override def isNullAt(ordinal: Int): Boolean = {
    row.isNullAt(colOrdinals(ordinal))
  }

  override def getBoolean(ordinal: Int): Boolean = {
    row.getBoolean(colOrdinals(ordinal))
  }

  override def getByte(ordinal: Int): Byte = {
    row.getByte(colOrdinals(ordinal))
  }

  override def getShort(ordinal: Int): Short = {
    row.getShort(colOrdinals(ordinal))
  }

  override def getInt(ordinal: Int): Int = {
    row.getInt(colOrdinals(ordinal))
  }

  override def getLong(ordinal: Int): Long = {
    row.getLong(colOrdinals(ordinal))
  }

  override def getFloat(ordinal: Int): Float = {
    row.getFloat(colOrdinals(ordinal))
  }

  override def getDouble(ordinal: Int): Double = {
    row.getDouble(colOrdinals(ordinal))
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    row.getDecimal(colOrdinals(ordinal), precision, scale)
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    row.getUTF8String(colOrdinals(ordinal))
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    row.getBinary(colOrdinals(ordinal))
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    row.getInterval(colOrdinals(ordinal))
  }

  override def getVariant(ordinal: Int): VariantVal = {
    row.getVariant(colOrdinals(ordinal))
  }

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    row.getStruct(colOrdinals(ordinal), numFields)
  }

  override def getArray(ordinal: Int): ArrayData = {
    row.getArray(colOrdinals(ordinal))
  }

  override def getMap(ordinal: Int): MapData = {
    row.getMap(colOrdinals(ordinal))
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    row.get(colOrdinals(ordinal), dataType)
  }
}

object ProjectingInternalRow {
  def apply(schema: StructType, colOrdinals: Seq[Int]): ProjectingInternalRow = {
    new ProjectingInternalRow(schema, colOrdinals.toIndexedSeq)
  }
}