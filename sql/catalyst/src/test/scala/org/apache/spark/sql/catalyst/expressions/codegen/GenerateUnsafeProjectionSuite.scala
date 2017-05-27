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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal, StringType, StructType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class GenerateUnsafeProjectionSuite extends SparkFunSuite {
  test("Test unsafe projection string access pattern") {
    val dataType = (new StructType).add("a", StringType)
    val exprs = BoundReference(0, dataType, nullable = true) :: Nil
    val projection = GenerateUnsafeProjection.generate(exprs)
    val result = projection.apply(InternalRow(AlwaysNull))
    assert(!result.isNullAt(0))
    assert(result.getStruct(0, 1).isNullAt(0))
  }
}

object AlwaysNull extends InternalRow {
  override def numFields: Int = 1
  override def setNullAt(i: Int): Unit = {}
  override def copy(): InternalRow = this
  override def anyNull: Boolean = true
  override def isNullAt(ordinal: Int): Boolean = true
  override def update(i: Int, value: Any): Unit = notSupported
  override def getBoolean(ordinal: Int): Boolean = notSupported
  override def getByte(ordinal: Int): Byte = notSupported
  override def getShort(ordinal: Int): Short = notSupported
  override def getInt(ordinal: Int): Int = notSupported
  override def getLong(ordinal: Int): Long = notSupported
  override def getFloat(ordinal: Int): Float = notSupported
  override def getDouble(ordinal: Int): Double = notSupported
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = notSupported
  override def getUTF8String(ordinal: Int): UTF8String = notSupported
  override def getBinary(ordinal: Int): Array[Byte] = notSupported
  override def getInterval(ordinal: Int): CalendarInterval = notSupported
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = notSupported
  override def getArray(ordinal: Int): ArrayData = notSupported
  override def getMap(ordinal: Int): MapData = notSupported
  override def get(ordinal: Int, dataType: DataType): AnyRef = notSupported
  private def notSupported: Nothing = throw new UnsupportedOperationException
}
