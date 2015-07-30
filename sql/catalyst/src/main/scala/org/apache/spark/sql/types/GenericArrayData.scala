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

package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.{UTF8String, CalendarInterval}

class GenericArrayData(array: Array[Any]) extends ArrayData {
  private def getAs[T](ordinal: Int) = get(ordinal).asInstanceOf[T]

  override def toArray(): Array[Any] = array

  override def get(ordinal: Int): Any = array(ordinal)

  override def isNullAt(ordinal: Int): Boolean = get(ordinal) == null

  override def getBoolean(ordinal: Int): Boolean = getAs(ordinal)

  override def getByte(ordinal: Int): Byte = getAs(ordinal)

  override def getShort(ordinal: Int): Short = getAs(ordinal)

  override def getInt(ordinal: Int): Int = getAs(ordinal)

  override def getLong(ordinal: Int): Long = getAs(ordinal)

  override def getFloat(ordinal: Int): Float = getAs(ordinal)

  override def getDouble(ordinal: Int): Double = getAs(ordinal)

  override def getDecimal(ordinal: Int): Decimal = getAs(ordinal)

  override def getUTF8String(ordinal: Int): UTF8String = getAs(ordinal)

  override def getBinary(ordinal: Int): Array[Byte] = getAs(ordinal)

  override def getInterval(ordinal: Int): CalendarInterval = getAs(ordinal)

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = getAs(ordinal)

  override def getArray(ordinal: Int): ArrayData = getAs(ordinal)

  override def numElements(): Int = array.length
}
