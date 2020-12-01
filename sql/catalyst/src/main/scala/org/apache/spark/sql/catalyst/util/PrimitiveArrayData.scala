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
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

// scalastyle:off line.size.limit
object PrimitiveArrayData {
  def create(input: Any): ArrayData = input match {
    case a: Array[Boolean] => new BooleanArrayData(a)
    case a: Array[Byte] => new ByteArrayData(a)
    case a: Array[Short] => new ShortArrayData(a)
    case a: Array[Int] => new IntArrayData(a)
    case a: Array[Long] => new LongArrayData(a)
    case a: Array[Float] => new FloatArrayData(a)
    case a: Array[Double] => new DoubleArrayData(a)
    case _ => throw new IllegalArgumentException(s"PrimitiveArrayData does not support " +
        s"construction from ${input.getClass} instances.")
  }
}

sealed abstract class PrimitiveArrayData[T <: AnyVal](private val arr: Array[T])
  extends ArrayData {

  override final def numElements(): Int = arr.length

  override final def array: Array[Any] =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override final def setNullAt(i: Int): Unit =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def update(i: Int, value: Any): Unit = arr(i) = value.asInstanceOf[T]

  override def copy(): ArrayData = {
    val classConstructor = this.getClass.getDeclaredConstructors.head
    classConstructor.newInstance(arr.clone()).asInstanceOf[PrimitiveArrayData[T]]
  }

  override final def isNullAt(ordinal: Int): Boolean = false

  override def getBoolean(ordinal: Int): Boolean =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getByte(ordinal: Int): Byte =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getShort(ordinal: Int): Short =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getInt(ordinal: Int): Int =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getLong(ordinal: Int): Long =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getFloat(ordinal: Int): Float =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getDouble(ordinal: Int): Double =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getUTF8String(ordinal: Int): UTF8String =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getBinary(ordinal: Int): Array[Byte] =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getStruct(ordinal: Int, numFields: Int): InternalRow =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getArray(ordinal: Int): ArrayData =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def getMap(ordinal: Int): MapData =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def get(ordinal: Int, dataType: DataType): AnyRef =
    throw new UnsupportedOperationException(s"Unsupported for $getClass")

  override def hashCode(): Int = arr.toSeq.hashCode()

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[PrimitiveArrayData[_]]) {
      false
    } else {
      val other = obj.asInstanceOf[PrimitiveArrayData[_]]
      if (this eq other) {
        true
      } else if (this.getClass != other.getClass || this.arr.length != other.arr.length) {
        false
      } else {
        var i = 0
        while (i < arr.length) {
          if (this.arr(i) != other.arr(i)) {
            return false
          }
          i += 1
        }
        true
      }
    }
  }
}

private class LongArrayData(longArray: Array[Long]) extends PrimitiveArrayData[Long](longArray) {
  override def toLongArray(): Array[Long] = longArray.clone()
  override def getLong(ordinal: Int): Long = longArray(ordinal)
}

private class IntArrayData(intArray: Array[Int]) extends PrimitiveArrayData[Int](intArray) {
  override def toIntArray(): Array[Int] = intArray.clone()
  override def getInt(ordinal: Int): Int = intArray(ordinal)
}

private class ShortArrayData(shortArray: Array[Short]) extends PrimitiveArrayData[Short](shortArray) {
  override def toShortArray(): Array[Short] = shortArray.clone()
  override def getShort(ordinal: Int): Short = shortArray(ordinal)
}

private class ByteArrayData(byteArray: Array[Byte]) extends PrimitiveArrayData[Byte](byteArray) {
  override def toByteArray(): Array[Byte] = byteArray.clone()
  override def getByte(ordinal: Int): Byte = byteArray(ordinal)
}

private class BooleanArrayData(booleanArray: Array[Boolean]) extends PrimitiveArrayData[Boolean](booleanArray) {
  override def toBooleanArray(): Array[Boolean] = booleanArray.clone()
  override def getBoolean(ordinal: Int): Boolean = booleanArray(ordinal)
}

private class DoubleArrayData(doubleArray: Array[Double]) extends PrimitiveArrayData[Double](doubleArray) {
  override def toDoubleArray(): Array[Double] = doubleArray.clone()
  override def getDouble(ordinal: Int): Double = doubleArray(ordinal)
}

private class FloatArrayData(floatArray: Array[Float]) extends PrimitiveArrayData[Float](floatArray) {
  override def toFloatArray(): Array[Float] = floatArray.clone()
  override def getFloat(ordinal: Int): Float = floatArray(ordinal)
}
// scalastyle:on line.size.limit
