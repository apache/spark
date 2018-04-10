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

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, UnsafeArrayData}
import org.apache.spark.sql.types._

object ArrayData {
  def toArrayData(input: Any): ArrayData = input match {
    case a: Array[Boolean] => UnsafeArrayData.fromPrimitiveArray(a)
    case a: Array[Byte] => UnsafeArrayData.fromPrimitiveArray(a)
    case a: Array[Short] => UnsafeArrayData.fromPrimitiveArray(a)
    case a: Array[Int] => UnsafeArrayData.fromPrimitiveArray(a)
    case a: Array[Long] => UnsafeArrayData.fromPrimitiveArray(a)
    case a: Array[Float] => UnsafeArrayData.fromPrimitiveArray(a)
    case a: Array[Double] => UnsafeArrayData.fromPrimitiveArray(a)
    case other => new GenericArrayData(other)
  }
}

abstract class ArrayData extends SpecializedGetters with Serializable {
  def numElements(): Int

  def copy(): ArrayData

  def array: Array[Any]

  def toSeq[T](dataType: DataType): IndexedSeq[T] =
    new ArrayDataIndexedSeq[T](this, dataType)

  def setNullAt(i: Int): Unit

  def update(i: Int, value: Any): Unit

  // default implementation (slow)
  def setBoolean(i: Int, value: Boolean): Unit = update(i, value)
  def setByte(i: Int, value: Byte): Unit = update(i, value)
  def setShort(i: Int, value: Short): Unit = update(i, value)
  def setInt(i: Int, value: Int): Unit = update(i, value)
  def setLong(i: Int, value: Long): Unit = update(i, value)
  def setFloat(i: Int, value: Float): Unit = update(i, value)
  def setDouble(i: Int, value: Double): Unit = update(i, value)

  def toBooleanArray(): Array[Boolean] = {
    val size = numElements()
    val values = new Array[Boolean](size)
    var i = 0
    while (i < size) {
      values(i) = getBoolean(i)
      i += 1
    }
    values
  }

  def toByteArray(): Array[Byte] = {
    val size = numElements()
    val values = new Array[Byte](size)
    var i = 0
    while (i < size) {
      values(i) = getByte(i)
      i += 1
    }
    values
  }

  def toShortArray(): Array[Short] = {
    val size = numElements()
    val values = new Array[Short](size)
    var i = 0
    while (i < size) {
      values(i) = getShort(i)
      i += 1
    }
    values
  }

  def toIntArray(): Array[Int] = {
    val size = numElements()
    val values = new Array[Int](size)
    var i = 0
    while (i < size) {
      values(i) = getInt(i)
      i += 1
    }
    values
  }

  def toLongArray(): Array[Long] = {
    val size = numElements()
    val values = new Array[Long](size)
    var i = 0
    while (i < size) {
      values(i) = getLong(i)
      i += 1
    }
    values
  }

  def toFloatArray(): Array[Float] = {
    val size = numElements()
    val values = new Array[Float](size)
    var i = 0
    while (i < size) {
      values(i) = getFloat(i)
      i += 1
    }
    values
  }

  def toDoubleArray(): Array[Double] = {
    val size = numElements()
    val values = new Array[Double](size)
    var i = 0
    while (i < size) {
      values(i) = getDouble(i)
      i += 1
    }
    values
  }

  def toObjectArray(elementType: DataType): Array[AnyRef] =
    toArray[AnyRef](elementType: DataType)

  def toArray[T: ClassTag](elementType: DataType): Array[T] = {
    val size = numElements()
    val values = new Array[T](size)
    var i = 0
    while (i < size) {
      if (isNullAt(i)) {
        values(i) = null.asInstanceOf[T]
      } else {
        values(i) = get(i, elementType).asInstanceOf[T]
      }
      i += 1
    }
    values
  }

  // todo: specialize this.
  def foreach(elementType: DataType, f: (Int, Any) => Unit): Unit = {
    val size = numElements()
    var i = 0
    while (i < size) {
      if (isNullAt(i)) {
        f(i, null)
      } else {
        f(i, get(i, elementType))
      }
      i += 1
    }
  }
}

class ArrayDataIndexedSeq[T](arrayData: ArrayData, dataType: DataType) extends IndexedSeq[T] {

  private def getAccessor(dataType: DataType): (Int) => Any = dataType match {
    case BooleanType => (idx: Int) => arrayData.getBoolean(idx)
    case ByteType => (idx: Int) => arrayData.getByte(idx)
    case ShortType => (idx: Int) => arrayData.getShort(idx)
    case IntegerType => (idx: Int) => arrayData.getInt(idx)
    case LongType => (idx: Int) => arrayData.getLong(idx)
    case FloatType => (idx: Int) => arrayData.getFloat(idx)
    case DoubleType => (idx: Int) => arrayData.getDouble(idx)
    case d: DecimalType => (idx: Int) => arrayData.getDecimal(idx, d.precision, d.scale)
    case CalendarIntervalType => (idx: Int) => arrayData.getInterval(idx)
    case StringType => (idx: Int) => arrayData.getUTF8String(idx)
    case BinaryType => (idx: Int) => arrayData.getBinary(idx)
    case s: StructType => (idx: Int) => arrayData.getStruct(idx, s.length)
    case _: ArrayType => (idx: Int) => arrayData.getArray(idx)
    case _: MapType => (idx: Int) => arrayData.getMap(idx)
    case u: UserDefinedType[_] => getAccessor(u.sqlType)
    case _ => (idx: Int) => arrayData.get(idx, dataType)
  }

  private val accessor: (Int) => Any = getAccessor(dataType)

  override def apply(idx: Int): T = if (0 <= idx && idx < arrayData.numElements()) {
    if (arrayData.isNullAt(idx)) {
      null.asInstanceOf[T]
    } else {
      accessor(idx).asInstanceOf[T]
    }
  } else {
    throw new IndexOutOfBoundsException(
      s"Index $idx must be between 0 and the length of the ArrayData.")
  }

  override def length: Int = arrayData.numElements()
}
