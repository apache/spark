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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, UnsafeArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods

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


  /**
   * Allocate [[UnsafeArrayData]] or [[GenericArrayData]] based on given parameters.
   *
   * @param elementSize a size of an element in bytes. If less than zero, the type of an element is
   *                    non-primitive type
   * @param numElements the number of elements the array should contain
   * @param additionalErrorMessage string to include in the error message
   */
  def allocateArrayData(
      elementSize: Int,
      numElements: Long,
      additionalErrorMessage: String): ArrayData = {
    if (elementSize >= 0 && !UnsafeArrayData.shouldUseGenericArrayData(elementSize, numElements)) {
      UnsafeArrayData.createFreshArray(numElements.toInt, elementSize)
    } else if (numElements <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH.toLong) {
      new GenericArrayData(new Array[Any](numElements.toInt))
    } else {
      throw new RuntimeException(s"Cannot create array with $numElements " +
        "elements of data due to exceeding the limit " +
        s"${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH} elements for ArrayData. " +
        additionalErrorMessage)
    }
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
    val accessor = InternalRow.getAccessor(elementType)
    val values = new Array[T](size)
    var i = 0
    while (i < size) {
      values(i) = accessor(this, i).asInstanceOf[T]
      i += 1
    }
    values
  }

  def foreach(elementType: DataType, f: (Int, Any) => Unit): Unit = {
    val size = numElements()
    val accessor = InternalRow.getAccessor(elementType)
    var i = 0
    while (i < size) {
      f(i, accessor(this, i))
      i += 1
    }
  }
}

/**
 * Implements an `IndexedSeq` interface for `ArrayData`. Notice that if the original `ArrayData`
 * is a primitive array and contains null elements, it is better to ask for `IndexedSeq[Any]`,
 * instead of `IndexedSeq[Int]`, in order to keep the null elements.
 */
class ArrayDataIndexedSeq[T](arrayData: ArrayData, dataType: DataType) extends IndexedSeq[T] {

  private val accessor: (SpecializedGetters, Int) => Any = InternalRow.getAccessor(dataType)

  override def apply(idx: Int): T =
    if (0 <= idx && idx < arrayData.numElements()) {
      accessor(arrayData, idx).asInstanceOf[T]
    } else {
      throw new IndexOutOfBoundsException(
        s"Index $idx must be between 0 and the length of the ArrayData.")
    }

  override def length: Int = arrayData.numElements()
}
