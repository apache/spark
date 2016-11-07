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
import org.apache.spark.sql.types.DataType

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
