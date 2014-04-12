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

package org.apache.spark.util.io

import java.io.OutputStream

/**
 * A simple, fast byte-array output stream that exposes the backing array,
 * inspired by fastutil's FastByteArrayOutputStream.
 *
 * [[java.io.ByteArrayOutputStream]] is nice, but to get its content you
 * must generate each time a new object. This doesn't happen here.
 *
 * This class will automatically enlarge the backing array, doubling its
 * size whenever new space is needed.
 */
private[spark] class FastByteArrayOutputStream(initialCapacity: Int = 16) extends OutputStream {

  private[this] var _array = new Array[Byte](initialCapacity)

  /** The current writing position. */
  private[this] var _position: Int = 0

  /** The array backing the output stream. */
  def array: Array[Byte] = _array

  /** The number of valid bytes in array. */
  def length: Int = _position

  override def write(b: Int): Unit = {
    if (_position >= _array.length ) {
      _array = FastByteArrayOutputStream.growArray(_array, _position + 1, _position)
    }
    _array(_position) = b.toByte
    _position += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int) {
    if (off < 0) {
      throw new ArrayIndexOutOfBoundsException(s"Offset ($off) is negative" )
    }
    if (len < 0) {
      throw new IllegalArgumentException(s"Length ($len) is negative" )
    }
    if (off + len > b.length) {
      throw new ArrayIndexOutOfBoundsException(
        s"Last index (${off+len}) is greater than array length (${b.length})")
    }
    if ( _position + len > _array.length ) {
      _array = FastByteArrayOutputStream.growArray(_array, _position + len, _position)
    }
    System.arraycopy(b, off, _array, _position, len)
    _position += len
  }

  /** Ensures that the length of the backing array is equal to [[length]]. */
  def trim(): this.type = {
    if (_position < _array.length) {
      val newArr = new Array[Byte](_position)
      System.arraycopy(_array, 0, newArr, 0, _position)
      _array = newArr
    }
    this
  }
}

private object FastByteArrayOutputStream {
  /**
   * Grows the given array to the maximum between the given length and the current length
   * multiplied by two, provided that the given length is larger than the current length,
   * preserving just a part of the array.
   *
   * @param arr input array
   * @param len the new minimum length for this array
   * @param preserve the number of elements of the array that must be preserved
   *                 in case a new allocation is necessary
   */
  private def growArray(arr: Array[Byte], len: Int, preserve: Int): Array[Byte] = {
    if (len > arr.length) {
      val maxArraySize = Integer.MAX_VALUE - 8
      val newLen = math.min( math.max(2L * arr.length, len), maxArraySize).toInt
      val newArr = new Array[Byte](newLen)
      System.arraycopy(arr, 0, newArr, 0, preserve)
      newArr
    } else {
      arr
    }
  }
}