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

import java.util.Arrays

import org.apache.spark.unsafe.PlatformDependent.BYTE_ARRAY_OFFSET
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.string.UTF8StringMethods

/**
 *  A UTF-8 String, as internal representation of StringType in SparkSQL
 *
 *  A String encoded in UTF-8 as an Array[Byte], which can be used for comparison,
 *  search, see http://en.wikipedia.org/wiki/UTF-8 for details.
 *
 *  Note: This is not designed for general use cases, should not be used outside SQL.
 */

final class UTF8String extends Ordered[UTF8String] with Serializable {

  private[this] var bytes: Array[Byte] = _

  /**
   * Update the UTF8String with String.
   */
  def set(str: String): UTF8String = {
    set(str.getBytes("utf-8"))
  }

  /**
   * Update the UTF8String with Array[Byte], which should be encoded in UTF-8
   */
  def set(bytes: Array[Byte]): UTF8String = {
    this.bytes = bytes
    this
  }

  /**
   * Return the number of code points in it.
   *
   * This is only used by Substring() when `start` is negative.
   */
  def length(): Int = {
    UTF8StringMethods.getLengthInCodePoints(bytes, BYTE_ARRAY_OFFSET, bytes.length)
  }

  def getBytes: Array[Byte] = {
    bytes
  }

  /**
   * Return a substring of this,
   * @param start the position of first code point
   * @param until the position after last code point
   */
  def slice(start: Int, until: Int): UTF8String = {
    if (until <= start || start >= bytes.length || bytes == null) {
      new UTF8String
    }

    var c = 0
    var i: Int = 0
    while (c < start && i < bytes.length) {
      i += UTF8StringMethods.numOfBytes(bytes(i))
      c += 1
    }
    var j = i
    while (c < until && j < bytes.length) {
      j += UTF8StringMethods.numOfBytes(bytes(j))
      c += 1
    }
    UTF8String(Arrays.copyOfRange(bytes, i, j))
  }

  def contains(sub: UTF8String): Boolean = {
    val b = sub.getBytes
    if (b.length == 0) {
      return true
    }
    var i: Int = 0
    while (i <= bytes.length - b.length) {
      // In worst case, it's O(N*K), but should works fine with SQL
      if (bytes(i) == b(0) && Arrays.equals(Arrays.copyOfRange(bytes, i, i + b.length), b)) {
        return true
      }
      i += 1
    }
    false
  }

  def startsWith(prefix: UTF8String): Boolean = {
    val prefixBytes = prefix.getBytes
    UTF8StringMethods.startsWith(
      bytes,
      BYTE_ARRAY_OFFSET,
      bytes.length,
      prefixBytes,
      BYTE_ARRAY_OFFSET,
      prefixBytes.length
    )
  }

  def endsWith(suffix: UTF8String): Boolean = {
    val suffixBytes = suffix.getBytes
    UTF8StringMethods.endsWith(
      bytes,
      BYTE_ARRAY_OFFSET,
      bytes.length,
      suffixBytes,
      BYTE_ARRAY_OFFSET,
      suffixBytes.length
    )
  }

  def toUpperCase(): UTF8String = {
    // upper case depends on locale, fallback to String.
    UTF8String(toString().toUpperCase)
  }

  def toLowerCase(): UTF8String = {
    // lower case depends on locale, fallback to String.
    UTF8String(toString().toLowerCase)
  }

  override def toString(): String = {
    new String(bytes, "utf-8")
  }

  override def clone(): UTF8String = new UTF8String().set(this.bytes)

  override def compare(other: UTF8String): Int = {
    val otherBytes = other.getBytes
    UTF8StringMethods.compare(
      bytes,
      BYTE_ARRAY_OFFSET,
      bytes.length,
      otherBytes,
      BYTE_ARRAY_OFFSET,
      otherBytes.length
    )
  }

  override def compareTo(other: UTF8String): Int = {
    compare(other)
  }

  override def equals(other: Any): Boolean = other match {
    case s: UTF8String =>
      val otherBytes = s.getBytes
      otherBytes.length == bytes.length && ByteArrayMethods.arrayEquals(
        bytes,
        BYTE_ARRAY_OFFSET,
        otherBytes,
        BYTE_ARRAY_OFFSET,
        otherBytes.length
      )
    case s: String =>
      // This is only used for Catalyst unit tests
      // fail fast
      bytes.length >= s.length && length() == s.length && toString() == s
    case _ =>
      false
  }

  override def hashCode(): Int = {
    Arrays.hashCode(bytes)
  }
}

object UTF8String {

  /**
   * Create a UTF-8 String from String
   */
  def apply(s: String): UTF8String = {
    if (s != null) {
      new UTF8String().set(s)
    } else{
      null
    }
  }

  /**
   * Create a UTF-8 String from Array[Byte], which should be encoded in UTF-8
   */
  def apply(bytes: Array[Byte]): UTF8String = {
    if (bytes != null) {
      new UTF8String().set(bytes)
    } else {
      null
    }
  }
}
