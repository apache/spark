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

/**
 *  A UTF-8 String used only in SparkSQL
 */

private[sql] final class UTF8String extends Ordered[UTF8String] with Serializable {
  private var bytes: Array[Byte] = _

  def set(str: String): UTF8String = {
    bytes = str.getBytes("utf-8")
    this
  }

  def set(bytes: Array[Byte]): UTF8String = {
    this.bytes = bytes.clone()
    this
  }

  def length(): Int = {
    var len = 0
    var i: Int = 0
    while (i < bytes.length) {
      val b = bytes(i)
      i += 1
      if (b >= 196) {
        i += UTF8String.bytesFromUTF8(b - 196)
      }
      len += 1
    }
    len
  }

  def getBytes: Array[Byte] = {
    bytes
  }

  def slice(start: Int, end: Int): UTF8String = {
    if (end <= start || start >= bytes.length || bytes == null) {
      new UTF8String
    }

    var c = 0
    var i: Int = 0
    while (c < start && i < bytes.length) {
      val b = bytes(i)
      i += 1
      if (b >= 196) {
        i += UTF8String.bytesFromUTF8(b - 196)
      }
      c += 1
    }
    val bstart = i
    while (c < end && i < bytes.length) {
      val b = bytes(i)
      i += 1
      if (b >= 196) {
        i += UTF8String.bytesFromUTF8(b - 196)
      }
      c += 1
    }
    UTF8String(java.util.Arrays.copyOfRange(bytes, bstart, i))
  }

  def contains(sub: UTF8String): Boolean = {
    bytes.containsSlice(sub.bytes)
  }

  def startsWith(prefix: UTF8String): Boolean = {
    bytes.startsWith(prefix.bytes)
  }

  def endsWith(suffix: UTF8String): Boolean = {
    bytes.endsWith(suffix.bytes)
  }

  def toUpperCase(): UTF8String = {
    UTF8String(toString().toUpperCase)
  }

  def toLowerCase(): UTF8String = {
    UTF8String(toString().toLowerCase)
  }

  override def toString(): String = {
    new String(bytes, "utf-8")
  }

  override def clone(): UTF8String = new UTF8String().set(this.bytes)

  override def compare(other: UTF8String): Int = {
    var i: Int = 0
    while (i < bytes.length && i < other.bytes.length) {
      val res = bytes(i).compareTo(other.bytes(i))
      if (res != 0) return res
      i += 1
    }
    bytes.length - other.bytes.length
  }

  override def compareTo(other: UTF8String): Int = {
    compare(other)
  }

  override def equals(other: Any): Boolean = other match {
    case s: UTF8String =>
      java.util.Arrays.equals(bytes, s.bytes)
    case s: String =>
      bytes.length >= s.length && length() == s.length && toString() == s
    case _ =>
      false
  }

  override def hashCode(): Int = {
    java.util.Arrays.hashCode(bytes)
  }
}

private[sql] object UTF8String {
  // number of tailing bytes in a UTF8 sequence for a code point
  private[types] val bytesFromUTF8: Array[Int] = Array(1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3,
    3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5)

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