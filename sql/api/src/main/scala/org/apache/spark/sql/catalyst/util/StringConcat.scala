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

import org.apache.spark.unsafe.array.ByteArrayUtils

/**
 * Concatenation of sequence of strings to final string with cheap append method and one memory
 * allocation for the final string. Can also bound the final size of the string.
 */
class StringConcat(val maxLength: Int = ByteArrayUtils.MAX_ROUNDED_ARRAY_LENGTH) {
  protected val strings = new java.util.ArrayList[String]
  protected var length: Int = 0

  def atLimit: Boolean = length >= maxLength

  /**
   * Appends a string and accumulates its length to allocate a string buffer for all appended
   * strings once in the toString method. Returns true if the string still has room for further
   * appends before it hits its max limit.
   */
  def append(s: String): Unit = {
    if (s != null) {
      val sLen = s.length
      if (!atLimit) {
        val available = maxLength - length
        val stringToAppend = if (available >= sLen) s else s.substring(0, available)
        strings.add(stringToAppend)
      }

      // Keeps the total length of appended strings. Note that we need to cap the length at
      // `ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH`; otherwise, we will overflow
      // length causing StringIndexOutOfBoundsException in the substring call above.
      length = Math.min(length.toLong + sLen, ByteArrayUtils.MAX_ROUNDED_ARRAY_LENGTH).toInt
    }
  }

  /**
   * The method allocates memory for all appended strings, writes them to the memory and returns
   * concatenated string.
   */
  override def toString: String = {
    val finalLength = if (atLimit) maxLength else length
    val result = new java.lang.StringBuilder(finalLength)
    strings.forEach(s => result.append(s))
    result.toString
  }
}

