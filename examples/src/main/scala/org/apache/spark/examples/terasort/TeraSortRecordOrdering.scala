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

package org.apache.spark.examples.terasort

/**
 * Comparator used for terasort. It compares the first ten bytes of the record by
 * first comparing the first 8 bytes, and then the last 2 bytes (using unsafe).
 */
class TeraSortRecordOrdering extends Ordering[Array[Byte]] {

  private[this] val UNSAFE: sun.misc.Unsafe = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get(null).asInstanceOf[sun.misc.Unsafe]
  }

  private[this] val BYTE_ARRAY_BASE_OFFSET: Long = UNSAFE.arrayBaseOffset(classOf[Array[Byte]])

  override def compare(x: Array[Byte], y: Array[Byte]): Int = {
    val leftWord = UNSAFE.getLong(x, BYTE_ARRAY_BASE_OFFSET)
    val rightWord = UNSAFE.getLong(y, BYTE_ARRAY_BASE_OFFSET)

    val diff = leftWord - rightWord
    if (diff != 0) {
      diff.toInt
    } else {
      UNSAFE.getChar(x, BYTE_ARRAY_BASE_OFFSET + 8) - UNSAFE.getChar(y, BYTE_ARRAY_BASE_OFFSET + 8)
    }
  }
}
