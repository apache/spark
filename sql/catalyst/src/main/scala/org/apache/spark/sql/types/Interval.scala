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

import java.nio.ByteBuffer

/**
 * The external representation of interval type.
 * Internally, we use 12 bytes to store values,
 * the first 4 bytes(a int) means the number of months,
 * the last 8 bytes(a long) means the number of microseconds.
 */
case class Interval(months: Int, microseconds: Long) {
  def toBinary: Array[Byte] =
    ByteBuffer.allocate(12).putInt(months).putLong(microseconds).array()
}

object Interval {
  def apply(binary: Array[Byte]): Interval = {
    assert(binary.length == 12)
    val b = ByteBuffer.wrap(binary)
    Interval(b.getInt(0), b.getLong(4))
  }
}
