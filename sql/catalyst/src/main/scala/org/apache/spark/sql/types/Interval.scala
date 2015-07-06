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
 * The internal representation of interval type.
 * Using 12 bytes to store values, the first 4 bytes(a int) means the number of months,
 * the last 8 bytes(a long) means the number of microseconds.
 */
final class Interval(private[this] val value: Array[Byte])
  extends Ordered[Interval] with Serializable {
  assert(value.length == 12)

  def numberOfMonth: Int = ByteBuffer.wrap(value).getInt(0)

  def numberOfMicrosecond: Long = ByteBuffer.wrap(value).getLong(4)

  override def compare(that: Interval): Int = {
    if (this.numberOfMonth > that.numberOfMonth) {
      1
    } else if (this.numberOfMonth < that.numberOfMonth) {
      -1
    } else if (this.numberOfMicrosecond > that.numberOfMicrosecond) {
      1
    } else if (this.numberOfMicrosecond < that.numberOfMicrosecond) {
      -1
    } else {
      0
    }
  }

  override def equals(other: Any): Boolean = other match {
    case d: Interval =>
      this.numberOfMonth == d.numberOfMonth && this.numberOfMicrosecond == d.numberOfMicrosecond
    case _ =>
      false
  }

  override def toString: String =
    s"interval($numberOfMonth months, $numberOfMicrosecond microseconds)"
}

object Interval {
  def apply(months: Int, microseconds: Long): Interval = {
    new Interval(ByteBuffer.allocate(12).putInt(months).putLong(microseconds).array())
  }
}
