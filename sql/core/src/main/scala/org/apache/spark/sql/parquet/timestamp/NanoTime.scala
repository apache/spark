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

package org.apache.spark.sql.parquet.timestamp

import java.nio.{ByteBuffer, ByteOrder}

import parquet.Preconditions
import parquet.io.api.{Binary, RecordConsumer}

private[parquet] class NanoTime extends Serializable {
  private var julianDay = 0
  private var timeOfDayNanos = 0L

  def set(julianDay: Int, timeOfDayNanos: Long) = {
    this.julianDay = julianDay
    this.timeOfDayNanos = timeOfDayNanos
    this
  }

  def getJulianDay: Int = julianDay

  def getTimeOfDayNanos: Long = timeOfDayNanos

  def toBinary: Binary = {
    val buf = ByteBuffer.allocate(12)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(timeOfDayNanos)
    buf.putInt(julianDay)
    buf.flip()
    Binary.fromByteBuffer(buf)
  }

  def writeValue(recordConsumer: RecordConsumer) {
    recordConsumer.addBinary(toBinary)
  }

  override def toString =
    "NanoTime{julianDay=" + julianDay + ", timeOfDayNanos=" + timeOfDayNanos + "}"
}

object NanoTime {
  def fromBinary(bytes: Binary): NanoTime = {
    Preconditions.checkArgument(bytes.length() == 12, "Must be 12 bytes")
    val buf = bytes.toByteBuffer
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val timeOfDayNanos = buf.getLong
    val julianDay = buf.getInt
    new NanoTime().set(julianDay, timeOfDayNanos)
  }

  def apply(julianDay: Int, timeOfDayNanos: Long): NanoTime = {
    new NanoTime().set(julianDay, timeOfDayNanos)
  }
}
