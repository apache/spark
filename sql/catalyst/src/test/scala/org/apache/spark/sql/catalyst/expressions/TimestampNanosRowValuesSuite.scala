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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.Platform

/**
 * Direct, byte-level tests for [[TimestampNanosRowValues]]. The roundtrip tests in
 * [[TimestampNanosRowSuite]] write and read through the same helper, so a consistent
 * encoder/decoder bug (swapped word order, wrong offset arithmetic) would pass them.
 * These tests pin the layout (word 0 = epoch micros, word 1 = nanos in the low 16 bits with the
 * upper 48 bits zero) against a raw byte buffer.
 */
class TimestampNanosRowValuesSuite extends SparkFunSuite {

  private val BASE = Platform.BYTE_ARRAY_OFFSET

  test("writePayload places epochMicros in word 0 and nanos in low 16 bits of word 1") {
    val buf = new Array[Byte](16)
    TimestampNanosRowValues.writePayload(buf, BASE, 0, 0x0123456789ABCDEFL, 42.toShort)
    assert(Platform.getLong(buf, BASE) === 0x0123456789ABCDEFL)
    // The whole 8-byte word at offset 8 equals nanos: low 16 bits = 42, upper 48 bits = 0.
    assert(Platform.getLong(buf, BASE + 8) === 42L)
  }

  test("readEpochMicros and readNanosWithinMicro decompose the payload") {
    val buf = new Array[Byte](16)
    TimestampNanosRowValues.writePayload(buf, BASE, 0, -1L, 999.toShort)
    assert(TimestampNanosRowValues.readEpochMicros(buf, BASE, 0) === -1L)
    assert(TimestampNanosRowValues.readNanosWithinMicro(buf, BASE, 0) === 999.toShort)
  }

  test("readVal reconstructs the TimestampNanosVal") {
    val buf = new Array[Byte](16)
    TimestampNanosRowValues.writePayload(buf, BASE, 0, 1234567890123L, 500.toShort)
    val v = TimestampNanosRowValues.readVal(buf, BASE, 0)
    assert(v.epochMicros === 1234567890123L)
    assert(v.nanosWithinMicro === 500.toShort)
  }

  test("zeroPayload clears both words") {
    val buf = new Array[Byte](16)
    TimestampNanosRowValues.writePayload(buf, BASE, 0, -1L, 42.toShort)
    TimestampNanosRowValues.zeroPayload(buf, BASE, 0)
    assert(Platform.getLong(buf, BASE) === 0L)
    assert(Platform.getLong(buf, BASE + 8) === 0L)
  }

  test("writePayload accepts boundary nanos values 0 and 999") {
    val buf = new Array[Byte](16)
    TimestampNanosRowValues.writePayload(buf, BASE, 0, 0L, 0.toShort)
    assert(TimestampNanosRowValues.readNanosWithinMicro(buf, BASE, 0) === 0.toShort)
    TimestampNanosRowValues.writePayload(buf, BASE, 0, 0L, 999.toShort)
    assert(TimestampNanosRowValues.readNanosWithinMicro(buf, BASE, 0) === 999.toShort)
  }

  test("writePayload preserves Long.MinValue and Long.MaxValue epoch micros") {
    val buf = new Array[Byte](16)
    TimestampNanosRowValues.writePayload(buf, BASE, 0, Long.MaxValue, 999.toShort)
    assert(TimestampNanosRowValues.readEpochMicros(buf, BASE, 0) === Long.MaxValue)
    assert(TimestampNanosRowValues.readNanosWithinMicro(buf, BASE, 0) === 999.toShort)

    TimestampNanosRowValues.writePayload(buf, BASE, 0, Long.MinValue, 0.toShort)
    assert(TimestampNanosRowValues.readEpochMicros(buf, BASE, 0) === Long.MinValue)
    assert(TimestampNanosRowValues.readNanosWithinMicro(buf, BASE, 0) === 0.toShort)
  }

  test("writePayload honours the cursor offset") {
    // Two payloads back-to-back; reads at the corresponding cursors must not see each other.
    val buf = new Array[Byte](32)
    TimestampNanosRowValues.writePayload(buf, BASE, 0, 111L, 1.toShort)
    TimestampNanosRowValues.writePayload(buf, BASE, 16, 222L, 2.toShort)
    assert(TimestampNanosRowValues.readEpochMicros(buf, BASE, 0) === 111L)
    assert(TimestampNanosRowValues.readNanosWithinMicro(buf, BASE, 0) === 1.toShort)
    assert(TimestampNanosRowValues.readEpochMicros(buf, BASE, 16) === 222L)
    assert(TimestampNanosRowValues.readNanosWithinMicro(buf, BASE, 16) === 2.toShort)
  }
}
