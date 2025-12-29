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
package org.apache.spark.util

import java.time.Instant
import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class UUIDv7GeneratorSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  private def convertTimestamp(uuid: UUID): Long = {
    val msb = uuid.getMostSignificantBits
    val timeLow = (msb >>> 32) & 0xFFFFFFFFL
    val timeMid = (msb >>> 16) & 0xFFFFL
    val timeHiAndVersion = msb & 0xFFFFL
    val timestamp48 = (timeLow << 16) | timeMid
    timestamp48
  }

  test("UUIDv7 correct format") {
    val uuid = UUIDv7Generator.generate()
    val parts = uuid.toString.split("-")

    // number of bits should be 8-4-4-4-12
    assert(parts.length == 5)
    assert(parts(0).length == 8 && parts(1).length == 4 && parts(2).length == 4 &&
      parts(3).length == 4 && parts(4).length == 12)

    val version = (uuid.getMostSignificantBits >>> 12) & 0xF
    assert(version == 0x7)

    val variant = (uuid.getLeastSignificantBits >>> 62) & 0x3
    assert(variant == 0x2)
  }

  test("UUIDv7 unique values") {
    val uuids = (1 to 10000).map(_ => UUIDv7Generator.generate())
    val unique = uuids.distinct
    assert(uuids.size == unique.size)
  }

  test("UUIDv7 monotonicity") {
    val uuids = (1 to 1000).map(_ => UUIDv7Generator.generate())

    val timestamps = uuids.map(convertTimestamp)
    val sorted = timestamps.sorted
    assert(timestamps == sorted)
  }

  test("UUIDv7 timestamp accuracy") {
    val now = Instant.now().toEpochMilli
    val uuid = UUIDv7Generator.generate()
    val uuidTs = convertTimestamp(uuid)
    val delta = math.abs(uuidTs - now)

    assert(delta < 10) // allow 10 ms tolerance
  }

  test("UUIDv7 uniqueness at same timestamp") {
    val epochMilli = 1717171717171L
    val nano = 555555555

    val uuids = (1 to 1000).map(_ => UUIDv7Generator.generateFrom(epochMilli, nano))
    val distinctCount = uuids.distinct.size

    assert(distinctCount == 1000)
  }

  test("UUIDv7 generateFrom correct timestamp encoding") {
    val epochMilli = 1717171717171L
    val nano = 987654321
    val uuid = UUIDv7Generator.generateFrom(epochMilli, nano)

    val msb = uuid.getMostSignificantBits
    val encodedTimestamp = (msb >>> 16) & 0xFFFFFFFFFFFFL // use 48-bit mask
    assert(encodedTimestamp == (epochMilli & 0xFFFFFFFFFFFFL))
  }

  test("UUIDv7 orderable by ascending timestamp") {
    val t1 = UUIDv7Generator.generateFrom(1717171717171L, 100000000)
    val t2 = UUIDv7Generator.generateFrom(1717171717172L, 100000000)
    val t3 = UUIDv7Generator.generateFrom(1717171717173L, 100000000)

    val ordered = List(t1, t2, t3).sorted
    assert(ordered == List(t1, t2, t3))
  }
}

