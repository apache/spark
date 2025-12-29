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
import java.util.concurrent.ThreadLocalRandom

object UUIDv7Generator {

  /**
   * Follow the UUIDv7 specification defined in
   * https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format#section-5.2
   */

  /**
   * Generate a UUIDv7 from the current time.
   */
  def generate(): UUID = {
    val now = Instant.now()
    generateFrom(now.toEpochMilli, now.getNano)
  }

  /**
   * Deterministic UUIDv7 generation from epochMilli and nanos.
   * Called by generate() and used for testing.
   */
  def generateFrom(epochMilli: Long, nano: Int): UUID = {
    // 48 bits for timestamp
    val timestampMs = epochMilli & 0xFFFFFFFFFFFFL

    // 12 bits, avoid LSB as most HW clocks have resolution in range of 10-40 ns
    val randA = (nano >> 4) & 0xFFF

    // Version 7 uses bits 12:15
    val msb = (timestampMs << 16) | (0x7 << 12) | randA

    val randB = ThreadLocalRandom.current().nextLong()

    // variant 0b10
    val randBWithVariant = (randB & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L

    new UUID(msb, randBWithVariant)
  }
}

