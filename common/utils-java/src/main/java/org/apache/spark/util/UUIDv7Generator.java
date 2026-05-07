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
package org.apache.spark.util;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * Generator for UUIDv7 as defined in RFC 9562.
 * https://www.rfc-editor.org/rfc/rfc9562.html
 *
 * UUIDv7 is a time-ordered UUID that embeds a Unix timestamp in milliseconds.
 *
 * Monotonicity is best-effort but not strictly guaranteed by this implementation,
 * in rare cases such as concurrent generation within the same millisecond or clock adjustments.
 * This trade-off is intentional to avoid throughput degradation or thread contention.
 */
public final class UUIDv7Generator {

  private UUIDv7Generator() {
    // Prevent instantiation, as this is a util class.
  }

  /**
   * Holder class used for lazy initialization of SecureRandom.
   */
  private static class Holder {
    static final SecureRandom SECURE_RANDOM = new SecureRandom();
  }

  /**
   * Generate a UUIDv7 from the current time.
   *
   * The generated UUID embeds a 48-bit Unix timestamp (milliseconds since epoch),
   * followed by random bits for uniqueness. Monotonicity is best-effort: UUIDs
   * generated across different milliseconds will be ordered, but UUIDs within
   * the same millisecond may have random ordering.
   *
   * @return a new UUIDv7
   */
  public static UUID generate() {
    long timestamp = System.currentTimeMillis();

    // 48-bit timestamp | 4-bit version (0111) | 12-bit rand_a
    long msb = (timestamp << 16) | (0x7L << 12) | (Holder.SECURE_RANDOM.nextInt() & 0xFFF);

    long randB = Holder.SECURE_RANDOM.nextLong();
    // Set variant to IETF (0b10)
    long lsb = (randB & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;

    return new UUID(msb, lsb);
  }
}
