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

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generator for UUIDv7 as defined in
 * https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format#section-5.2
 *
 * UUIDv7 is a time-ordered UUID that embeds a Unix timestamp in milliseconds,
 * making it suitable for use as database keys and for cases where temporal
 * ordering is beneficial.
 */
public final class UUIDv7Generator {

    private UUIDv7Generator() {
        // Prevent instantiation
    }

    /**
     * Generate a UUIDv7 from the current time.
     *
     * @return a new UUIDv7
     */
    public static UUID generate() {
        Instant now = Instant.now();
        return generateFrom(now.toEpochMilli(), now.getNano());
    }

    /**
     * Generate a UUIDv7 from the given timestamp components.
     * This method is deterministic for the timestamp portion but includes
     * random bits for uniqueness.
     *
     * @param epochMilli the Unix timestamp in milliseconds
     * @param nano the nanosecond component (used for sub-millisecond ordering)
     * @return a new UUIDv7
     */
    public static UUID generateFrom(long epochMilli, int nano) {
        // 48 bits for timestamp
        long timestampMs = epochMilli & 0xFFFFFFFFFFFFL;

        // 12 bits from nano, avoid LSB as most HW clocks have resolution in range of 10-40 ns
        int randA = (nano >> 4) & 0xFFF;

        // Version 7 uses bits 12:15
        long msb = (timestampMs << 16) | (0x7L << 12) | randA;

        long randB = ThreadLocalRandom.current().nextLong();

        // variant 0b10
        long randBWithVariant = (randB & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;

        return new UUID(msb, randBWithVariant);
    }
}

