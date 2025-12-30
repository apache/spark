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

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generator for UUIDv7 as defined in RFC 9562.
 * https://www.rfc-editor.org/rfc/rfc9562.html
 *
 * UUIDv7 is a time-ordered UUID that embeds a Unix timestamp in milliseconds.
 *
 * The generate() method guarantees monotonicity by tracking the last used timestamp
 * and increments if necessary when the clock hasn't advanced.
 */
public final class UUIDv7Generator {

    /**
     * Tracks the last timestamp used to ensure monotonicity.
     * If the system clock returns the same or earlier time, we increment from this value.
     */
    private static final AtomicLong lastTimestamp = new AtomicLong(0);

    private UUIDv7Generator() {
        // Prevent instantiation, as this is a util class.
    }

    /**
     * Generate a UUIDv7.
     *
     * This method ensures each generated UUID has a timestamp greater than or equal to
     * the previous one, incrementing when the clock hasn't advanced from lastTimestamp.
     *
     * @return a new UUIDv7
     */
    public static UUID generate() {
        long timestamp = lastTimestamp.updateAndGet(last -> {
            long now = System.currentTimeMillis();
            return now > last ? now : last + 1;
        });

        // 48-bit timestamp | 4-bit version (0111) | 12-bit rand_a
        long msb = (timestamp << 16) | (0x7L << 12) | (ThreadLocalRandom.current().nextInt() & 0xFFF);

        long randB = ThreadLocalRandom.current().nextLong();
        // Set variant to IETF (0b10)
        long lsb = (randB & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;

        return new UUID(msb, lsb);
    }
}
