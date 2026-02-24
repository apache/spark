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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UUIDv7GeneratorSuite {

  private long extractTimestamp(UUID uuid) {
    long msb = uuid.getMostSignificantBits();
    return (msb >>> 16) & 0xFFFFFFFFFFFFL;
  }

  @Test
  void testCorrectFormat() {
    UUID uuid = UUIDv7Generator.generate();
    String[] parts = uuid.toString().split("-");

    // number of parts should be 5 with lengths 8-4-4-4-12
    assertEquals(5, parts.length);
    assertEquals(8, parts[0].length());
    assertEquals(4, parts[1].length());
    assertEquals(4, parts[2].length());
    assertEquals(4, parts[3].length());
    assertEquals(12, parts[4].length());

    // Version should be 7
    int version = (int) ((uuid.getMostSignificantBits() >>> 12) & 0xF);
    assertEquals(0x7, version);

    // Variant should be 2 (0b10)
    int variant = (int) ((uuid.getLeastSignificantBits() >>> 62) & 0x3);
    assertEquals(0x2, variant);
  }

  @Test
  void testUniqueValues() {
    Set<UUID> uuids = new HashSet<>();
    for (int i = 0; i < 10000; i++) {
      uuids.add(UUIDv7Generator.generate());
    }
    assertEquals(10000, uuids.size());
  }

  @Test
  void testTimestampAccuracy() {
    long before = System.currentTimeMillis();
    UUID uuid = UUIDv7Generator.generate();
    long after = System.currentTimeMillis();
    long uuidTs = extractTimestamp(uuid);

    assertTrue(uuidTs >= before && uuidTs <= after);
  }
}
