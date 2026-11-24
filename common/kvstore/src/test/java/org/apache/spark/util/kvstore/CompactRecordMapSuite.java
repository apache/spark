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

package org.apache.spark.util.kvstore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CompactRecordMapSuite {

  @Test
  public void primitiveKeysMatchHashMapAcrossUpdatesAndRemovals() {
    for (Class<?> type : new Class<?>[] {Integer.class, Long.class}) {
      CompactRecordMap<String> actual = CompactRecordMap.create(type);
      Map<Number, String> expected = new HashMap<>();
      Random random = new Random(8675309L);
      for (int i = 0; i < 20000; i++) {
        int candidate = random.nextInt(512) - 256;
        // Use separate branches: a numeric conditional expression would promote ints to longs.
        Number key;
        if (type == Integer.class) {
          key = candidate;
        } else {
          key = ((long) candidate << 32) | (candidate & 0xffffffffL);
        }
        if (random.nextBoolean()) {
          String value = "value" + i;
          assertEquals(expected.put(key, value), actual.put(key, value));
        } else {
          assertEquals(expected.remove(key), actual.remove(key));
        }
        assertEquals(expected.get(key), actual.get(key));
        assertEquals(expected.size(), actual.size());
        if (i % 127 == 0) {
          expected.forEach((k, v) -> assertEquals(v, actual.get(k)));
          Set<String> values = new HashSet<>();
          actual.values().forEach(values::add);
          assertEquals(new HashSet<>(expected.values()), values);
        }
      }
      for (Number key : expected.keySet()) {
        assertEquals(expected.get(key), actual.remove(key));
      }
      assertEquals(0, actual.size());
    }
  }

  @Test
  public void extremeKeysAndKeyTypes() {
    CompactRecordMap<String> longs = CompactRecordMap.create(Long.class);
    for (long key : new long[] {0, -1, Long.MIN_VALUE, Long.MAX_VALUE}) {
      longs.put(key, Long.toString(key));
      assertEquals(Long.toString(key), longs.get(key));
    }
    assertNull(longs.get(0));
    assertNull(longs.remove(0));
    assertEquals(4, longs.size());
    assertEquals("0", longs.remove(0L));
  }
}
