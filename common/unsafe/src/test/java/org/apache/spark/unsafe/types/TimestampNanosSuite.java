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

package org.apache.spark.unsafe.types;

import org.apache.spark.SparkIllegalArgumentException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class TimestampNanosSuite {

  @Test
  public void timestampNanosValEqualsAndHashCode() {
    TimestampNanosVal t1 = TimestampNanosVal.fromParts(1000L, (short) 100);
    TimestampNanosVal t2 = TimestampNanosVal.fromParts(1001L, (short) 100);
    TimestampNanosVal t3 = TimestampNanosVal.fromParts(1000L, (short) 101);
    TimestampNanosVal t4 = TimestampNanosVal.fromParts(1000L, (short) 100);

    assertNotEquals(t1, t2);
    assertNotEquals(t1, t3);
    assertEquals(t1, t4);
    assertEquals(t1.hashCode(), t4.hashCode());
  }

  @Test
  public void invalidNanosWithinMicro() {
    assertThrows(SparkIllegalArgumentException.class,
      () -> TimestampNanosVal.fromParts(0L, (short) -1));
    assertThrows(SparkIllegalArgumentException.class,
      () -> TimestampNanosVal.fromParts(0L, (short) 1000));
  }

  @Test
  public void boundaryNanosWithinMicro() {
    assertEquals((short) 0, TimestampNanosVal.fromParts(0L, (short) 0).nanosWithinMicro);
    assertEquals((short) TimestampNanosVal.MAX_NANOS_WITHIN_MICRO,
      TimestampNanosVal.fromParts(0L, (short) TimestampNanosVal.MAX_NANOS_WITHIN_MICRO)
        .nanosWithinMicro);
  }

  @Test
  public void fromTrustedRowBytesSkipsValidation() {
    // Documents the trust contract: callers on the row-read path may pass any short without
    // the [0, 999] check. The fields are stored as-given.
    TimestampNanosVal v = TimestampNanosVal.fromTrustedRowBytes(0L, (short) 1234);
    assertEquals((short) 1234, v.nanosWithinMicro);
  }

  @Test
  public void equalsHandlesNullAndOtherTypes() {
    TimestampNanosVal v = TimestampNanosVal.fromParts(0L, (short) 0);
    assertNotEquals(v, null);
    assertNotEquals(v, "not a timestamp");
  }

  @Test
  public void constants() {
    assertEquals(16, TimestampNanosVal.SIZE_IN_BYTES);
    assertEquals(999, TimestampNanosVal.MAX_NANOS_WITHIN_MICRO);
    assertEquals(0L, TimestampNanosVal.ZERO.epochMicros);
    assertEquals((short) 0, TimestampNanosVal.ZERO.nanosWithinMicro);
  }

  @Test
  public void compareToOrdersByEpochMicrosThenNanos() {
    TimestampNanosVal a = TimestampNanosVal.fromParts(1000L, (short) 100);
    TimestampNanosVal b = TimestampNanosVal.fromParts(1001L, (short) 0);
    TimestampNanosVal c = TimestampNanosVal.fromParts(1000L, (short) 101);
    TimestampNanosVal d = TimestampNanosVal.fromParts(1000L, (short) 100);

    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertTrue(a.compareTo(c) < 0);
    assertTrue(c.compareTo(a) > 0);
    assertEquals(0, a.compareTo(d));
  }

  @Test
  public void compareToIsConsistentWithEquals() {
    // The Comparable contract requires a.compareTo(b) == 0 iff a.equals(b). Without this,
    // TreeSet/TreeMap-backed Catalyst ops would silently dedup or lose values.
    TimestampNanosVal a = TimestampNanosVal.fromParts(-42L, (short) 7);
    TimestampNanosVal b = TimestampNanosVal.fromParts(-42L, (short) 7);
    assertEquals(a, b);
    assertEquals(0, a.compareTo(b));
    assertEquals(0, b.compareTo(a));
  }

  @Test
  public void compareToHandlesLongBoundaries() {
    // Plain (a.epochMicros - b.epochMicros) would overflow here; Long.compare must protect us.
    TimestampNanosVal min = TimestampNanosVal.fromParts(Long.MIN_VALUE, (short) 0);
    TimestampNanosVal minPlusNanos = TimestampNanosVal.fromParts(Long.MIN_VALUE, (short) 999);
    TimestampNanosVal max = TimestampNanosVal.fromParts(Long.MAX_VALUE, (short) 0);
    TimestampNanosVal maxMinusNanos = TimestampNanosVal.fromParts(Long.MAX_VALUE - 1, (short) 999);

    assertTrue(min.compareTo(max) < 0);
    assertTrue(max.compareTo(min) > 0);
    // Within the same epochMicros, the nanos tie-breaker decides.
    assertTrue(min.compareTo(minPlusNanos) < 0);
    // epochMicros wins even when the smaller epochMicros has larger nanos.
    assertTrue(maxMinusNanos.compareTo(max) < 0);
  }

  @Test
  public void compareToHandlesNegativeEpoch() {
    // Pre-epoch instants are valid (the SPIP keeps the 0001-9999 calendar range). Verify
    // a negative epochMicros sorts before a positive one regardless of the nanos field.
    TimestampNanosVal preEpoch = TimestampNanosVal.fromParts(-1L, (short) 999);
    TimestampNanosVal postEpoch = TimestampNanosVal.fromParts(0L, (short) 0);
    assertTrue(preEpoch.compareTo(postEpoch) < 0);
    assertTrue(postEpoch.compareTo(preEpoch) > 0);
  }

  @Test
  public void compareToIsAntisymmetricAndTransitive() {
    TimestampNanosVal a = TimestampNanosVal.fromParts(10L, (short) 1);
    TimestampNanosVal b = TimestampNanosVal.fromParts(10L, (short) 2);
    TimestampNanosVal c = TimestampNanosVal.fromParts(11L, (short) 0);

    // antisymmetry: sign(a.compareTo(b)) == -sign(b.compareTo(a))
    assertEquals(Integer.signum(a.compareTo(b)), -Integer.signum(b.compareTo(a)));
    assertEquals(Integer.signum(b.compareTo(c)), -Integer.signum(c.compareTo(b)));
    // transitivity: a < b and b < c implies a < c
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(c) < 0);
    assertTrue(a.compareTo(c) < 0);
  }

  @Test
  public void compareToThrowsOnNull() {
    // The Comparable javadoc requires NullPointerException when the argument is null.
    TimestampNanosVal v = TimestampNanosVal.fromParts(0L, (short) 0);
    assertThrows(NullPointerException.class, () -> v.compareTo(null));
  }

  @Test
  public void arraysSortUsesComparable() {
    TimestampNanosVal[] xs = new TimestampNanosVal[] {
      TimestampNanosVal.fromParts(5L, (short) 0),
      TimestampNanosVal.fromParts(-1L, (short) 999),
      TimestampNanosVal.fromParts(0L, (short) 0),
      TimestampNanosVal.fromParts(5L, (short) 999),
      TimestampNanosVal.fromParts(5L, (short) 1)
    };
    Arrays.sort(xs);
    assertArrayEquals(new TimestampNanosVal[] {
      TimestampNanosVal.fromParts(-1L, (short) 999),
      TimestampNanosVal.fromParts(0L, (short) 0),
      TimestampNanosVal.fromParts(5L, (short) 0),
      TimestampNanosVal.fromParts(5L, (short) 1),
      TimestampNanosVal.fromParts(5L, (short) 999)
    }, xs);
  }
}
