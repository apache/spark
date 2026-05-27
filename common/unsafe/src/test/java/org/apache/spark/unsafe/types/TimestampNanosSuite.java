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
}
