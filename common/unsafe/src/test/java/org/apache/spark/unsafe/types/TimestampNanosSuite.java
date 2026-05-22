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
  public void timestampNTZNanosEqualsAndHashCode() {
    TimestampNTZNanos t1 = new TimestampNTZNanos(1000L, (short) 100);
    TimestampNTZNanos t2 = new TimestampNTZNanos(1001L, (short) 100);
    TimestampNTZNanos t3 = new TimestampNTZNanos(1000L, (short) 101);
    TimestampNTZNanos t4 = new TimestampNTZNanos(1000L, (short) 100);

    assertNotEquals(t1, t2);
    assertNotEquals(t1, t3);
    assertEquals(t1, t4);
    assertEquals(t1.hashCode(), t4.hashCode());
  }

  @Test
  public void timestampLTZNanosEqualsAndHashCode() {
    TimestampLTZNanos t1 = new TimestampLTZNanos(2000L, (short) 0);
    TimestampLTZNanos t2 = new TimestampLTZNanos(2000L, (short) 1);
    TimestampLTZNanos t3 = new TimestampLTZNanos(2000L, (short) 0);

    assertNotEquals(t1, t2);
    assertEquals(t1, t3);
  }

  @Test
  public void invalidNanosWithinMicroNTZ() {
    assertThrows(SparkIllegalArgumentException.class, () -> new TimestampNTZNanos(0L, (short) -1));
    assertThrows(SparkIllegalArgumentException.class, () -> new TimestampNTZNanos(0L, (short) 1000));
  }

  @Test
  public void invalidNanosWithinMicroLTZ() {
    assertThrows(SparkIllegalArgumentException.class, () -> new TimestampLTZNanos(0L, (short) -1));
    assertThrows(SparkIllegalArgumentException.class, () -> new TimestampLTZNanos(0L, (short) 1000));
  }
}
