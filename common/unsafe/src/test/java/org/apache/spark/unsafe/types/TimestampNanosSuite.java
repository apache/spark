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
}
