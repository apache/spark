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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.TimestampLTZNanos;
import org.apache.spark.unsafe.types.TimestampNTZNanos;

/**
 * Shared read/write helpers for nanosecond timestamp values in UnsafeRow/UnsafeArrayData.
 */
public final class TimestampNanosRowValues {
  public static final int SIZE_IN_BYTES = TimestampNTZNanos.SIZE_IN_BYTES;

  private TimestampNanosRowValues() {
  }

  public static void writePayload(
      Object baseObject, long baseOffset, int cursor, long epochMicros, short nanosWithinMicro) {
    Platform.putLong(baseObject, baseOffset + cursor, epochMicros);
    // Store nanos in the low 16 bits; upper 48 bits remain zero.
    Platform.putLong(baseObject, baseOffset + cursor + 8, ((long) nanosWithinMicro) & 0xFFFFL);
  }

  public static void zeroPayload(Object baseObject, long baseOffset, int cursor) {
    Platform.putLong(baseObject, baseOffset + cursor, 0L);
    Platform.putLong(baseObject, baseOffset + cursor + 8, 0L);
  }

  public static long readEpochMicros(Object baseObject, long baseOffset, int offset) {
    return Platform.getLong(baseObject, baseOffset + offset);
  }

  public static short readNanosWithinMicro(Object baseObject, long baseOffset, int offset) {
    return Platform.getShort(baseObject, baseOffset + offset + 8);
  }

  public static TimestampNTZNanos readNTZ(Object baseObject, long baseOffset, int offset) {
    return new TimestampNTZNanos(
        readEpochMicros(baseObject, baseOffset, offset),
        readNanosWithinMicro(baseObject, baseOffset, offset));
  }

  public static TimestampLTZNanos readLTZ(Object baseObject, long baseOffset, int offset) {
    return new TimestampLTZNanos(
        readEpochMicros(baseObject, baseOffset, offset),
        readNanosWithinMicro(baseObject, baseOffset, offset));
  }
}
