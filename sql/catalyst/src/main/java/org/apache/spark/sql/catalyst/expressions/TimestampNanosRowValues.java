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
import org.apache.spark.unsafe.types.TimestampNanosVal;

/**
 * Shared read/write helpers for nanosecond timestamp values in {@link UnsafeRow} and
 * {@link UnsafeArrayData}.
 *
 * <p>Each value occupies a 16-byte payload in the row's variable-length region (the field's 8-byte
 * word stores {@code (offset << 32) | size}, like
 * {@link org.apache.spark.unsafe.types.CalendarInterval}).
 * The logical composite is 10 bytes (8-byte epoch micros + 2-byte sub-micro nanos), but UnsafeRow
 * stores it as two aligned 8-byte words:
 * <pre>
 *   word 0: epochMicros (long)
 *   word 1: nanosWithinMicro in the low 16 bits; upper 48 bits are zero
 * </pre>
 *
 * <p>The second word is written and read as a full {@code long} (not {@code short}) so the upper
 * 48 padding bits are deterministically zero. {@link UnsafeRow} compares and hashes rows
 * byte-wise, so any non-deterministic padding would make logically-equal values look different.
 * Byte order is not a concern because writes and reads both go through {@link Platform} and
 * therefore share the same native endianness.
 */
public final class TimestampNanosRowValues {
  /** Payload size in the UnsafeRow variable-length region (two 8-byte words). */
  public static final int SIZE_IN_BYTES = TimestampNanosVal.SIZE_IN_BYTES;

  private TimestampNanosRowValues() {
  }

  /**
   * Writes a non-null nanosecond timestamp payload at {@code baseOffset + cursor}.
   */
  public static void writePayload(
      Object baseObject, long baseOffset, int cursor, long epochMicros, short nanosWithinMicro) {
    assert nanosWithinMicro >= 0 && nanosWithinMicro <= TimestampNanosVal.MAX_NANOS_WITHIN_MICRO :
      "nanosWithinMicro out of range: " + nanosWithinMicro;
    Platform.putLong(baseObject, baseOffset + cursor, epochMicros);
    // Validated to [0, 999], so widening to long zero-extends; upper 48 bits become zero.
    Platform.putLong(baseObject, baseOffset + cursor + 8, nanosWithinMicro);
  }

  /**
   * Zeroes a payload slot. Used when updating a nullable column to null while retaining the
   * variable-length offset for in-place updates (same pattern as {@link UnsafeRow#setInterval}).
   */
  public static void zeroPayload(Object baseObject, long baseOffset, int cursor) {
    Platform.putLong(baseObject, baseOffset + cursor, 0L);
    Platform.putLong(baseObject, baseOffset + cursor + 8, 0L);
  }

  public static long readEpochMicros(Object baseObject, long baseOffset, int offset) {
    return Platform.getLong(baseObject, baseOffset + offset);
  }

  public static short readNanosWithinMicro(Object baseObject, long baseOffset, int offset) {
    // Match writePayload's putLong; the (short) cast truncates to the low 16 bits.
    return (short) Platform.getLong(baseObject, baseOffset + offset + 8);
  }

  public static TimestampNanosVal readVal(Object baseObject, long baseOffset, int offset) {
    // Use the trusted factory: every value that ever reached the row was validated at its
    // origin (the public constructor / fromParts), so re-checking the nanos range on every
    // cell read would be wasted work on a hot path.
    return TimestampNanosVal.fromTrustedRowBytes(
        readEpochMicros(baseObject, baseOffset, offset),
        readNanosWithinMicro(baseObject, baseOffset, offset));
  }
}
