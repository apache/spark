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

import org.apache.spark.annotation.Unstable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Physical representation of {@code TIMESTAMP_NTZ(p)} with nanosecond-capable fractional precision
 * ({@code p} in [7, 9]). Analogous to microsecond {@code TimestampNTZType}, which stores a single
 * {@code long} epoch micros value, but with sub-microsecond resolution.
 *
 * <p>Values are stored as two components:
 * <ul>
 *   <li>{@link #epochMicros} - microseconds since the Unix epoch in the proleptic Gregorian
 *   calendar (same unit as {@code TimestampNTZType}),</li>
 *   <li>{@link #nanosWithinMicro} - additional nanoseconds within that microsecond, in [0, 999].
 *   The SQL fractional-second precision {@code p} applies when converting to and from external
 *   representations; the physical value always retains up to three sub-micro digits.</li>
 * </ul>
 *
 * <p>Logical row-size estimation uses 10 bytes (8 + 2). In {@code UnsafeRow}, values are stored in
 * the variable-length region using a 16-byte payload (see
 * {@link org.apache.spark.sql.catalyst.expressions.TimestampNanosRowValues}), the same pattern as
 * {@link CalendarInterval}.
 *
 * <p>NTZ and LTZ nanosecond timestamps share this composite layout at the row layer; time-zone
 * semantics are enforced in the SQL and conversion layers, not in this class.
 *
 * @since 4.2.0
 */
@Unstable
public final class TimestampNTZNanos implements Serializable {
  /** Size of the {@code UnsafeRow} variable-length payload for this type (two 8-byte words). */
  public static final int SIZE_IN_BYTES = 16;

  /** Microseconds since the Unix epoch. */
  public final long epochMicros;
  /** Nanoseconds within {@link #epochMicros}, in [0, 999]. */
  public final short nanosWithinMicro;

  /**
   * @param epochMicros microseconds since the Unix epoch
   * @param nanosWithinMicro nanoseconds within {@code epochMicros}, must be in [0, 999]
   */
  public TimestampNTZNanos(long epochMicros, short nanosWithinMicro) {
    TimestampNanosUtils.validateNanosWithinMicro(nanosWithinMicro);
    this.epochMicros = epochMicros;
    this.nanosWithinMicro = nanosWithinMicro;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimestampNTZNanos that = (TimestampNTZNanos) o;
    return epochMicros == that.epochMicros && nanosWithinMicro == that.nanosWithinMicro;
  }

  @Override
  public int hashCode() {
    return Objects.hash(epochMicros, nanosWithinMicro);
  }

  @Override
  public String toString() {
    return "TimestampNTZNanos(" + epochMicros + ", " + nanosWithinMicro + ")";
  }
}
