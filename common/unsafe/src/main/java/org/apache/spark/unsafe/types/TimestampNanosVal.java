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
import org.apache.spark.annotation.Unstable;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Physical representation for nanosecond-capable timestamp types ({@code TIMESTAMP_NTZ(p)} and
 * {@code TIMESTAMP_LTZ(p)} with {@code p} in [7, 9]). Analogous to {@link GeometryVal} for
 * GEOMETRY: this class is only a container for the composite value; NTZ vs LTZ semantics live in
 * {@link org.apache.spark.sql.catalyst.util.TimestampNTZNanos} and
 * {@link org.apache.spark.sql.catalyst.util.TimestampLTZNanos}.
 *
 * <p>Values are stored as two components:
 * <ul>
 *   <li>{@link #epochMicros} - microseconds since the Unix epoch (same unit as microsecond
 *   timestamp types),</li>
 *   <li>{@link #nanosWithinMicro} - additional nanoseconds within that microsecond, in [0, 999].
 *   </li>
 * </ul>
 *
 * <p>Logical row-size estimation uses 10 bytes (8 + 2). In {@code UnsafeRow}, values are stored in
 * the variable-length region using a 16-byte payload (see
 * {@link org.apache.spark.sql.catalyst.expressions.TimestampNanosRowValues}), the same pattern as
 * {@link CalendarInterval}.
 *
 * @since 4.3.0
 */
@Unstable
public final class TimestampNanosVal implements Serializable {
  /** Size of the {@code UnsafeRow} variable-length payload for this type (two 8-byte words). */
  public static final int SIZE_IN_BYTES = 16;

  /** Maximum valid value for {@link #nanosWithinMicro} (three sub-micro decimal digits). */
  public static final int MAX_NANOS_WITHIN_MICRO = 999;

  /** Microseconds since the Unix epoch. */
  public final long epochMicros;
  /** Nanoseconds within {@link #epochMicros}, in [0, 999]. */
  public final short nanosWithinMicro;

  /**
   * @param epochMicros microseconds since the Unix epoch
   * @param nanosWithinMicro nanoseconds within {@code epochMicros}, must be in [0, 999]
   */
  public TimestampNanosVal(long epochMicros, short nanosWithinMicro) {
    if (nanosWithinMicro < 0 || nanosWithinMicro > MAX_NANOS_WITHIN_MICRO) {
      throw new SparkIllegalArgumentException(
        "INTERNAL_ERROR",
        Map.of(
          "message",
          "nanosWithinMicro must be in [0, " + MAX_NANOS_WITHIN_MICRO + "], got: "
            + nanosWithinMicro));
    }
    this.epochMicros = epochMicros;
    this.nanosWithinMicro = nanosWithinMicro;
  }

  /**
   * Creates a non-null value from its components.
   */
  public static TimestampNanosVal fromParts(long epochMicros, short nanosWithinMicro) {
    return new TimestampNanosVal(epochMicros, nanosWithinMicro);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimestampNanosVal that = (TimestampNanosVal) o;
    return epochMicros == that.epochMicros && nanosWithinMicro == that.nanosWithinMicro;
  }

  @Override
  public int hashCode() {
    return Objects.hash(epochMicros, nanosWithinMicro);
  }

  @Override
  public String toString() {
    return "TimestampNanosVal(" + epochMicros + ", " + nanosWithinMicro + ")";
  }
}
