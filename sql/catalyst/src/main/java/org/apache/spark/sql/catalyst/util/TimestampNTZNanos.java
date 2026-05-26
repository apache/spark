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

package org.apache.spark.sql.catalyst.util;

import org.apache.spark.unsafe.types.TimestampNanosVal;

/**
 * Catalyst-internal server-side wrapper for {@code TIMESTAMP_NTZ(p)} nanosecond-capable values.
 * Analogous to {@link Geometry} for GEOMETRY: holds a {@link TimestampNanosVal} and is the place
 * for NTZ-specific conversion and arithmetic (for example {@code LocalDateTime}) as those APIs
 * are added.
 */
public final class TimestampNTZNanos {

  private final TimestampNanosVal value;

  private TimestampNTZNanos(TimestampNanosVal value) {
    this.value = value;
  }

  public static TimestampNTZNanos fromValue(TimestampNanosVal value) {
    return value == null ? null : new TimestampNTZNanos(value);
  }

  public static TimestampNTZNanos fromParts(long epochMicros, short nanosWithinMicro) {
    return fromValue(TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro));
  }

  public TimestampNanosVal getValue() {
    return value;
  }

  public long epochMicros() {
    return value.epochMicros;
  }

  public short nanosWithinMicro() {
    return value.nanosWithinMicro;
  }
}
