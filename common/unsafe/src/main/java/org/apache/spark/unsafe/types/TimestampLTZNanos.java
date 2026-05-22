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
 * Physical representation of {@code TIMESTAMP_LTZ(p)} with nanosecond-capable precision.
 * Values are stored as epoch microseconds plus nanoseconds within that microsecond (0-999).
 *
 * @since 4.2.0
 */
@Unstable
public final class TimestampLTZNanos implements Serializable {
  public static final int SIZE_IN_BYTES = 16;

  public final long epochMicros;
  public final short nanosWithinMicro;

  public TimestampLTZNanos(long epochMicros, short nanosWithinMicro) {
    TimestampNanosUtils.validateNanosWithinMicro(nanosWithinMicro);
    this.epochMicros = epochMicros;
    this.nanosWithinMicro = nanosWithinMicro;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimestampLTZNanos that = (TimestampLTZNanos) o;
    return epochMicros == that.epochMicros && nanosWithinMicro == that.nanosWithinMicro;
  }

  @Override
  public int hashCode() {
    return Objects.hash(epochMicros, nanosWithinMicro);
  }

  @Override
  public String toString() {
    return "TimestampLTZNanos(" + epochMicros + ", " + nanosWithinMicro + ")";
  }
}
