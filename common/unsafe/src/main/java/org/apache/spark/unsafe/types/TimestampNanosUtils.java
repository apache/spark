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

import java.util.Map;

import org.apache.spark.SparkIllegalArgumentException;

final class TimestampNanosUtils {
  static final int MAX_NANOS_WITHIN_MICRO = 999;

  private TimestampNanosUtils() {}

  static void validateNanosWithinMicro(short nanosWithinMicro) {
    if (nanosWithinMicro < 0 || nanosWithinMicro > MAX_NANOS_WITHIN_MICRO) {
      throw new SparkIllegalArgumentException(
        "INTERNAL_ERROR",
        Map.of(
          "message",
          "nanosWithinMicro must be in [0, " + MAX_NANOS_WITHIN_MICRO + "], got: "
            + nanosWithinMicro));
    }
  }
}
