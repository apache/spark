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
package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_MILLIS

object SparkDateTimeUtils {
  /**
   * Converts the timestamp to milliseconds since epoch. In Spark timestamp values have microseconds
   * precision, so this conversion is lossy.
   */
  def microsToMillis(micros: Long): Long = {
    // When the timestamp is negative i.e before 1970, we need to adjust the milliseconds portion.
    // Example - 1965-01-01 10:11:12.123456 is represented as (-157700927876544) in micro precision.
    // In millis precision the above needs to be represented as (-157700927877).
    Math.floorDiv(micros, MICROS_PER_MILLIS)
  }

  /**
   * Converts milliseconds since the epoch to microseconds.
   */
  def millisToMicros(millis: Long): Long = {
    Math.multiplyExact(millis, MICROS_PER_MILLIS)
  }
}
