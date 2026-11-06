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

package org.apache.spark.sql.catalyst.statsEstimation

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

class EstimationUtilsSuite extends SparkFunSuite {

  private val nanosTypes: Seq[DataType] =
    Seq(TimestampLTZNanosType(9), TimestampNTZNanosType(9))

  // 2022-01-01 00:00:00 UTC, in epoch microseconds: a magnitude representative of real-world
  // data, unlike the epoch-adjacent values used below, where a Double happens to have more
  // spare precision than this conversion actually promises.
  private val realisticEpochMicros = 1640995200000000L

  test("SPARK-57812: toDouble/fromDouble round-trip nanosecond timestamps at microsecond " +
    "resolution") {
    nanosTypes.foreach { dataType =>
      val value = TimestampNanosVal.fromParts(100L, 5.toShort)
      val truncated = TimestampNanosVal.fromParts(100L, 0.toShort)
      val roundTripped = EstimationUtils.fromDouble(EstimationUtils.toDouble(value, dataType),
        dataType)
      assert(roundTripped === truncated)
    }
  }

  test("SPARK-57812: toDouble collapses distinct nanosecond values sharing an epochMicros, " +
    "at realistic timestamp magnitudes") {
    // This is the resolution the conversion actually provides -- see the comment on
    // EstimationUtils.toDouble's AnyTimestampNanoType case. A Double's 52-bit mantissa cannot
    // also hold a distinguishable sub-microsecond fraction once epochMicros grows past 2^43
    // (~1970-04-12), which covers every realistic (i.e. non-epoch-adjacent) timestamp, so two
    // values sharing an epochMicros are indistinguishable to CBO estimation by design.
    nanosTypes.foreach { dataType =>
      val low = TimestampNanosVal.fromParts(realisticEpochMicros, 1.toShort)
      val high = TimestampNanosVal.fromParts(realisticEpochMicros, 999.toShort)
      assert(EstimationUtils.toDouble(low, dataType) === EstimationUtils.toDouble(high, dataType))

      val truncated = TimestampNanosVal.fromParts(realisticEpochMicros, 0.toShort)
      val roundTripped = EstimationUtils.fromDouble(EstimationUtils.toDouble(low, dataType),
        dataType)
      assert(roundTripped === truncated)
    }
  }

  test("SPARK-57812: toDouble is monotone across distinct epochMicros at realistic magnitudes") {
    // Ordering across microseconds -- rather than precision within one -- is what CBO
    // range/IN-list estimation relies on, and this holds regardless of the resolution
    // limitation documented above.
    nanosTypes.foreach { dataType =>
      val earlier = TimestampNanosVal.fromParts(realisticEpochMicros, 999.toShort)
      val later = TimestampNanosVal.fromParts(realisticEpochMicros + 1, 0.toShort)
      val earlierAsDouble = EstimationUtils.toDouble(earlier, dataType)
      val laterAsDouble = EstimationUtils.toDouble(later, dataType)
      assert(earlierAsDouble < laterAsDouble)
    }
  }

  test("SPARK-57812: toDouble/fromDouble round-trip pre-1970 nanosecond timestamps at " +
    "microsecond resolution") {
    nanosTypes.foreach { dataType =>
      val value = TimestampNanosVal.fromParts(-100L, 500.toShort)
      val truncated = TimestampNanosVal.fromParts(-100L, 0.toShort)
      val roundTripped = EstimationUtils.fromDouble(EstimationUtils.toDouble(value, dataType),
        dataType)
      assert(roundTripped === truncated)
    }
  }
}
