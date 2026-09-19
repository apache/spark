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

  test("SPARK-57812: toDouble/fromDouble distinguish nanosecond values sharing an epochMicros") {
    nanosTypes.foreach { dataType =>
      val low = TimestampNanosVal.fromParts(100L, 5.toShort)
      val high = TimestampNanosVal.fromParts(100L, 900.toShort)

      // Both values share the same epochMicros; only an epochMicros-only projection would
      // collapse them to the same Double, which previously broke ordering/tie-breaking
      // (e.g. IN-list min/max, join-key interval intersection) for CBO estimation.
      val lowAsDouble = EstimationUtils.toDouble(low, dataType)
      val highAsDouble = EstimationUtils.toDouble(high, dataType)
      assert(lowAsDouble < highAsDouble)

      assert(EstimationUtils.fromDouble(lowAsDouble, dataType) === low)
      assert(EstimationUtils.fromDouble(highAsDouble, dataType) === high)
    }
  }

  test("SPARK-57812: toDouble/fromDouble round-trip pre-1970 nanosecond timestamps") {
    nanosTypes.foreach { dataType =>
      val value = TimestampNanosVal.fromParts(-100L, 500.toShort)
      assert(
        EstimationUtils.fromDouble(EstimationUtils.toDouble(value, dataType), dataType) === value)
    }
  }
}
