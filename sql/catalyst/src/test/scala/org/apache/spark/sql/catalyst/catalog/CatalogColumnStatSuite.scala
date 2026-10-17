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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.TimestampNanosTestUtils.{foreachNanosPrecision, nanoOfSecTruncator, nanosVal}
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType, TimestampNTZType, TimestampType}

class CatalogColumnStatSuite extends SparkFunSuite {

  test("SPARK-57812: nanosecond timestamp min/max round-trip through catalog stats") {
    // 1970-01-01 00:00:00.123456789 on the UTC grid.
    val epochMicros = 123456L
    val fullNanoOfSec = 123456789
    val value = nanosVal(epochMicros, 789)

    foreachNanosPrecision { precision =>
      // Sub-precision digits are truncated (floored) on both format and parse, matching the
      // truncation rule used by the underlying formatter.
      val truncatedNanoOfSec = nanoOfSecTruncator(precision)(fullNanoOfSec)
      val expected = nanosVal(epochMicros, truncatedNanoOfSec % 1000)
      val expectedString = f"1970-01-01 00:00:00.$truncatedNanoOfSec%09d"

      Seq[DataType](
        TimestampLTZNanosType(precision),
        TimestampNTZNanosType(precision)).foreach { dataType =>
        val external = CatalogColumnStat.toExternalString(value, "c", dataType)
        assert(external === expectedString)
        assert(
          CatalogColumnStat.fromExternalString(
            external, "c", dataType, CatalogColumnStat.VERSION) === expected)
      }
    }
  }

  test("SPARK-57812: microsecond timestamp stats format is unchanged") {
    // 1970-01-01 00:00:00.123456 UTC, the format ANALYZE TABLE has always persisted.
    assert(CatalogColumnStat.toExternalString(123456L, "c", TimestampType) ===
      "1970-01-01 00:00:00.123456")
    assert(CatalogColumnStat.toExternalString(123456L, "c", TimestampNTZType) ===
      "1970-01-01 00:00:00.123456")
    assert(CatalogColumnStat.fromExternalString(
      "1970-01-01 00:00:00.123456", "c", TimestampType, CatalogColumnStat.VERSION) === 123456L)
    assert(CatalogColumnStat.fromExternalString(
      "1970-01-01 00:00:00.123456", "c", TimestampNTZType, CatalogColumnStat.VERSION) === 123456L)
  }
}
