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

package org.apache.spark.sql.catalyst.analysis

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{ChangelogInfo, ChangelogRange}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ChangelogInfoUtilsSuite extends SparkFunSuite {

  private def makeOptions(kvs: (String, String)*): CaseInsensitiveStringMap = {
    new CaseInsensitiveStringMap(kvs.toMap.asJava)
  }

  test("version range with both start and end") {
    val info = ChangelogInfoUtils.fromOptions(
      makeOptions("startingVersion" -> "1", "endingVersion" -> "5"))
    val range = info.range().asInstanceOf[ChangelogRange.VersionRange]
    assert(range.startingVersion() == "1")
    assert(range.endingVersion().get() == "5")
    assert(range.startingBoundInclusive())
    assert(range.endingBoundInclusive())
  }

  test("version range with only start") {
    val info = ChangelogInfoUtils.fromOptions(
      makeOptions("startingVersion" -> "10"))
    val range = info.range().asInstanceOf[ChangelogRange.VersionRange]
    assert(range.startingVersion() == "10")
    assert(!range.endingVersion().isPresent)
  }

  test("version range - endingVersion without startingVersion throws") {
    checkError(
      intercept[AnalysisException] {
        ChangelogInfoUtils.fromOptions(makeOptions("endingVersion" -> "5"))
      },
      condition = "INVALID_CDC_OPTION.MISSING_STARTING_VERSION")
  }

  test("timestamp range with both start and end") {
    val info = ChangelogInfoUtils.fromOptions(
      makeOptions("startingTimestamp" -> "2026-01-01", "endingTimestamp" -> "2026-02-01"))
    val range = info.range().asInstanceOf[ChangelogRange.TimestampRange]
    assert(range.endingTimestamp().isPresent)
    assert(range.startingBoundInclusive())
    assert(range.endingBoundInclusive())
  }

  test("timestamp range with only start") {
    val info = ChangelogInfoUtils.fromOptions(
      makeOptions("startingTimestamp" -> "2026-01-01"))
    val range = info.range().asInstanceOf[ChangelogRange.TimestampRange]
    assert(!range.endingTimestamp().isPresent)
  }

  test("timestamp range - endingTimestamp without startingTimestamp throws") {
    checkError(
      intercept[AnalysisException] {
        ChangelogInfoUtils.fromOptions(makeOptions("endingTimestamp" -> "2026-02-01"))
      },
      condition = "INVALID_CDC_OPTION.MISSING_STARTING_TIMESTAMP")
  }

  test("cannot mix version and timestamp range") {
    checkError(
      intercept[AnalysisException] {
        ChangelogInfoUtils.fromOptions(
          makeOptions("startingVersion" -> "1", "startingTimestamp" -> "2026-01-01"))
      },
      condition = "INVALID_CDC_OPTION.CONFLICTING_RANGE_TYPES")
  }

  test("unbounded range when no version or timestamp specified") {
    val info = ChangelogInfoUtils.fromOptions(makeOptions())
    assert(info.range().isInstanceOf[ChangelogRange.Unbounded])
  }

  test("deduplication mode - none") {
    val info = ChangelogInfoUtils.fromOptions(
      makeOptions("deduplicationMode" -> "none"))
    assert(info.deduplicationMode() == ChangelogInfo.DeduplicationMode.NONE)
  }

  test("deduplication mode - dropCarryovers (default)") {
    val info = ChangelogInfoUtils.fromOptions(makeOptions())
    assert(info.deduplicationMode() == ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS)
  }

  test("deduplication mode - netChanges") {
    val info = ChangelogInfoUtils.fromOptions(
      makeOptions("deduplicationMode" -> "netChanges"))
    assert(info.deduplicationMode() == ChangelogInfo.DeduplicationMode.NET_CHANGES)
  }

  test("deduplication mode - case insensitive") {
    val info = ChangelogInfoUtils.fromOptions(
      makeOptions("deduplicationMode" -> "DROPCARRYOVERS"))
    assert(info.deduplicationMode() == ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS)
  }

  test("deduplication mode - invalid value throws") {
    checkError(
      intercept[AnalysisException] {
        ChangelogInfoUtils.fromOptions(
          makeOptions("deduplicationMode" -> "invalid"))
      },
      condition = "INVALID_CDC_OPTION.INVALID_DEDUPLICATION_MODE",
      parameters = Map("mode" -> "invalid"))
  }

  test("computeUpdates option") {
    val info = ChangelogInfoUtils.fromOptions(
      makeOptions("computeUpdates" -> "true"))
    assert(info.computeUpdates())
  }

  test("computeUpdates defaults to false") {
    val info = ChangelogInfoUtils.fromOptions(makeOptions())
    assert(!info.computeUpdates())
  }

  test("bound inclusivity options") {
    val info = ChangelogInfoUtils.fromOptions(
      makeOptions(
        "startingVersion" -> "1",
        "endingVersion" -> "5",
        "startingBoundInclusive" -> "false",
        "endingBoundInclusive" -> "false"))
    val range = info.range().asInstanceOf[ChangelogRange.VersionRange]
    assert(!range.startingBoundInclusive())
    assert(!range.endingBoundInclusive())
  }

  test("invalid timestamp throws") {
    checkError(
      intercept[AnalysisException] {
        ChangelogInfoUtils.fromOptions(
          makeOptions("startingTimestamp" -> "not-a-timestamp"))
      },
      condition = "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.OPTION",
      parameters = Map("expr" -> "'not-a-timestamp'"))
  }
}
