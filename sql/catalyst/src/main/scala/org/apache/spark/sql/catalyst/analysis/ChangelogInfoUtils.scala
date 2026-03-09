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

import java.util.Optional

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{ChangelogInfo, ChangelogRange}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Utility methods for constructing [[ChangelogInfo]] from DataFrame API options.
 */
object ChangelogInfoUtils {

  /**
   * Build a [[ChangelogInfo]] from the options specified via `.option()` calls on
   * `DataFrameReader` or `DataStreamReader`.
   */
  def fromOptions(options: CaseInsensitiveStringMap): ChangelogInfo = {
    val startVersion = Option(options.get("startingVersion"))
    val endVersion = Option(options.get("endingVersion"))
    val startTimestamp = Option(options.get("startingTimestamp"))
    val endTimestamp = Option(options.get("endingTimestamp"))

    val startInclusive = options.getBoolean("startingBoundInclusive", true)
    val endInclusive = options.getBoolean("endingBoundInclusive", true)

    val deduplicationModeStr = Option(options.get("deduplicationMode"))
      .getOrElse("dropCarryovers").toLowerCase(java.util.Locale.ROOT)
    val deduplicationMode = deduplicationModeStr match {
      case "none" => ChangelogInfo.DeduplicationMode.NONE
      case "dropcarryovers" => ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS
      case "netchanges" => ChangelogInfo.DeduplicationMode.NET_CHANGES
      case other =>
        throw new AnalysisException(
          "INVALID_CDC_OPTION.INVALID_DEDUPLICATION_MODE",
          Map("mode" -> other))
    }
    val computeUpdates = options.getBoolean("computeUpdates", false)

    // Determine range from options
    val hasVersionRange = startVersion.isDefined || endVersion.isDefined
    val hasTimestampRange = startTimestamp.isDefined || endTimestamp.isDefined

    if (hasVersionRange && hasTimestampRange) {
      throw new AnalysisException(
        "INVALID_CDC_OPTION.CONFLICTING_RANGE_TYPES",
        Map.empty[String, String])
    }

    val range = if (hasVersionRange) {
      val sv = startVersion.getOrElse(
        throw new AnalysisException(
          "INVALID_CDC_OPTION.MISSING_STARTING_VERSION",
          Map.empty[String, String]))
      new ChangelogRange.VersionRange(
        sv,
        endVersion.map(Optional.of[String]).getOrElse(Optional.empty[String]),
        startInclusive,
        endInclusive)
    } else if (hasTimestampRange) {
      val startTsValue = startTimestamp.map(parseTimestamp).getOrElse(
        throw new AnalysisException(
          "INVALID_CDC_OPTION.MISSING_STARTING_TIMESTAMP",
          Map.empty[String, String]))
      val endTsValue = endTimestamp.map(ts => java.lang.Long.valueOf(parseTimestamp(ts)))
      new ChangelogRange.TimestampRange(
        startTsValue,
        endTsValue.map(Optional.of[java.lang.Long]).getOrElse(Optional.empty[java.lang.Long]),
        startInclusive,
        endInclusive)
    } else {
      // No range specified — unbounded (streaming use case)
      new ChangelogRange.Unbounded()
    }

    new ChangelogInfo(range, deduplicationMode, computeUpdates)
  }

  private def parseTimestamp(timestampStr: String): Long = {
    val value = Cast(
      Literal(timestampStr),
      TimestampType,
      Some(DateTimeUtils.getZoneId("UTC").toString),
      ansiEnabled = false
    ).eval()
    if (value == null) {
      throw new AnalysisException(
        "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.OPTION",
        Map("expr" -> s"'$timestampStr'"))
    }
    value.asInstanceOf[Long]
  }
}
