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

import java.lang.{Long => JLong}
import java.util.{Locale, Optional => JOptional}

import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.connector.catalog.ChangelogInfo
import org.apache.spark.sql.connector.catalog.ChangelogRange.{TimestampRange, UnboundedRange, VersionRange}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Utility methods for constructing [[ChangelogInfo]] from DataFrame API options.
 */
object ChangelogInfoUtils {

  private val STARTING_VERSION = "startingVersion"
  private val ENDING_VERSION = "endingVersion"
  private val STARTING_TIMESTAMP = "startingTimestamp"
  private val ENDING_TIMESTAMP = "endingTimestamp"
  private val STARTING_BOUND_INCLUSIVE = "startingBoundInclusive"
  private val ENDING_BOUND_INCLUSIVE = "endingBoundInclusive"
  private val DEDUPLICATION_MODE = "deduplicationMode"
  private val COMPUTE_UPDATES = "computeUpdates"

  /**
   * Build a [[ChangelogInfo]] from the options specified via `.option()` calls on
   * `DataFrameReader` or `DataStreamReader`.
   */
  def fromOptions(
      options: CaseInsensitiveStringMap,
      sessionLocalTimeZone: String): ChangelogInfo = {
    val startVersion = Option(options.get(STARTING_VERSION))
    val endVersion = Option(options.get(ENDING_VERSION))
    val startTimestamp = Option(options.get(STARTING_TIMESTAMP))
    val endTimestamp = Option(options.get(ENDING_TIMESTAMP))

    val startInclusive = options.getBoolean(STARTING_BOUND_INCLUSIVE, true)
    val endInclusive = options.getBoolean(ENDING_BOUND_INCLUSIVE, true)

    val deduplicationModeStr = Option(options.get(DEDUPLICATION_MODE))
      .getOrElse("dropCarryovers").toLowerCase(Locale.ROOT)
    val deduplicationMode = deduplicationModeStr match {
      case "none" => ChangelogInfo.DeduplicationMode.NONE
      case "dropcarryovers" => ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS
      case "netchanges" => ChangelogInfo.DeduplicationMode.NET_CHANGES
      case other =>
        throw QueryCompilationErrors.invalidCdcOptionInvalidDeduplicationMode(other)
    }
    val computeUpdates = options.getBoolean(COMPUTE_UPDATES, false)

    // Determine range from options
    val hasVersionRange = startVersion.isDefined || endVersion.isDefined
    val hasTimestampRange = startTimestamp.isDefined || endTimestamp.isDefined

    if (hasVersionRange && hasTimestampRange) {
      throw QueryCompilationErrors.invalidCdcOptionConflictingRangeTypes()
    }

    val range = if (hasVersionRange) {
      val sv = startVersion.getOrElse(
        throw QueryCompilationErrors.invalidCdcOptionMissingStartingVersion())
      new VersionRange(
        sv,
        endVersion.map(JOptional.of[String]).getOrElse(JOptional.empty[String]),
        startInclusive,
        endInclusive)
    } else if (hasTimestampRange) {
      val startTsValue = startTimestamp.map(parseTimestamp(_, sessionLocalTimeZone)).getOrElse(
        throw QueryCompilationErrors.invalidCdcOptionMissingStartingTimestamp())
      val endTsValue = endTimestamp.map(ts =>
        JLong.valueOf(parseTimestamp(ts, sessionLocalTimeZone)))
      new TimestampRange(
        startTsValue,
        endTsValue.map(JOptional.of[JLong]).getOrElse(JOptional.empty[JLong]),
        startInclusive,
        endInclusive)
    } else {
      // No range specified — unbounded (streaming use case)
      new UnboundedRange()
    }

    new ChangelogInfo(range, deduplicationMode, computeUpdates)
  }

  private def parseTimestamp(timestampStr: String, sessionLocalTimeZone: String): Long = {
    val value = Cast(
      Literal(timestampStr),
      TimestampType,
      Some(sessionLocalTimeZone),
      ansiEnabled = false
    ).eval()
    if (value == null) {
      throw QueryCompilationErrors.invalidCdcOptionInvalidTimestamp(timestampStr)
    }
    value.asInstanceOf[Long]
  }
}
