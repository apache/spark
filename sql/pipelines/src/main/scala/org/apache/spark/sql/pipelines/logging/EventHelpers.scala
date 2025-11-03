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
package org.apache.spark.sql.pipelines.logging

import java.sql.Timestamp
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

/** Contains helpers and implicits for working with [[PipelineEvent]]s. */
object EventHelpers {

  /** A format string that defines how timestamps are serialized in a [[PipelineEvent]]. */
  private val timestampFormat: String = "yyyy-MM-dd'T'HH:mm:ss.SSSXX"
  // Currently only the UTC timezone is supported. Eventually we want to allow the user to specify
  // the timezone as a pipeline level setting using the SESSION_LOCAL_TIMEZONE key, and it should
  // not be possible to change this setting during a pipeline run.
  private val zoneId: ZoneId = ZoneId.of("UTC")

  private val formatter: DateTimeFormatter = DateTimeFormatter
    .ofPattern(timestampFormat)
    .withZone(zoneId)

  /** Converts a timestamp to a string in ISO 8601 format. */
  def formatTimestamp(ts: Timestamp): String = {
    val instant = Instant.ofEpochMilli(ts.getTime)
    formatter.format(instant)
  }

  /** Converts an ISO 8601 formatted timestamp to a {@link java.sql.Timestamp}.  */
  def parseTimestamp(timeString: String): Timestamp = {
    if (timeString.isEmpty) {
      new Timestamp(0L)
    } else {
      val instant = Instant.from(formatter.parse(timeString))
      new Timestamp(instant.toEpochMilli)
    }
  }
}
