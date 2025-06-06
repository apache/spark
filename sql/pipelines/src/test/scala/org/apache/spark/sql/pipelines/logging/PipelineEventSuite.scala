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
import java.time.ZonedDateTime
import java.time.format.{DateTimeFormatter, FormatStyle}
import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.pipelines.common.FlowStatus

class PipelineEventSuite extends SparkFunSuite with Logging {

  test("event timestamps are formatted correctly") {
    // MTV was launched on Saturday, August 1, 1981, at 12:01 am Eastern Time
    val formatter = DateTimeFormatter
      .ofLocalizedDateTime(FormatStyle.LONG, FormatStyle.LONG)
      .withLocale(Locale.forLanguageTag("en_US"))

    val mtvLaunch = ZonedDateTime.parse("1981 Aug 1 00:01:00 EDT", formatter)

    val ts = new Timestamp(mtvLaunch.toInstant.toEpochMilli)
    val formatted = EventHelpers.formatTimestamp(ts)
    // expect 4 hours later in UTC (5 time zones minus 1 hour daylight savings).
    assert(formatted == "1981-08-01T04:01:00.000Z")
  }

  test("event timestamps can be formatted and parsed") {
    val ts = new Timestamp(System.currentTimeMillis())
    assert(ts == EventHelpers.parseTimestamp(EventHelpers.formatTimestamp(ts)))
  }

  private def makeEvent() = {
    ConstructPipelineEvent(
      origin = PipelineEventOrigin(
        flowName = Option("a"),
        datasetName = None,
        sourceCodeLocation = None
      ),
      level = EventLevel.INFO,
      message = "OK",
      details = FlowProgress(FlowStatus.STARTING)
    )
  }

  test("Two consecutive events have different id and are ordered by timestamp") {
    val event1 = makeEvent()
    val event2 = makeEvent()
    // ensure the timestamp is close to current time - this avoids grave mistakes in time scale
    assert(System.currentTimeMillis() >= EventHelpers.parseTimestamp(event2.timestamp).getTime)
    assert(
      System.currentTimeMillis() - EventHelpers.parseTimestamp(event2.timestamp).getTime < 1000
    )
    // can't write ts1 <= ts2, but this expresses the same
    assert(
      !EventHelpers
        .parseTimestamp(event1.timestamp)
        .after(EventHelpers.parseTimestamp(event2.timestamp))
    )
    // the two events, being created right after one another, should not be far apart
    assert(
      EventHelpers.parseTimestamp(event2.timestamp).getTime -
      EventHelpers.parseTimestamp(event1.timestamp).getTime < 1000
    )
  }
  test("formatTimestamp / parseTimestamp") {
    val ts = new Timestamp(1747338049615L)
    val expectedTsString = "2025-05-15T19:40:49.615Z"
    assert(EventHelpers.formatTimestamp(ts) == "2025-05-15T19:40:49.615Z")
    assert(EventHelpers.parseTimestamp(expectedTsString) == ts)
    // Ensure parseTimestamp can handle an empty String
    assert(EventHelpers.parseTimestamp("") == new Timestamp(0L))
  }

  test("basic flow progress event has expected fields set") {
    val event = ConstructPipelineEvent(
      origin = PipelineEventOrigin(
        flowName = Option("a"),
        datasetName = None,
        sourceCodeLocation = None
      ),
      level = EventLevel.INFO,
      message = "Flow 'a' has completed",
      details = FlowProgress(FlowStatus.COMPLETED)
    )
    assert(event.level == EventLevel.INFO)
    assert(event.message == "Flow 'a' has completed")
    assert(event.details.isInstanceOf[FlowProgress])
    assert(event.origin.flowName == Option("a"))
  }
}
