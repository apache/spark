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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, ReadLimit, SparkDataStream, SupportsAdmissionControl, SupportsTriggerAvailableNow}
import org.apache.spark.sql.connector.read.streaming

/**
 * This class wraps a [[SparkDataStream]] and makes it support Trigger.AvailableNow, by
 * overriding its [[latestOffset]] method to always return the latest offset when the method is
 * first called. It is as if there is no new data coming in from the source after the first
 * [[latestOffset]] call.
 */
class FakeLatestOffsetSupportsTriggerAvailableNow(source: SparkDataStream)
  extends SparkDataStream with SupportsTriggerAvailableNow with Logging {

  private var fetchedOffset: Option[streaming.Offset] = _

  override def initialOffset(): streaming.Offset = source.initialOffset()

  override def deserializeOffset(json: String): streaming.Offset = source.deserializeOffset(json)

  override def commit(end: streaming.Offset): Unit = source.commit(end)

  override def stop(): Unit = source.stop()

  override def prepareForTriggerAvailableNow(): Unit = {}

  /**
   * Always return [[ReadLimit.allAvailable]]
   */
  override def getDefaultReadLimit: ReadLimit = source match {
    case s: SupportsAdmissionControl =>
      val limit = s.getDefaultReadLimit
      if (limit != ReadLimit.allAvailable()) {
        logWarning(s"The read limit $limit is ignored because source $source does not " +
          "support running Trigger.AvailableNow queries.")
      }
      ReadLimit.allAvailable()

    case _ => ReadLimit.allAvailable()
  }

  /**
   * Get and return the latest offset for all available data when called for the first time, then
   * return the same result when called later.
   *
   * It is as if there is no new data coming in from the source after the first method call.
   */
  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    if (fetchedOffset == null) {
      fetchedOffset = source match {
        case s: SupportsAdmissionControl =>
          Option(s.latestOffset(startOffset, ReadLimit.allAvailable()))
        case s: Source => s.getOffset
        case m: MicroBatchStream => Option(m.latestOffset())
        case s => throw new IllegalStateException(s"Unexpected source: $s")
      }
    }
    fetchedOffset.orNull
  }

  override def reportLatestOffset: streaming.Offset = source match {
    // Return the real latest offset here since this is only used for metrics
    case s: SupportsAdmissionControl => s.reportLatestOffset()
    case s: Source => s.getOffset.orNull
    case s: MicroBatchStream => s.latestOffset()
  }
}
