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
 * This class wraps a [[SparkDataStream]] and makes it support Trigger.AvailableNow, by overriding
 * its [[latestOffset]] method to always return the latest offset at the beginning of the query.
 */
class AvailableNowDataStreamWrapper(val delegate: SparkDataStream)
  extends SparkDataStream with SupportsTriggerAvailableNow with Logging {

  private var fetchedOffset: streaming.Offset = _

  override def initialOffset(): streaming.Offset = delegate.initialOffset()

  override def deserializeOffset(json: String): streaming.Offset = delegate.deserializeOffset(json)

  override def commit(end: streaming.Offset): Unit = delegate.commit(end)

  override def stop(): Unit = delegate.stop()

  private def getInitialOffset: streaming.Offset = {
    delegate match {
      case _: Source => null
      case m: MicroBatchStream => m.initialOffset
    }
  }

  /**
   * Fetch and store the latest offset for all available data at the beginning of the query.
   */
  override def prepareForTriggerAvailableNow(): Unit = {
    fetchedOffset = delegate match {
      case s: SupportsAdmissionControl =>
        s.latestOffset(getInitialOffset, ReadLimit.allAvailable())
      case s: Source => s.getOffset.orNull
      case m: MicroBatchStream => m.latestOffset()
      case s => throw new IllegalStateException(s"Unexpected source: $s")
    }
  }

  /**
   * Always return [[ReadLimit.allAvailable]]
   */
  override def getDefaultReadLimit: ReadLimit = delegate match {
    case s: SupportsAdmissionControl =>
      val limit = s.getDefaultReadLimit
      if (limit != ReadLimit.allAvailable()) {
        logWarning(s"The read limit $limit is ignored because source $delegate does not " +
          "support running Trigger.AvailableNow queries.")
      }
      ReadLimit.allAvailable()

    case _ => ReadLimit.allAvailable()
  }

  /**
   * Return the latest offset pre-fetched in [[prepareForTriggerAvailableNow]].
   */
  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset =
    fetchedOffset

  override def reportLatestOffset: streaming.Offset = delegate match {
    // Return the real latest offset here since this is only used for metrics
    case s: SupportsAdmissionControl => s.reportLatestOffset()
    case s: Source => s.getOffset.orNull
    case s: MicroBatchStream => s.latestOffset()
  }
}
