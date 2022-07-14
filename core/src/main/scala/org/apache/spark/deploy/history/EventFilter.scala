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

package org.apache.spark.deploy.history

import scala.io.{Codec, Source}
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.deploy.history.EventFilter.FilterStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * EventFilterBuilder provides the interface to gather the information from events being received
 * by [[SparkListenerInterface]], and create a new [[EventFilter]] instance which leverages
 * information gathered to decide whether the event should be accepted or not.
 */
private[spark] trait EventFilterBuilder extends SparkListenerInterface {
  def createFilter(): EventFilter
}

/** [[EventFilter]] decides whether the given event should be accepted or rejected. */
private[spark] trait EventFilter {
  /**
   * Provide statistic information of event filter, which would be used for measuring the score
   * of compaction.
   *
   * To simplify the condition, currently the fields of statistic are static, since major kinds of
   * events compaction would filter out are job related event types. If the filter doesn't track
   * with job related events, return None instead.
   */
  def statistics(): Option[FilterStatistics]

  /**
   * Classify whether the event is accepted or rejected by this filter.
   *
   * The method should return the partial function which matches the events where the filter can
   * decide whether the event should be accepted or rejected. Otherwise it should leave the events
   * be unmatched.
   */
  def acceptFn(): PartialFunction[SparkListenerEvent, Boolean]
}

private[spark] object EventFilter extends Logging {
  case class FilterStatistics(
      totalJobs: Long,
      liveJobs: Long,
      totalStages: Long,
      liveStages: Long,
      totalTasks: Long,
      liveTasks: Long)

  def applyFilterToFile(
      fs: FileSystem,
      filters: Seq[EventFilter],
      path: Path,
      onAccepted: (String, SparkListenerEvent) => Unit,
      onRejected: (String, SparkListenerEvent) => Unit,
      onUnidentified: String => Unit): Unit = {
    Utils.tryWithResource(EventLogFileReader.openEventLog(path, fs)) { in =>
      val lines = Source.fromInputStream(in)(Codec.UTF8).getLines()

      lines.zipWithIndex.foreach { case (line, lineNum) =>
        try {
          val event = try {
            Some(JsonProtocol.sparkEventFromJson(line))
          } catch {
            // ignore any exception occurred from unidentified json
            case NonFatal(_) =>
              onUnidentified(line)
              None
          }

          event.foreach { e =>
            val results = filters.flatMap(_.acceptFn().lift.apply(e))
            if (results.nonEmpty && results.forall(_ == false)) {
              onRejected(line, e)
            } else {
              onAccepted(line, e)
            }
          }
        } catch {
          case e: Exception =>
            logError(s"Exception parsing Spark event log: ${path.getName}", e)
            logError(s"Malformed line #$lineNum: $line\n")
            throw e
        }
      }
    }
  }
}
