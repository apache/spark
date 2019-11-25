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

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * EventFilterBuilder provides the interface to gather the information from events being received
 * by [[SparkListenerInterface]], and create a new [[EventFilter]] instance which leverages
 * information gathered to decide whether the event should be filtered or not.
 */
private[spark] trait EventFilterBuilder extends SparkListenerInterface {
  def createFilter(): EventFilter
}

object EventFilterBuilder {
  /**
   * Loads all available EventFilterBuilders in classloader via ServiceLoader, and initializes
   * them via replaying events in given files.
   */
  def initializeBuilders(fs: FileSystem, files: Seq[Path]): Seq[EventFilterBuilder] = {
    val bus = new ReplayListenerBus()

    val builders = ServiceLoader.load(classOf[EventFilterBuilder],
      Utils.getContextOrSparkClassLoader).asScala.toSeq
    builders.foreach(bus.addListener)

    files.foreach { log =>
      Utils.tryWithResource(EventLogFileReader.openEventLog(log, fs)) { in =>
        bus.replay(in, log.getName)
      }
    }

    builders
  }
}

/**
 * [[EventFilter]] decides whether the given event should be filtered in, or filtered out when
 * compacting event log files.
 *
 * The meaning of return values of each filterXXX method are following:
 * - Some(true): Filter in this event.
 * - Some(false): Filter out this event.
 * - None: Don't mind about this event. No problem even other filters decide to filter out.
 *
 * Please refer [[FilteredEventLogFileRewriter]] for more details on how the filter will be used.
 */
private[spark] trait EventFilter {
  def filterStageCompleted(event: SparkListenerStageCompleted): Option[Boolean] = None

  def filterStageSubmitted(event: SparkListenerStageSubmitted): Option[Boolean] = None

  def filterTaskStart(event: SparkListenerTaskStart): Option[Boolean] = None

  def filterTaskGettingResult(event: SparkListenerTaskGettingResult): Option[Boolean] = None

  def filterTaskEnd(event: SparkListenerTaskEnd): Option[Boolean] = None

  def filterJobStart(event: SparkListenerJobStart): Option[Boolean] = None

  def filterJobEnd(event: SparkListenerJobEnd): Option[Boolean] = None

  def filterEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Option[Boolean] = None

  def filterBlockManagerAdded(event: SparkListenerBlockManagerAdded): Option[Boolean] = None

  def filterBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Option[Boolean] = None

  def filterUnpersistRDD(event: SparkListenerUnpersistRDD): Option[Boolean] = None

  def filterApplicationStart(event: SparkListenerApplicationStart): Option[Boolean] = None

  def filterApplicationEnd(event: SparkListenerApplicationEnd): Option[Boolean] = None

  def filterExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Option[Boolean] = None

  def filterStageExecutorMetrics(event: SparkListenerStageExecutorMetrics): Option[Boolean] = None

  def filterExecutorAdded(event: SparkListenerExecutorAdded): Option[Boolean] = None

  def filterExecutorRemoved(event: SparkListenerExecutorRemoved): Option[Boolean] = None

  def filterExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Option[Boolean] = None

  def filterExecutorBlacklistedForStage(
      event: SparkListenerExecutorBlacklistedForStage): Option[Boolean] = None

  def filterNodeBlacklistedForStage(
      event: SparkListenerNodeBlacklistedForStage): Option[Boolean] = None

  def filterExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Option[Boolean] = None

  def filterNodeBlacklisted(event: SparkListenerNodeBlacklisted): Option[Boolean] = None

  def filterNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Option[Boolean] = None

  def filterBlockUpdated(event: SparkListenerBlockUpdated): Option[Boolean] = None

  def filterSpeculativeTaskSubmitted(
      event: SparkListenerSpeculativeTaskSubmitted): Option[Boolean] = None

  def filterOtherEvent(event: SparkListenerEvent): Option[Boolean] = None
}

object EventFilter {
  def checkFilters(filters: Seq[EventFilter], event: SparkListenerEvent): Boolean = {
    val results = filters.flatMap(filter => applyFilter(filter, event))
    results.isEmpty || results.forall(_ == true)
  }

  private def applyFilter(filter: EventFilter, event: SparkListenerEvent): Option[Boolean] = {
    // This pattern match should have same list of event types, but it would be safe even if
    // it's out of sync, once filter doesn't mark events to filter out for unknown event types.
    event match {
      case event: SparkListenerStageSubmitted => filter.filterStageSubmitted(event)
      case event: SparkListenerStageCompleted => filter.filterStageCompleted(event)
      case event: SparkListenerJobStart => filter.filterJobStart(event)
      case event: SparkListenerJobEnd => filter.filterJobEnd(event)
      case event: SparkListenerTaskStart => filter.filterTaskStart(event)
      case event: SparkListenerTaskGettingResult => filter.filterTaskGettingResult(event)
      case event: SparkListenerTaskEnd => filter.filterTaskEnd(event)
      case event: SparkListenerEnvironmentUpdate => filter.filterEnvironmentUpdate(event)
      case event: SparkListenerBlockManagerAdded => filter.filterBlockManagerAdded(event)
      case event: SparkListenerBlockManagerRemoved => filter.filterBlockManagerRemoved(event)
      case event: SparkListenerUnpersistRDD => filter.filterUnpersistRDD(event)
      case event: SparkListenerApplicationStart => filter.filterApplicationStart(event)
      case event: SparkListenerApplicationEnd => filter.filterApplicationEnd(event)
      case event: SparkListenerExecutorMetricsUpdate => filter.filterExecutorMetricsUpdate(event)
      case event: SparkListenerStageExecutorMetrics => filter.filterStageExecutorMetrics(event)
      case event: SparkListenerExecutorAdded => filter.filterExecutorAdded(event)
      case event: SparkListenerExecutorRemoved => filter.filterExecutorRemoved(event)
      case event: SparkListenerExecutorBlacklistedForStage =>
        filter.filterExecutorBlacklistedForStage(event)
      case event: SparkListenerNodeBlacklistedForStage =>
        filter.filterNodeBlacklistedForStage(event)
      case event: SparkListenerExecutorBlacklisted => filter.filterExecutorBlacklisted(event)
      case event: SparkListenerExecutorUnblacklisted => filter.filterExecutorUnblacklisted(event)
      case event: SparkListenerNodeBlacklisted => filter.filterNodeBlacklisted(event)
      case event: SparkListenerNodeUnblacklisted => filter.filterNodeUnblacklisted(event)
      case event: SparkListenerBlockUpdated => filter.filterBlockUpdated(event)
      case event: SparkListenerSpeculativeTaskSubmitted =>
        filter.filterSpeculativeTaskSubmitted(event)
      case _ => filter.filterOtherEvent(event)
    }
  }
}

trait EventFilterApplier extends Logging {
  val fs: FileSystem
  val filters: Seq[EventFilter]

  def applyFilter(path: Path): Unit = {
    Utils.tryWithResource(EventLogFileReader.openEventLog(path, fs)) { in =>
      val lines = Source.fromInputStream(in)(Codec.UTF8).getLines()

      var currentLine: String = null
      var lineNumber: Int = 0

      try {
        val lineEntries = lines.zipWithIndex
        while (lineEntries.hasNext) {
          val entry = lineEntries.next()

          currentLine = entry._1
          lineNumber = entry._2 + 1

          val event = try {
            Some(JsonProtocol.sparkEventFromJson(parse(currentLine)))
          } catch {
            // ignore any exception occurred from unidentified json
            // just skip handling and write the line
            case NonFatal(_) =>
              handleUnidentifiedLine(currentLine)
              None
          }

          event.foreach { e =>
            if (EventFilter.checkFilters(filters, e)) {
              handleFilteredInEvent(currentLine, e)
            } else {
              handleFilteredOutEvent(currentLine, e)
            }
          }
        }
      } catch {
        case e: Exception =>
          logError(s"Exception parsing Spark event log: ${path.getName}", e)
          logError(s"Malformed line #$lineNumber: $currentLine\n")
          throw e
      }
    }
  }

  protected def handleFilteredInEvent(line: String, event: SparkListenerEvent): Unit

  protected def handleFilteredOutEvent(line: String, event: SparkListenerEvent): Unit

  protected def handleUnidentifiedLine(line: String): Unit
}
