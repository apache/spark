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

/** [[EventFilter]] decides whether the given event should be accepted or rejected. */
private[spark] trait EventFilter {
  /**
   * Classify whether the event is accepted or rejected by this filter.
   *
   * The method should return the partial function which matches the events where the filter can
   * decide whether the event should be accepted or rejected. Otherwise it should leave the events
   * be unmatched.
   */
  def acceptFn(): PartialFunction[SparkListenerEvent, Boolean]
}

object EventFilter extends Logging {
  def checkFilters(filters: Seq[EventFilter], event: SparkListenerEvent): Boolean = {
    val results = filters.flatMap(_.acceptFn().lift.apply(event))
    results.isEmpty || results.forall(_ == true)
  }

  def applyFilterToFile(
      fs: FileSystem,
      filters: Seq[EventFilter],
      path: Path)(
      fnAccepted: (String, SparkListenerEvent) => Unit)(
      fnRejected: (String, SparkListenerEvent) => Unit)(
      fnUnidentified: String => Unit): Unit = {
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
              fnUnidentified(currentLine)
              None
          }

          event.foreach { e =>
            if (EventFilter.checkFilters(filters, e)) {
              fnAccepted(currentLine, e)
            } else {
              fnRejected(currentLine, e)
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
}
