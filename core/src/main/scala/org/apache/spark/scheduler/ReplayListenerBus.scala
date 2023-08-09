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

package org.apache.spark.scheduler

import java.io.{EOFException, InputStream, IOException}

import scala.io.{Codec, Source}

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ReplayListenerBus._
import org.apache.spark.util.JsonProtocol

/**
 * A SparkListenerBus that can be used to replay events from serialized event data.
 */
private[spark] class ReplayListenerBus extends SparkListenerBus with Logging {

  /**
   * Replay each event in the order maintained in the given stream. The stream is expected to
   * contain one JSON-encoded SparkListenerEvent per line.
   *
   * This method can be called multiple times, but the listener behavior is undefined after any
   * error is thrown by this method.
   *
   * @param logData Stream containing event log data.
   * @param sourceName Filename (or other source identifier) from whence @logData is being read
   * @param maybeTruncated Indicate whether log file might be truncated (some abnormal situations
   *        encountered, log file might not finished writing) or not
   * @param eventsFilter Filter function to select JSON event strings in the log data stream that
   *        should be parsed and replayed. When not specified, all event strings in the log data
   *        are parsed and replayed.
   * @return whether it succeeds to replay the log file entirely without error including
   *         HaltReplayException. false otherwise.
   */
  def replay(
      logData: InputStream,
      sourceName: String,
      maybeTruncated: Boolean = false,
      eventsFilter: ReplayEventsFilter = SELECT_ALL_FILTER): Boolean = {
    val lines = Source.fromInputStream(logData)(Codec.UTF8).getLines()
    replay(lines, sourceName, maybeTruncated, eventsFilter)
  }

  /**
   * Overloaded variant of [[replay()]] which accepts an iterator of lines instead of an
   * [[InputStream]]. Exposed for use by custom ApplicationHistoryProvider implementations.
   */
  def replay(
      lines: Iterator[String],
      sourceName: String,
      maybeTruncated: Boolean,
      eventsFilter: ReplayEventsFilter): Boolean = {
    var currentLine: String = null
    var lineNumber: Int = 0
    val unrecognizedEvents = new scala.collection.mutable.HashSet[String]
    val unrecognizedProperties = new scala.collection.mutable.HashSet[String]

    try {
      val lineEntries = lines
        .zipWithIndex
        .filter { case (line, _) => eventsFilter(line) }

      while (lineEntries.hasNext) {
        try {
          val entry = lineEntries.next()

          currentLine = entry._1
          lineNumber = entry._2 + 1

          postToAll(JsonProtocol.sparkEventFromJson(currentLine))
        } catch {
          case e: ClassNotFoundException =>
            // Ignore unknown events, parse through the event log file.
            // To avoid spamming, warnings are only displayed once for each unknown event.
            if (!unrecognizedEvents.contains(e.getMessage)) {
              logWarning(s"Drop unrecognized event: ${e.getMessage}")
              unrecognizedEvents.add(e.getMessage)
            }
            logDebug(s"Drop incompatible event log: $currentLine")
          case e: UnrecognizedPropertyException =>
            // Ignore unrecognized properties, parse through the event log file.
            // To avoid spamming, warnings are only displayed once for each unrecognized property.
            if (!unrecognizedProperties.contains(e.getMessage)) {
              logWarning(s"Drop unrecognized property: ${e.getMessage}")
              unrecognizedProperties.add(e.getMessage)
            }
            logDebug(s"Drop incompatible event log: $currentLine")
          case jpe: JsonParseException =>
            // We can only ignore exception from last line of the file that might be truncated
            // the last entry may not be the very last line in the event log, but we treat it
            // as such in a best effort to replay the given input
            if (!maybeTruncated || lineEntries.hasNext) {
              throw jpe
            } else {
              logWarning(s"Got JsonParseException from log file $sourceName" +
                s" at line $lineNumber, the file might not have finished writing cleanly.")
            }
        }
      }
      true
    } catch {
      case e: HaltReplayException =>
        // Just stop replay.
        false
      case _: EOFException if maybeTruncated => false
      case ioe: IOException =>
        throw ioe
      case e: Exception =>
        logError(s"Exception parsing Spark event log: $sourceName", e)
        logError(s"Malformed line #$lineNumber: $currentLine\n")
        false
    }
  }

  override protected def isIgnorableException(e: Throwable): Boolean = {
    e.isInstanceOf[HaltReplayException]
  }

}

/**
 * Exception that can be thrown by listeners to halt replay. This is handled by ReplayListenerBus
 * only, and will cause errors if thrown when using other bus implementations.
 */
private[spark] class HaltReplayException extends RuntimeException

private[spark] object ReplayListenerBus {

  type ReplayEventsFilter = (String) => Boolean

  // utility filter that selects all event logs during replay
  val SELECT_ALL_FILTER: ReplayEventsFilter = { (eventString: String) => true }
}
