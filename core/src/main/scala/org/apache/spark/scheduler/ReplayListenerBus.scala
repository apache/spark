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

import java.io.{BufferedReader, EOFException, InputStream, InputStreamReader, IOException}
import java.nio.charset.{CodingErrorAction, StandardCharsets}

import scala.annotation.tailrec

import com.fasterxml.jackson.core.{JsonParseException, JsonProcessingException}
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.History
import org.apache.spark.scheduler.ReplayListenerBus._
import org.apache.spark.util.JsonProtocol

/**
 * A SparkListenerBus that can be used to replay events from serialized event data.
 *
 * @param maxLineLength Maximum UTF-8 byte length of a single event log line, excluding its line
 *                      ending. Longer lines are drained, skipped and logged, bounding the
 *                      memory replay can use when an event log is corrupt or unexpectedly large.
 */
private[spark] class ReplayListenerBus(
    maxLineLength: Int = ReplayListenerBus.DEFAULT_MAX_LINE_LENGTH)
  extends SparkListenerBus with Logging {

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
    val lines = boundedLines(logData, sourceName)
    replayEntries(lines, sourceName, maybeTruncated, eventsFilter)
  }

  /**
   * Reads '\n'-terminated lines and retains their original zero-based indices. The limit
   * measures UTF-8 content bytes, excluding the line ending, rather than JVM heap usage.
   * The character buffer is bounded by the limit plus one possible trailing CR. An over-long
   * line is drained and skipped with a warning instead of being turned into a String.
   */
  private def boundedLines(
      logData: InputStream,
      sourceName: String): Iterator[(String, Int)] = {
    // Fail on malformed input like Source.getLines() does instead of replacing it.
    val decoder = StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    val reader = new BufferedReader(new InputStreamReader(logData, decoder))
    new Iterator[(String, Int)] {
      private var nextLine: (String, Int) = _
      private var lineIndex = 0
      private var lineFetched = false
      private var warned = false

      override def hasNext: Boolean = {
        if (!lineFetched) {
          nextLine = fetchLine()
          lineFetched = true
        }
        nextLine != null
      }

      override def next(): (String, Int) = {
        if (!hasNext) {
          throw new NoSuchElementException("No more lines")
        }
        val line = nextLine
        nextLine = null
        lineFetched = false
        line
      }

      @tailrec private def fetchLine(): (String, Int) = {
        val buffer = new BoundedLineBuffer(maxLineLength)
        var c = reader.read()
        if (c == -1) {
          null
        } else {
          val index = lineIndex
          lineIndex += 1
          while (c != -1 && c != '\n') {
            buffer.append(c.toChar)
            c = reader.read()
          }
          val maybeLine = buffer.result()
          if (maybeLine.isEmpty) {
            if (!warned) {
              logWarning(log"Skipped event log lines longer than " +
                log"${MDC(MAX_SIZE, maxLineLength)} bytes in " +
                log"${MDC(FILE_NAME, sourceName)}")
              warned = true
            }
            fetchLine()
          } else {
            (maybeLine.get, index)
          }
        }
      }
    }
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
    replayEntries(lines.zipWithIndex, sourceName, maybeTruncated, eventsFilter)
  }

  private def replayEntries(
      lines: Iterator[(String, Int)],
      sourceName: String,
      maybeTruncated: Boolean,
      eventsFilter: ReplayEventsFilter): Boolean = {
    var currentLine: String = null
    var lineNumber: Int = 0
    val unrecognizedEvents = new scala.collection.mutable.HashSet[String]
    val unrecognizedProperties = new scala.collection.mutable.HashSet[String]

    try {
      val lineEntries = lines.filter { case (line, _) => eventsFilter(line) }

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
              logWarning(log"Drop unrecognized event: ${MDC(ERROR, e.getMessage)}")
              unrecognizedEvents.add(e.getMessage)
            }
            logDebug(s"Drop incompatible event log: $currentLine")
          case e: UnrecognizedPropertyException =>
            // Ignore unrecognized properties, parse through the event log file.
            // To avoid spamming, warnings are only displayed once for each unrecognized property.
            if (!unrecognizedProperties.contains(e.getMessage)) {
              logWarning(log"Drop unrecognized property: ${MDC(ERROR, e.getMessage)}")
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
              logWarning(log"Got JsonParseException from log file ${MDC(FILE_NAME, sourceName)}" +
                log" at line ${MDC(LINE_NUM, lineNumber)}, " +
                log"the file might not have finished writing cleanly.")
            }
        }
      }
      true
    } catch {
      case e: HaltReplayException =>
        // Just stop replay.
        false
      case _: EOFException if maybeTruncated => false
      case jpe: JsonProcessingException =>
        logError(log"Exception parsing Spark event log: ${MDC(PATH, sourceName)} " +
          log"at line ${MDC(LINE_NUM, lineNumber)}", jpe)
        throw jpe
      case ioe: IOException =>
        throw ioe
      case e: Exception =>
        logError(log"Exception parsing Spark event log: ${MDC(PATH, sourceName)}", e)
        logError(log"Malformed line #${MDC(LINE_NUM, lineNumber)}: ${MDC(LINE, currentLine)}\n")
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

  /**
   * Default UTF-8 line-length cap, matching spark.history.fs.eventLog.maxLineLength.
   * Keep the maximum buffered character count close to the original 512 MiB / 2 cap.
   */
  val DEFAULT_MAX_LINE_LENGTH: Int = 256 * 1024 * 1024

  /** Resolves the byte limit, using Int.MaxValue for non-positive or larger values. */
  def maxLineLength(conf: SparkConf): Int = {
    val configured = conf.get(History.EVENT_LOG_MAX_LINE_LENGTH)
    if (configured <= 0 || configured > Int.MaxValue) Int.MaxValue else configured.toInt
  }

  type ReplayEventsFilter = (String) => Boolean

  // utility filter that selects all event logs during replay
  val SELECT_ALL_FILTER: ReplayEventsFilter = { (eventString: String) => true }

  /** Accumulates a bounded prefix of one decoded line, allowing a possible trailing CR. */
  private[scheduler] class BoundedLineBuffer(maxLineLength: Int) {
    private val buffer = new java.lang.StringBuilder()
    private var byteLength = 0L
    private val bufferLimit = maxLineLength.toLong + 1

    def length: Int = buffer.length()

    def append(c: Char): Unit = {
      if (byteLength <= bufferLimit) {
        // The strict decoder emits valid surrogate pairs: count two bytes per half.
        byteLength += (if (c < 0x80) 1 else if (c < 0x800 || Character.isSurrogate(c)) 2 else 3)
        // Reserve one extra byte for a possible CR in the line ending.
        if (byteLength <= bufferLimit) {
          buffer.append(c)
        }
      }
    }

    def result(): Option[String] = {
      val trailingCR = length > 0 && buffer.charAt(length - 1) == '\r'
      val contentLength = byteLength - (if (trailingCR) 1 else 0)
      if (contentLength > maxLineLength) {
        None
      } else if (trailingCR) {
        Some(buffer.substring(0, length - 1))
      } else {
        Some(buffer.toString)
      }
    }
  }

}
