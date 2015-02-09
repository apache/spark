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

import java.io.{BufferedInputStream, InputStream}

import scala.io.Source

import org.apache.hadoop.fs.{Path, FileSystem}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.JsonProtocol

/**
 * A SparkListenerBus that replays logged events from persisted storage.
 *
 * This assumes the given paths are valid log files, where each line can be deserialized into
 * exactly one SparkListenerEvent.
 */
private[spark] class ReplayListenerBus(
    logPaths: Seq[Path],
    fileSystem: FileSystem,
    compressionCodec: Option[CompressionCodec])
  extends SparkListenerBus with Logging {

  private var replayed = false

  if (logPaths.length == 0) {
    logWarning("Log path provided contains no log files.")
  }

  /**
   * Replay each event in the order maintained in the given logs.
   * This should only be called exactly once.
   */
  def replay() {
    assert(!replayed, "ReplayListenerBus cannot replay events more than once")
    logPaths.foreach { path =>
      // Keep track of input streams at all levels to close them later
      // This is necessary because an exception can occur in between stream initializations
      var fileStream: Option[InputStream] = None
      var bufferedStream: Option[InputStream] = None
      var compressStream: Option[InputStream] = None
      var currentLine = "<not started>"
      try {
        fileStream = Some(fileSystem.open(path))
        bufferedStream = Some(new BufferedInputStream(fileStream.get))
        compressStream = Some(wrapForCompression(bufferedStream.get))

        // Parse each line as an event and post the event to all attached listeners
        val lines = Source.fromInputStream(compressStream.get).getLines()
        lines.foreach { line =>
          currentLine = line
          postToAll(JsonProtocol.sparkEventFromJson(parse(line)))
        }
      } catch {
        case e: Exception =>
          logError("Exception in parsing Spark event log %s".format(path), e)
          logError("Malformed line: %s\n".format(currentLine))
      } finally {
        fileStream.foreach(_.close())
        bufferedStream.foreach(_.close())
        compressStream.foreach(_.close())
      }
    }
    replayed = true
  }

  /** If a compression codec is specified, wrap the given stream in a compression stream. */
  private def wrapForCompression(stream: InputStream): InputStream = {
    compressionCodec.map(_.compressedInputStream(stream)).getOrElse(stream)
  }
}
