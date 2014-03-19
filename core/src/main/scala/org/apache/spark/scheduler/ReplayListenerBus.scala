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

import java.io.InputStream
import java.net.URI

import scala.io.Source

import it.unimi.dsi.fastutil.io.FastBufferedInputStream
import org.apache.hadoop.fs.{Path, FileSystem}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * An EventBus that replays logged events from persisted storage
 */
private[spark] class ReplayListenerBus(conf: SparkConf) extends SparkListenerBus with Logging {
  private val compressed = conf.getBoolean("spark.eventLog.compress", false)

  // Only used if compression is enabled
  private lazy val compressionCodec = CompressionCodec.createCodec(conf)

  /**
   * Return a list of paths representing log files in the given directory.
   */
  private def getLogFilePaths(logDir: String, fileSystem: FileSystem): Array[Path] = {
    val path = new Path(logDir)
    if (!fileSystem.exists(path) || !fileSystem.getFileStatus(path).isDir) {
      logWarning("Log path provided is not a valid directory: %s".format(logDir))
      return Array[Path]()
    }
    val logStatus = fileSystem.listStatus(path)
    if (logStatus == null || !logStatus.exists(!_.isDir)) {
      logWarning("Log path provided contains no log files: %s".format(logDir))
      return Array[Path]()
    }
    logStatus.filter(!_.isDir).map(_.getPath).sortBy(_.getName)
  }

  /**
   * Replay each event in the order maintained in the given logs.
   */
  def replay(logDir: String): Boolean = {
    val fileSystem = Utils.getHadoopFileSystem(new URI(logDir))
    val logPaths = getLogFilePaths(logDir, fileSystem)
    if (logPaths.length == 0) {
      return false
    }

    logPaths.foreach { path =>
      // Keep track of input streams at all levels to close them later
      // This is necessary because an exception can occur in between stream initializations
      var fileStream: Option[InputStream] = None
      var bufferedStream: Option[InputStream] = None
      var compressStream: Option[InputStream] = None
      var currentLine = ""
      try {
        currentLine = "<not started>"
        fileStream = Some(fileSystem.open(path))
        bufferedStream = Some(new FastBufferedInputStream(fileStream.get))
        compressStream =
          if (compressed) {
            Some(compressionCodec.compressedInputStream(bufferedStream.get))
          } else bufferedStream

        // Parse each line as an event and post it to all attached listeners
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
    fileSystem.close()
    true
  }
}
