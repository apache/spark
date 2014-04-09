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

import scala.io.Source

import it.unimi.dsi.fastutil.io.FastBufferedInputStream
import org.apache.hadoop.fs.{Path, FileSystem}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * A SparkListenerBus that replays logged events from persisted storage.
 *
 * This class expects files to be appropriately prefixed as specified in EventLoggingListener.
 * There exists a one-to-one mapping between ReplayListenerBus and event logging applications.
 */
private[spark] class ReplayListenerBus(
    logDir: String,
    val fileSystem: FileSystem)
  extends SparkListenerBus with Logging {

  def this(logDir: String) = this(logDir, Utils.getHadoopFileSystem(logDir))

  private var applicationComplete = false
  private var sparkVersion: Option[String] = None
  private var compressionCodec: Option[CompressionCodec] = None
  private var logPaths = Array[Path]()
  private var started = false
  private var replayed = false

  /**
   * Prepare state for reading event logs.
   *
   * This gathers relevant files in the given directory and extracts meaning from each category.
   * More specifically, this involves looking for event logs, the Spark version file, the
   * compression codec file (if event logs are compressed), and the application completion
   * file (if the application has run to completion).
   */
  def start() {
    val filePaths = getFilePaths(logDir, fileSystem)
    logPaths = filePaths
      .filter { file => EventLoggingListener.isEventLogFile(file.getName) }
    sparkVersion = filePaths
      .find { file => EventLoggingListener.isSparkVersionFile(file.getName) }
      .map { file => EventLoggingListener.parseSparkVersion(file.getName) }
    compressionCodec = filePaths
      .find { file => EventLoggingListener.isCompressionCodecFile(file.getName) }
      .map { file =>
        val codec = EventLoggingListener.parseCompressionCodec(file.getName)
        val conf = new SparkConf
        conf.set("spark.io.compression.codec", codec)
        CompressionCodec.createCodec(conf)
      }
    applicationComplete = filePaths
      .exists { file => EventLoggingListener.isApplicationCompleteFile(file.getName) }
    started = true
  }

  /** Return whether the associated application signaled completion. */
  def isApplicationComplete: Boolean = {
    assert(started, "ReplayListenerBus not started yet")
    applicationComplete
  }

  /** Return the version of Spark on which the given application was run. */
  def getSparkVersion: String = {
    assert(started, "ReplayListenerBus not started yet")
    sparkVersion.getOrElse("<Unknown>")
  }

  /**
   * Replay each event in the order maintained in the given logs. This should only be called
   * exactly once. Return whether event logs are actually found.
   */
  def replay(): Boolean = {
    assert(started, "ReplayListenerBus must be started before replaying logged events")
    assert(!replayed, "ReplayListenerBus cannot replay events more than once")

    if (logPaths.length == 0) {
      logWarning("Log path provided contains no log files: %s".format(logDir))
      return false
    }

    logPaths.foreach { path =>
      // Keep track of input streams at all levels to close them later
      // This is necessary because an exception can occur in between stream initializations
      var fileStream: Option[InputStream] = None
      var bufferedStream: Option[InputStream] = None
      var compressStream: Option[InputStream] = None
      var currentLine = "<not started>"
      try {
        fileStream = Some(fileSystem.open(path))
        bufferedStream = Some(new FastBufferedInputStream(fileStream.get))
        compressStream = Some(wrapForCompression(bufferedStream.get))

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

    replayed = true
    true
  }

  /** Stop the file system. */
  def stop() {
    fileSystem.close()
  }

  /** If a compression codec is specified, wrap the given stream in a compression stream. */
  private def wrapForCompression(stream: InputStream): InputStream = {
    compressionCodec.map(_.compressedInputStream(stream)).getOrElse(stream)
  }

  /** Return a list of paths representing files found in the given directory. */
  private def getFilePaths(logDir: String, fileSystem: FileSystem): Array[Path] = {
    try {
      val path = new Path(logDir)
      if (!fileSystem.exists(path) || !fileSystem.getFileStatus(path).isDir) {
        logWarning("Log path provided is not a valid directory: %s".format(logDir))
        return Array[Path]()
      }
      val logStatus = fileSystem.listStatus(path)
      if (logStatus == null || !logStatus.exists(!_.isDir)) {
        logWarning("No files are found in the given log directory: %s".format(logDir))
        return Array[Path]()
      }
      logStatus.filter(!_.isDir).map(_.getPath).sortBy(_.getName)
    } catch {
      case t: Throwable =>
        logError("Exception in accessing log files in %s".format(logDir), t)
        Array[Path]()
    }
  }
}
