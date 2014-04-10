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

import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{FileLogger, JsonProtocol}

/**
 * A SparkListener that logs events to persistent storage.
 *
 * Event logging is specified by the following configurable parameters:
 *   spark.eventLog.enabled - Whether event logging is enabled.
 *   spark.eventLog.compress - Whether to compress logged events
 *   spark.eventLog.overwrite - Whether to overwrite any existing files.
 *   spark.eventLog.dir - Path to the directory in which events are logged.
 *   spark.eventLog.buffer.kb - Buffer size to use when writing to output streams
 */
private[spark] class EventLoggingListener(appName: String, conf: SparkConf)
  extends SparkListener with Logging {

  import EventLoggingListener._

  private val shouldCompress = conf.getBoolean("spark.eventLog.compress", false)
  private val shouldOverwrite = conf.getBoolean("spark.eventLog.overwrite", false)
  private val outputBufferSize = conf.getInt("spark.eventLog.buffer.kb", 100) * 1024
  private val logBaseDir = conf.get("spark.eventLog.dir", "/tmp/spark-events").stripSuffix("/")
  private val name = appName.replaceAll("[ :/]", "-").toLowerCase + "-" + System.currentTimeMillis
  val logDir = logBaseDir + "/" + name

  private val logger =
    new FileLogger(logDir, conf, outputBufferSize, shouldCompress, shouldOverwrite)

  /**
   * Begin logging events.
   * If compression is used, log a file that indicates which compression library is used.
   */
  def start() {
    logInfo("Logging events to %s".format(logDir))
    if (shouldCompress) {
      val codec = conf.get("spark.io.compression.codec", CompressionCodec.DEFAULT_COMPRESSION_CODEC)
      logger.newFile(COMPRESSION_CODEC_PREFIX + codec)
    }
    logger.newFile(SPARK_VERSION_PREFIX + SparkContext.SPARK_VERSION)
    logger.newFile(LOG_PREFIX + logger.fileIndex)
  }

  /** Log the event as JSON. */
  private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false) {
    val eventJson = compact(render(JsonProtocol.sparkEventToJson(event)))
    logger.logLine(eventJson)
    if (flushLogger) {
      logger.flush()
    }
  }

  // Events that do not trigger a flush
  override def onStageSubmitted(event: SparkListenerStageSubmitted) =
    logEvent(event)
  override def onTaskStart(event: SparkListenerTaskStart) =
    logEvent(event)
  override def onTaskGettingResult(event: SparkListenerTaskGettingResult) =
    logEvent(event)
  override def onTaskEnd(event: SparkListenerTaskEnd) =
    logEvent(event)
  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate) =
    logEvent(event)

  // Events that trigger a flush
  override def onStageCompleted(event: SparkListenerStageCompleted) =
    logEvent(event, flushLogger = true)
  override def onJobStart(event: SparkListenerJobStart) =
    logEvent(event, flushLogger = true)
  override def onJobEnd(event: SparkListenerJobEnd) =
    logEvent(event, flushLogger = true)
  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded) =
    logEvent(event, flushLogger = true)
  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved) =
    logEvent(event, flushLogger = true)
  override def onUnpersistRDD(event: SparkListenerUnpersistRDD) =
    logEvent(event, flushLogger = true)
  override def onApplicationStart(event: SparkListenerApplicationStart) =
    logEvent(event, flushLogger = true)
  override def onApplicationEnd(event: SparkListenerApplicationEnd) =
    logEvent(event, flushLogger = true)

  /**
   * Stop logging events.
   * In addition, create an empty special file to indicate application completion.
   */
  def stop() = {
    logger.newFile(APPLICATION_COMPLETE)
    logger.stop()
  }
}

private[spark] object EventLoggingListener extends Logging {
  val LOG_PREFIX = "EVENT_LOG_"
  val SPARK_VERSION_PREFIX = "SPARK_VERSION_"
  val COMPRESSION_CODEC_PREFIX = "COMPRESSION_CODEC_"
  val APPLICATION_COMPLETE = "APPLICATION_COMPLETE"

  // A cache for compression codecs to avoid creating the same codec many times
  private val codecMap = new mutable.HashMap[String, CompressionCodec]

  def isEventLogFile(fileName: String): Boolean = {
    fileName.startsWith(LOG_PREFIX)
  }

  def isSparkVersionFile(fileName: String): Boolean = {
    fileName.startsWith(SPARK_VERSION_PREFIX)
  }

  def isCompressionCodecFile(fileName: String): Boolean = {
    fileName.startsWith(COMPRESSION_CODEC_PREFIX)
  }

  def isApplicationCompleteFile(fileName: String): Boolean = {
    fileName == APPLICATION_COMPLETE
  }

  def parseSparkVersion(fileName: String): String = {
    if (isSparkVersionFile(fileName)) {
      fileName.replaceAll(SPARK_VERSION_PREFIX, "")
    } else ""
  }

  def parseCompressionCodec(fileName: String): String = {
    if (isCompressionCodecFile(fileName)) {
      fileName.replaceAll(COMPRESSION_CODEC_PREFIX, "")
    } else ""
  }

  /**
   * Parse the event logging information associated with the logs in the given directory.
   *
   * Specifically, this looks for event log files, the Spark version file, the compression
   * codec file (if event logs are compressed), and the application completion file (if the
   * application has run to completion).
   */
  def parseLoggingInfo(logDir: Path, fileSystem: FileSystem): EventLoggingInfo = {
    try {
      val fileStatuses = fileSystem.listStatus(logDir)
      val filePaths =
        if (fileStatuses != null) {
          fileStatuses.filter(!_.isDir).map(_.getPath).toSeq
        } else {
          Seq[Path]()
        }
      if (filePaths.isEmpty) {
        logWarning("No files found in logging directory %s".format(logDir))
      }
      EventLoggingInfo(
        logPaths = filePaths.filter { path => isEventLogFile(path.getName) },
        sparkVersion = filePaths
          .find { path => isSparkVersionFile(path.getName) }
          .map { path => parseSparkVersion(path.getName) }
          .getOrElse("<Unknown>"),
        compressionCodec = filePaths
          .find { path => isCompressionCodecFile(path.getName) }
          .map { path =>
            val codec = EventLoggingListener.parseCompressionCodec(path.getName)
            val conf = new SparkConf
            conf.set("spark.io.compression.codec", codec)
            codecMap.getOrElseUpdate(codec, CompressionCodec.createCodec(conf))
          },
        applicationComplete = filePaths.exists { path => isApplicationCompleteFile(path.getName) }
      )
    } catch {
      case t: Throwable =>
        logError("Exception in parsing logging info from directory %s".format(logDir), t)
      EventLoggingInfo.empty
    }
  }

  /**
   * Parse the event logging information associated with the logs in the given directory.
   */
  def parseLoggingInfo(logDir: String, fileSystem: FileSystem): EventLoggingInfo = {
    parseLoggingInfo(new Path(logDir), fileSystem)
  }
}


/**
 * Information needed to process the event logs associated with an application.
 */
private[spark] case class EventLoggingInfo(
    logPaths: Seq[Path],
    sparkVersion: String,
    compressionCodec: Option[CompressionCodec],
    applicationComplete: Boolean = false)

private[spark] object EventLoggingInfo {
  def empty = EventLoggingInfo(Seq[Path](), "<Unknown>", None, applicationComplete = false)
}
