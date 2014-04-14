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

import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, FileLogger}

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

  private val shouldCompress = conf.getBoolean("spark.eventLog.compress", false)
  private val shouldOverwrite = conf.getBoolean("spark.eventLog.overwrite", false)
  private val outputBufferSize = conf.getInt("spark.eventLog.buffer.kb", 100) * 1024
  private val logBaseDir = conf.get("spark.eventLog.dir", "/tmp/spark-events").stripSuffix("/")
  private val name = appName.replaceAll("[ :/]", "-").toLowerCase + "-" + System.currentTimeMillis
  val logDir = logBaseDir + "/" + name

  private val logger =
    new FileLogger(logDir, conf, outputBufferSize, shouldCompress, shouldOverwrite)

  // Information needed to replay the events logged by this listener later
  val info = {
    val compressionCodec = if (shouldCompress) {
      Some(conf.get("spark.io.compression.codec", CompressionCodec.DEFAULT_COMPRESSION_CODEC))
    } else None
    EventLoggingInfo(logDir, compressionCodec)
  }

  logInfo("Logging events to %s".format(logDir))

  /** Log the event as JSON */
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

  def stop() = logger.stop()
}

// If compression is not enabled, compressionCodec is None
private[spark] case class EventLoggingInfo(logDir: String, compressionCodec: Option[String])
