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

import java.io._
import java.net.URI

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FSDataOutputStream}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, JsonGroupProtocol, Utils}
import org.apache.spark._
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

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
private[spark] class EventLoggingGroupListener (
    appId: String,
    appAttemptId: Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SparkListener with EventLoggingWriterListener with Logging {

  import EventLoggingWriterListener._

  def this(appId: String, appAttemptId: Option[String], logBaseDir: URI, sparkConf: SparkConf) =
    this(appId, appAttemptId, logBaseDir, sparkConf,
      SparkHadoopUtil.get.newConfiguration(sparkConf))

  private val groupSize = sparkConf.getInt("spark.eventLog.group.size", 0)
  private val shouldCompress = sparkConf.getBoolean("spark.eventLog.compress", false)
  private val shouldOverwrite = sparkConf.getBoolean("spark.eventLog.overwrite", false)
  private val outputBufferSize = sparkConf.getInt("spark.eventLog.buffer.kb", 100) * 1024
  private val fileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)
  private val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf))
    } else {
      None
    }
  private val compressionCodecName = compressionCodec.map { c =>
    CompressionCodec.getShortName(c.getClass.getName)
  }
  private val codec = compressionCodecName.map("." + _).getOrElse("")

  // The Hadoop APIs have changed over time, so we use reflection to figure out
  // the correct method to use to flush a hadoop data stream. See SPARK-1518
  // for details.
  private val hadoopFlushMethod = getHadoopFlushMethod

  // Only defined if the file system scheme is not local
  private var metaHadoopDataStream: Option[FSDataOutputStream] = None
  private var metaFilePath: String = null
  private var metaWriter: PrintWriter = null
  private val groupWriters = new mutable.HashMap[PrintWriter, EventLogGroupInfo]
  private var lastGroupWriter: PrintWriter = null
  private var lastGroupInfo: EventLogGroupInfo = null
  private var groupNum = 0

  private val logPath = getLogPath(logBaseDir, appId, appAttemptId)

  /**
   * Creates the meta file and the first part file in the configured log directory.
   */
  def start() {
    if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDir) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir does not exist.")
    }

    // Create meta file writer
    metaFilePath = logPath + "-meta" + codec
    val metaWriterAndStream = getFileWriter(metaFilePath, fileSystem, shouldOverwrite,
      outputBufferSize, hadoopConf, compressionCodec, true)
    metaWriter = metaWriterAndStream._1
    metaHadoopDataStream = metaWriterAndStream._2

    // Create the first group file writer and update the last group info
    updateLastGroupInfo()
  }

  def getLogPath(logBaseDir: URI, appId: String, appAttemptId: Option[String]): String = {
    val base = logBaseDir.toString.stripSuffix("/") + "/" + sanitize(appId)
    if (appAttemptId.isDefined) {
      base + "_" + sanitize(appAttemptId.get)
    } else {
      base
    }
  }

  /** Create a new group file and update the last groupWriter anf lastDataStream. */
  private def updateLastGroupInfo(): Unit = {
    groupNum += 1
    val groupPath = logPath + "-part" + groupNum + codec
    val writerAndStream = getFileWriter(groupPath, fileSystem, shouldOverwrite, outputBufferSize,
      hadoopConf, compressionCodec, false)
    lastGroupWriter = writerAndStream._1
    lastGroupInfo = new EventLogGroupInfo
    lastGroupInfo.hadoopDataStream = writerAndStream._2
    lastGroupInfo.filePath = groupPath
    groupWriters.put(writerAndStream._1, lastGroupInfo)
  }

  /** Log event as JSON. */
  private def logEvent(
      writer: Option[PrintWriter],
      hadoopDataStream: Option[FSDataOutputStream],
      json: JValue,
      flushLogger: Boolean = false) {
    // scalastyle:off println
    writer.foreach(_.println(compact(render(json))))
    // scalastyle:on println
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(hadoopFlushMethod.invoke(_))
    }
  }

  /** Log meta event as JSON to meta file. */
  private def logMetaEvent(json: JValue, flushLogger: Boolean = false): Unit = {
    // scalastyle:off println
    metaWriter.println(compact(render(json)))
    // scalastyle:on println
    if (flushLogger) {
      metaWriter.flush()
      metaHadoopDataStream.foreach(hadoopFlushMethod.invoke(_))
    }
  }

  /** Log job event as JSON to part file. */
  private def logJobEvent(stageId: Int, json: JValue, isFlush: Boolean = false): Unit = {
    var rightWriter: Option[PrintWriter] = None
    var hadoopDstream: Option[FSDataOutputStream] = None
    for (writer <- groupWriters) {
      for (stages <- writer._2.jobIdToStages.values) {
        if (stages.contains(stageId)) {
          rightWriter = Some(writer._1)
          hadoopDstream = writer._2.hadoopDataStream
        }
      }
    }

    logEvent(rightWriter, hadoopDstream, json, isFlush)
  }

  // Events that do not trigger a flush
  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val jsonAndStage = JsonGroupProtocol.stageSubmittedToJson(event)
    logJobEvent(jsonAndStage._2, jsonAndStage._1)
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    val jsonAndStage = JsonGroupProtocol.taskStartToJson(event)
    logJobEvent(jsonAndStage._2, jsonAndStage._1)
  }

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = {
    val jsonAndStage = JsonGroupProtocol.taskGettingResultToJson(event)
    logJobEvent(jsonAndStage._2, jsonAndStage._1)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val jsonAndStage = JsonGroupProtocol.taskEndToJson(event)
    logJobEvent(jsonAndStage._2, jsonAndStage._1)
  }

  // Events that trigger a flush
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val jsonAndStage = JsonGroupProtocol.stageCompletedToJson(event)
    logJobEvent(jsonAndStage._2, jsonAndStage._1, true)
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val jsonJobStage = JsonGroupProtocol.jobStartToJson(event)
    if (lastGroupInfo.jobNum >= groupSize) {
      updateLastGroupInfo()
    }
    logEvent(
      Some(lastGroupWriter), lastGroupInfo.hadoopDataStream, jsonJobStage._1, flushLogger = true)
    lastGroupInfo.jobNum += 1
    lastGroupInfo.jobIdToStages.put(jsonJobStage._2, jsonJobStage._3)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    val jsonAndJobId = JsonGroupProtocol.jobEndToJson(event)
    var jobWriter: Option[PrintWriter] = None
    var groupInfo: EventLogGroupInfo = null
    for (writer <- groupWriters) {
      if (writer._2.jobIdToStages.contains(jsonAndJobId._2)) {
        jobWriter = Some(writer._1)
        groupInfo = writer._2
      }
    }

    logEvent(jobWriter, groupInfo.hadoopDataStream, jsonAndJobId._1, flushLogger = true)
    groupInfo.completedJobNum += 1
    if (groupInfo.jobNum == groupSize && groupInfo.completedJobNum == groupSize) {
      jobWriter.foreach(_.close())
      groupWriters.remove(jobWriter.get)
      renameFile(fileSystem, shouldOverwrite, groupInfo.filePath)
    }
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    logMetaEvent(JsonProtocol.environmentUpdateToJson(event))
  }

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    logMetaEvent(JsonProtocol.blockManagerAddedToJson(event), flushLogger = true)
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    logMetaEvent(JsonProtocol.blockManagerRemovedToJson(event), flushLogger = true)
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    logMetaEvent(JsonProtocol.unpersistRDDToJson(event), flushLogger = true)
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    logMetaEvent(JsonProtocol.applicationStartToJson(event), flushLogger = true)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    logMetaEvent(JsonProtocol.applicationEndToJson(event), flushLogger = true)
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    logMetaEvent(JsonProtocol.executorAddedToJson(event), flushLogger = true)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    logMetaEvent(JsonProtocol.executorRemovedToJson(event), flushLogger = true)
  }

  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = { }

  /**
   * Stop logging events. The log file will be renamed so that it loses the
   * ".inprogress" suffix.
   */
  def stop(): Unit = {
    metaWriter.close()
    renameFile(fileSystem, shouldOverwrite, metaFilePath)
    for (groupWriter <- groupWriters) {
      groupWriter._1.close()
      renameFile(fileSystem, shouldOverwrite, groupWriter._2.filePath)
    }
  }

  override def getFilePath(): String = {
    logPath
  }

  override def getLoggedEvent: ArrayBuffer[JValue] = {
    null
  }
}

