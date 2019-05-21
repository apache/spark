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

package org.apache.spark.sql.hive.thriftserver

import java.io._
import java.net.URI
import java.util.{EnumSet, Locale}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.hdfs.DFSOutputStream
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart, SparkListenerSQLSessionEnd, SparkListenerSQLSessionStart}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.{JsonProtocol, Utils}

private[spark] class SessionAwareEventLoggingListener(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    sqlConf: SQLConf,
    hadoopConf: Configuration)
  extends SparkListener with Logging {

  import EventLoggingListener._

  def this(appId: String, appAttemptId: Option[String], logBaseDir: URI,
      sparkConf: SparkConf, sqlConf: SQLConf) =
    this(appId, appAttemptId, logBaseDir, sparkConf, sqlConf,
      SparkHadoopUtil.get.newConfiguration(sparkConf))

  private val shouldCompress = sqlConf.getConf(StaticSQLConf.HIVE_THRIFT_SERVER_EVENTLOG_COMPRESS)
  private val shouldOverwrite = sqlConf.getConf(StaticSQLConf.HIVE_THRIFT_SERVER_EVENTLOG_OVERWRITE)
  private val shouldLogStageExecutorMetrics =
    sqlConf.getConf(StaticSQLConf.HIVE_THRIFT_SERVER_EVENTLOG_STAGE_EXECUTOR_METRICS)
  private val testing = sqlConf.getConf(StaticSQLConf.HIVE_THRIFT_SERVER_EVENTLOG_TESTING)
  private val outputBufferSize =
    sqlConf.getConf(StaticSQLConf.HIVE_THRIFT_SERVER_EVENTLOG_OUTPUT_BUFFER_SIZE).toInt
  // should be synchronized
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
  // Only defined if the file system scheme is not local
  private val hadoopDataStreams = new mutable.HashMap[String, FSDataOutputStream]()
  private val writers = new mutable.HashMap[String, PrintWriter]()
  // For testing. Keep track of all JSON serialized events that have been logged.
  private val loggedEvents = new mutable.HashMap[String, ArrayBuffer[JValue]]()
  // Visible for tests only.
  private val logPaths = mutable.HashMap[String, String]()

  // map of (stageId, stageAttempt), to peak executor metrics for the stage
  private val liveStageExecutorMetrics = Map.empty[(Int, Int), Map[String, ExecutorMetrics]]

  private val executionIdToSessionId = new mutable.HashMap[String, String]()
  private val jobIdToSessionId = new mutable.HashMap[Int, String]
  private val stageIdToSessionId = new mutable.HashMap[(Int, Int), String]()

  // Events that will be write ahead of each file
  private var globalApplicationStart: SparkListenerApplicationStart = _
  private var globalEnvironmentUpdate: SparkListenerEnvironmentUpdate = _

  /**
   * Creates the log file in the configured log directory.
   */
  private def startSession(sessionId: String): Unit = this.synchronized {
    if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDirectory) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir is not a directory.")
    }
    val logPath = getLogPath(
      logBaseDir, appId, sessionId, appAttemptId, compressionCodecName)
    val workingPath = logPath + IN_PROGRESS
    val path = new Path(workingPath)
    val uri = path.toUri
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"
    if (shouldOverwrite && fileSystem.delete(path, true)) {
      logWarning(s"Event log $path already exists. Overwriting...")
    }
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        val fs = fileSystem.create(path)
        hadoopDataStreams.put(sessionId, fs)
        fs
      }
    try {
      val cstream = compressionCodec.map(_.compressedOutputStream(dstream)).getOrElse(dstream)
      val bstream = new BufferedOutputStream(cstream, outputBufferSize)
      val arrayBuffer = new ArrayBuffer[JValue]()
      loggedEvents(sessionId) = arrayBuffer
      initEventLog(bstream, testing, arrayBuffer)
      fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)
      writers(sessionId) = new PrintWriter(bstream)
      logInfo("Logging events to %s".format(logPaths))
    } catch {
      case e: Exception =>
        dstream.close()
        throw e
    }
    logPaths(sessionId) = logPath
    val sparkAppStart = SparkListenerApplicationStart(globalApplicationStart.appName,
      Some {
        if (globalApplicationStart.appId.isDefined) {
          globalApplicationStart.appId.get + "_" + sessionId
        } else {
          sessionId
        }
      },
      System.currentTimeMillis(),
      globalApplicationStart.sparkUser,
      globalApplicationStart.appAttemptId,
      globalApplicationStart.driverLogs
    )
    logEvent(sparkAppStart, flushLogger = true, sessionId)
    logEvent(redactEvent(globalEnvironmentUpdate), flushLogger = true, sessionId)
  }

  /** Log the event as JSON. */
  private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false, sessionId: String) {
    val eventJson = JsonProtocol.sparkEventToJson(event)
    // scalastyle:off println
    writers.get(sessionId).foreach(_.println(compact(render(eventJson))))
    // scalastyle:on println
    if (flushLogger) {
      writers.get(sessionId).foreach(_.flush())
      hadoopDataStreams.get(sessionId).foreach(ds => ds.getWrappedStream match {
        case wrapped: DFSOutputStream => wrapped.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH))
        case _ => ds.hflush()
      })
    }
    if (testing) {
      loggedEvents.get(sessionId).foreach(_ += eventJson)
    }
  }

  protected def withSession(event: SparkListenerEvent)(f: String => Unit): Unit = {
    val s = event match {
      case e: SparkListenerJobStart =>
        val executionId = e.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
        executionIdToSessionId.get(executionId)
      case e: SparkListenerJobEnd =>
        jobIdToSessionId.get(e.jobId)
      case e: SparkListenerStageSubmitted =>
        val executionId = e.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
        executionIdToSessionId.get(executionId)
      case e: SparkListenerStageCompleted =>
        stageIdToSessionId.get((e.stageInfo.stageId, e.stageInfo.attemptNumber()))
      case e: SparkListenerTaskStart =>
        stageIdToSessionId.get((e.stageId, e.stageAttemptId))
      case e: SparkListenerTaskEnd =>
        stageIdToSessionId.get((e.stageId, e.stageAttemptId))
      case _ =>
        None

    }
    s.foreach(f(_))
  }

  // Events that be handled only once
  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    globalApplicationStart = event
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    globalEnvironmentUpdate = event
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = withSession(event) { sessionId =>
    jobIdToSessionId(event.jobId) = sessionId
    logEvent(event, flushLogger = true, sessionId)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = withSession(event) { sessionId =>
    jobIdToSessionId.remove(event.jobId)
    logEvent(event, flushLogger = true, sessionId)
  }

  override def onStageSubmitted(
      event: SparkListenerStageSubmitted): Unit = withSession(event) { sessionId =>
    stageIdToSessionId((event.stageInfo.stageId, event.stageInfo.attemptNumber())) = sessionId
    logEvent(event, flushLogger = false, sessionId)
    if (shouldLogStageExecutorMetrics) {
      // record the peak metrics for the new stage
      liveStageExecutorMetrics.put((event.stageInfo.stageId, event.stageInfo.attemptNumber()),
        Map.empty[String, ExecutorMetrics])
    }
  }

  override def onStageCompleted(
      event: SparkListenerStageCompleted): Unit = withSession(event) { sessionId =>
    stageIdToSessionId.remove((event.stageInfo.stageId, event.stageInfo.attemptNumber()))
    if (shouldLogStageExecutorMetrics) {
      // clear out any previous attempts, that did not have a stage completed event
      val prevAttemptId = event.stageInfo.attemptNumber() - 1
      for (attemptId <- 0 to prevAttemptId) {
        liveStageExecutorMetrics.remove((event.stageInfo.stageId, attemptId))
      }

      // log the peak executor metrics for the stage, for each live executor,
      // whether or not the executor is running tasks for the stage
      val executorOpt = liveStageExecutorMetrics.remove(
        (event.stageInfo.stageId, event.stageInfo.attemptNumber()))
      executorOpt.foreach { execMap =>
        execMap.foreach { case (executorId, peakExecutorMetrics) =>
          logEvent(SparkListenerStageExecutorMetrics(executorId, event.stageInfo.stageId,
            event.stageInfo.attemptNumber(), peakExecutorMetrics), flushLogger = false, sessionId)
        }
      }
    }
    logEvent(event, flushLogger = true, sessionId)
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = withSession(event) { sessionId =>
    logEvent(event, flushLogger = false, sessionId)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = withSession(event) { sessionId =>
    logEvent(event, flushLogger = false, sessionId)
  }

  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case execStart: SparkListenerSQLExecutionStart =>
        if (execStart.sessionId.isDefined) {
          executionIdToSessionId(String.valueOf(execStart.executionId)) = execStart.sessionId.get
          logEvent(event, flushLogger = true, execStart.sessionId.get)
        }
      case execEnd: SparkListenerSQLExecutionEnd =>
        executionIdToSessionId.get(String.valueOf(execEnd.executionId)) match {
          case Some(sessionId) =>
            executionIdToSessionId.remove(String.valueOf(execEnd.executionId))
            logEvent(event, flushLogger = true, sessionId)
          case None =>
            logInfo(s"Execution ${execEnd.executionId} doesn't belong to any session")
        }
      case sessionStart: SparkListenerSQLSessionStart =>
        startSession(sessionStart.sessionId)
      case sessionEnd: SparkListenerSQLSessionEnd =>
        stopSession(sessionEnd.sessionId)
      case _ =>
    }
  }

  private def stopSession(sessionId: String): Unit = {
    logEvent(
      SparkListenerApplicationEnd(System.currentTimeMillis()), flushLogger = true, sessionId)
    try {
      writers.get(sessionId).foreach(_.close())
      val target = new Path(logPaths(sessionId))
      if (fileSystem.exists(target)) {
        if (shouldOverwrite) {
          logWarning(s"Event log $target already exists. Overwriting...")
          if (!fileSystem.delete(target, true)) {
            logWarning(s"Error deleting $target")
          }
        } else {
          throw new IOException("Target log file already exists (%s)".format(logPaths(sessionId)))
        }
      }
      fileSystem.rename(new Path(logPaths(sessionId) + IN_PROGRESS), target)
      try {
        fileSystem.setTimes(target, System.currentTimeMillis(), -1)
      } catch {
        case e: Exception => logDebug(s"failed to set time of $target", e)
      }
    } finally {
      hadoopDataStreams.remove(sessionId).foreach(_.close())
      writers.remove(sessionId).foreach(_.close())
      logPaths.remove(sessionId)
      loggedEvents.remove(sessionId)
    }
  }

  def getLogPath(
      logBaseDir: URI,
      appId: String,
      sessionId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String] = None): String = {
    val base = new Path(logBaseDir).toString.stripSuffix("/") + "/" + sanitize(appId)
    val codec = compressionCodecName.map("." + _).getOrElse("")
    val attempt = if (appAttemptId.isDefined) {
      base + "_" + sanitize(appAttemptId.get) + codec
    } else {
      base + codec
    }
    attempt + "_" + sessionId
  }

  private def sanitize(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase(Locale.ROOT)
  }

  private[spark] def redactEvent(
      event: SparkListenerEnvironmentUpdate): SparkListenerEnvironmentUpdate = {
    val redactedProps = event.environmentDetails.map{ case (name, props) =>
      name -> Utils.redact(sparkConf, props)
    }
    SparkListenerEnvironmentUpdate(redactedProps)
  }

  def stop(): Unit = {
    logPaths.keysIterator.foreach(stopSession)
  }
}
