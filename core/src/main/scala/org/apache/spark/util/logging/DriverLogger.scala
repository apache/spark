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

package org.apache.spark.util.logging

import java.io._
import java.util.concurrent.TimeUnit

import scala.util.{Failure, Try}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.log4j.{FileAppender => Log4jFileAppender, _}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class DriverLogger(conf: SparkConf) extends Logging {

  private val UPLOAD_CHUNK_SIZE = 1024 * 1024
  private val UPLOAD_INTERVAL_IN_SECS = 5
  private val DRIVER_LOG_FILE = "driver.log"
  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  private var localLogFile: String = FileUtils.getFile(
    Utils.getLocalDir(conf), "driver_logs", DRIVER_LOG_FILE).getAbsolutePath()
  // Visible for testing
  private[spark] var writer: Option[DfsAsyncWriter] = None

  addLogAppender()

  private def addLogAppender(): Unit = {
    val appenders = LogManager.getRootLogger().getAllAppenders()
    val layout = if (conf.contains(DRIVER_LOG_LAYOUT)) {
      new PatternLayout(conf.get(DRIVER_LOG_LAYOUT))
    } else if (appenders.hasMoreElements()) {
      appenders.nextElement().asInstanceOf[Appender].getLayout()
    } else {
      new PatternLayout(DRIVER_LOG_LAYOUT.defaultValueString)
    }
    val fa = new Log4jFileAppender(layout, localLogFile)
    fa.setName(DriverLogger.APPENDER_NAME)
    LogManager.getRootLogger().addAppender(fa)
    logInfo(s"Added a local log appender at: ${localLogFile}")
  }

  def startSync(hadoopConf: Configuration): Unit = {
    try {
      // Setup a writer which moves the local file to hdfs continuously
      val appId = Utils.sanitizeDirName(conf.getAppId)
      writer = Some(new DfsAsyncWriter(appId, hadoopConf))
    } catch {
      case e: Exception =>
        logError(s"Could not sync driver logs to spark dfs", e)
    }
  }

  def stop(): Unit = {
    try {
      LogManager.getRootLogger().removeAppender(DriverLogger.APPENDER_NAME)
      writer.foreach(_.closeWriter())
    } catch {
      case e: Exception =>
        logError(s"Error in persisting driver logs", e)
    } finally {
      Utils.tryLogNonFatalError(JavaUtils.deleteRecursively(
        FileUtils.getFile(localLogFile).getParentFile()))
    }
  }

  // Visible for testing
  private[spark] class DfsAsyncWriter(appId: String, hadoopConf: Configuration) extends Runnable
    with Logging {

    private var streamClosed = false
    private val fileSystem: FileSystem = FileSystem.get(hadoopConf)
    private val dfsLogFile: String = {
      val rootDir = conf.get(DRIVER_LOG_DFS_DIR).get.split(",").head
      if (!fileSystem.exists(new Path(rootDir))) {
        throw new RuntimeException(s"${rootDir} does not exist." +
          s" Please create this dir in order to sync driver logs")
      }
      FileUtils.getFile(rootDir, appId, DRIVER_LOG_FILE).getAbsolutePath()
    }
    private var inStream: InputStream = null
    private var outputStream: FSDataOutputStream = null
    try {
      inStream = new BufferedInputStream(new FileInputStream(localLogFile))
      outputStream = fileSystem.create(new Path(dfsLogFile), true)
      fileSystem.setPermission(new Path(dfsLogFile), LOG_FILE_PERMISSIONS)
    } catch {
      case e: Exception =>
        if (inStream != null) {
          Utils.tryLogNonFatalError(inStream.close())
        }
        if (outputStream != null) {
          Utils.tryLogNonFatalError(outputStream.close())
        }
        throw e
    }
    private val tmpBuffer = new Array[Byte](UPLOAD_CHUNK_SIZE)
    private val threadpool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("dfsSyncThread")
    threadpool.scheduleWithFixedDelay(this, UPLOAD_INTERVAL_IN_SECS, UPLOAD_INTERVAL_IN_SECS,
      TimeUnit.SECONDS)
    logInfo(s"The local driver log file is being synced to spark dfs at: ${dfsLogFile}")

    def run(): Unit = {
      if (streamClosed) {
        return
      }
      try {
        var remaining = inStream.available()
        while (remaining > 0) {
          val read = inStream.read(tmpBuffer, 0, math.min(remaining, UPLOAD_CHUNK_SIZE))
          outputStream.write(tmpBuffer, 0, read)
          remaining -= read
        }
        outputStream.hflush()
      } catch {
        case e: Exception => logError("Failed to write to spark dfs", e)
      }
    }

    def close(): Unit = {
      if (streamClosed) {
        return
      }
      try {
        // Write all remaining bytes
        run()
      } finally {
        try {
          streamClosed = true
          inStream.close()
          outputStream.close()
        } catch {
          case t: Throwable =>
            logError("Error in closing driver log input/output stream", t)
        }
      }
    }

    def closeWriter(): Unit = {
      try {
        threadpool.execute(new Runnable() {
          override def run(): Unit = DfsAsyncWriter.this.close()
        })
        threadpool.shutdown()
        threadpool.awaitTermination(1, TimeUnit.MINUTES)
      } catch {
        case e: Exception =>
          logError("Error in shutting down threadpool", e)
      }
    }
  }

}

private[spark] object DriverLogger extends Logging {
  val APPENDER_NAME = "_DriverLogAppender"

  def apply(conf: SparkConf): Option[DriverLogger] = {
    if (conf.get(DRIVER_LOG_SYNCTODFS)
      && conf.get(DRIVER_LOG_DFS_DIR).isDefined
      && Utils.isClientMode(conf)) {
      Try[DriverLogger] { new DriverLogger(conf) }
        .recoverWith {
          case t: Throwable =>
            logError("Could not add driver logger", t)
            Failure(t)
        }.toOption
    } else {
      None
    }
  }
}
