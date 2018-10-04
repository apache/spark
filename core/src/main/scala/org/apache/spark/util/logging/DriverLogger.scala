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
  private var writer: Option[HdfsAsyncWriter] = None
  private val syncToDfs: Boolean = conf.get(DRIVER_LOG_SYNCTODFS)

  addLogAppender()

  private def addLogAppender(): Unit = {
    val appenders = LogManager.getRootLogger().getAllAppenders()
    val layout = if (appenders.hasMoreElements()) {
      appenders.nextElement().asInstanceOf[Appender].getLayout()
    } else {
      new PatternLayout(conf.get(DRIVER_LOG_LAYOUT))
    }
    val fa = new Log4jFileAppender(layout, localLogFile)
    fa.setName(DriverLogger.APPENDER_NAME)
    LogManager.getRootLogger().addAppender(fa)
    logInfo(s"Added a local log appender at: ${localLogFile}")
  }

  def startSync(hadoopConf: Configuration): Unit = {
    try {
      // Setup a writer which moves the local file to hdfs continuously
      if (syncToDfs) {
        val appId = Utils.sanitizeDirName(conf.getAppId)
        writer = Some(new HdfsAsyncWriter(appId, hadoopConf))
      }
    } catch {
      case e: Exception =>
        logError(s"Could not sync driver logs to spark dfs", e)
    }
  }

  def stop(): Unit = {
    try {
      writer.map(_.closeWriter())
    } catch {
      case e: Exception =>
        logError(s"Error in persisting driver logs", e)
    } finally {
      try {
        JavaUtils.deleteRecursively(FileUtils.getFile(localLogFile).getParentFile())
      } catch {
        case e: Exception =>
          logError(s"Error in deleting local driver log dir", e)
      }
    }
  }

  private class HdfsAsyncWriter(appId: String, hadoopConf: Configuration) extends Runnable
    with Logging {

    private var streamClosed = false
    private val inStream = new BufferedInputStream(new FileInputStream(localLogFile))
    private var hdfsLogFile: String = {
      val rootDir = conf.get(DRIVER_LOG_DFS_DIR.key, "/tmp/driver_logs").split(",").head
      FileUtils.getFile(rootDir, appId, DRIVER_LOG_FILE).getAbsolutePath()
    }
    private var fileSystem: FileSystem = FileSystem.get(hadoopConf)
    private val outputStream: FSDataOutputStream = fileSystem.create(new Path(hdfsLogFile), true)
    fileSystem.setPermission(new Path(hdfsLogFile), LOG_FILE_PERMISSIONS)
    private val tmpBuffer = new Array[Byte](UPLOAD_CHUNK_SIZE)
    private val threadpool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("dfsSyncThread")
    threadpool.scheduleWithFixedDelay(this, UPLOAD_INTERVAL_IN_SECS, UPLOAD_INTERVAL_IN_SECS,
      TimeUnit.SECONDS)
    logInfo(s"The local driver log file is being synced to spark dfs at: ${hdfsLogFile}")

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
      try {
        // Write all remaining bytes
        run()
      } finally {
        inStream.close()
        outputStream.close()
        streamClosed = true
      }
    }

    def closeWriter(): Unit = {
      try {
        threadpool.execute(new Runnable() {
          def run(): Unit = HdfsAsyncWriter.this.close()
        })
        threadpool.shutdown()
        threadpool.awaitTermination(1, TimeUnit.MINUTES)
      } catch {
        case e: Exception =>
          logError("Error in closing driver log input/output stream", e)
      }
    }
  }

}

private[spark] object DriverLogger extends Logging {
  val APPENDER_NAME = "_DriverLogAppender"

  def apply(conf: SparkConf): Option[DriverLogger] = {
    if (conf.get(DRIVER_LOG_SYNCTODFS) && Utils.isClientMode(conf)) {
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
