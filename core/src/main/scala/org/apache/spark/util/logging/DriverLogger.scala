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
import org.apache.hadoop.security.{AccessControlException, UserGroupInformation}
import org.apache.hadoop.yarn.conf.YarnConfiguration
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
  private var hdfsLogFile: String = _
  private var writer: Option[HdfsAsyncWriter] = None
  private var hadoopConfiguration: Configuration = _
  private var fileSystem: FileSystem = _

  private val syncToDfs: Boolean = conf.get(DRIVER_LOG_SYNCTODFS)
  private val syncToYarn: Boolean = conf.get(DRIVER_LOG_SYNCTOYARN)

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
      val appId = Utils.sanitizeDirName(conf.getAppId)
      hdfsLogFile = {
        val rootDir = conf.get(DRIVER_LOG_DFS_DIR.key, "/tmp/driver_logs").split(",").head
        FileUtils.getFile(rootDir, appId, DRIVER_LOG_FILE).getAbsolutePath()
      }
      hadoopConfiguration = hadoopConf
      fileSystem = FileSystem.get(hadoopConf)

      // Setup a writer which moves the local file to hdfs continuously
      if (syncToDfs) {
        writer = Some(new HdfsAsyncWriter())
        logInfo(s"The local driver log file is being synced to spark dfs at: ${hdfsLogFile}")
      }
    } catch {
      case e: Exception =>
        logError(s"Could not sync driver logs to spark dfs", e)
    }
  }

  def stop(): Unit = {
    try {
      writer.map(_.closeWriter())
      if (syncToYarn) {
        moveToYarnAppDir()
      }
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

  private def moveToYarnAppDir(): Unit = {
    try {
      val appId = Utils.sanitizeDirName(conf.getAppId)
      val yarnConf = new YarnConfiguration(hadoopConfiguration)
      val rootDir = yarnConf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR)
      if (rootDir != null && !rootDir.isEmpty()) {
        val parentDir = FileUtils.getFile(
            rootDir,
            UserGroupInformation.getCurrentUser().getShortUserName(),
            "logs",
            appId)
        if (fileSystem.exists(new Path(parentDir.getAbsolutePath()))) {
          if (syncToDfs) {
            fileSystem.rename(new Path(hdfsLogFile), new Path(parentDir.getAbsolutePath()))
            fileSystem.delete(new Path(FileUtils.getFile(hdfsLogFile).getParent()), true)
            logInfo(
              s"Moved the driver log file to: ${parentDir.getAbsolutePath()}")
          } else {
            fileSystem.copyFromLocalFile(true,
              new Path(localLogFile),
              new Path(parentDir.getAbsolutePath()))
            logInfo(
              s"Moved the local driver log file to spark dfs at: ${parentDir.getAbsolutePath()}")
          }
        } else if (!syncToDfs) {
          // Move the file to spark dfs if yarn app dir does not exist and not syncing to dfs
          moveToDfs()
        }
      }
    } catch {
      case nse: NoSuchElementException =>
        logWarning("Couldn't move driver log to YARN application dir as no appId defined")
      case ace: AccessControlException =>
        if (!syncToDfs) {
          logWarning(s"Couldn't move local driver log to YARN application dir," +
            s" trying Spark application dir now")
          moveToDfs()
        } else {
          logError(s"Couldn't move driver log to YARN application dir", ace)
        }
      case e: Exception =>
        logError(s"Couldn't move driver log to YARN application dir", e)
    }
  }

  private def moveToDfs(): Unit = {
    try {
      fileSystem.copyFromLocalFile(true,
        new Path(localLogFile),
        new Path(hdfsLogFile))
      fileSystem.setPermission(new Path(hdfsLogFile), LOG_FILE_PERMISSIONS)
      logInfo(s"Moved the local driver log file to spark dfs at: ${hdfsLogFile}")
    } catch {
      case e: Exception =>
        logError(s"Could not move local driver log to spark dfs", e)
    }
  }

  private class HdfsAsyncWriter extends Runnable with Logging {

    private var streamClosed = false
    private val inStream = new BufferedInputStream(new FileInputStream(localLogFile))
    private val outputStream: FSDataOutputStream = fileSystem.create(new Path(hdfsLogFile), true)
    fileSystem.setPermission(new Path(hdfsLogFile), LOG_FILE_PERMISSIONS)
    private val tmpBuffer = new Array[Byte](UPLOAD_CHUNK_SIZE)
    private val threadpool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("dfsSyncThread")
    threadpool.scheduleWithFixedDelay(this, UPLOAD_INTERVAL_IN_SECS, UPLOAD_INTERVAL_IN_SECS,
      TimeUnit.SECONDS)

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
    if ((conf.get(DRIVER_LOG_SYNCTODFS) || conf.get(DRIVER_LOG_SYNCTOYARN))
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
