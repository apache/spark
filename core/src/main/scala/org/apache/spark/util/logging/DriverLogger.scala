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
import java.util.EnumSet
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream
import org.apache.logging.log4j._
import org.apache.logging.log4j.core.Logger
import org.apache.logging.log4j.core.appender.{FileAppender => Log4jFileAppender}
import org.apache.logging.log4j.core.layout.PatternLayout

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class DriverLogger(conf: SparkConf) extends Logging {

  private val UPLOAD_CHUNK_SIZE = 1024 * 1024
  private val UPLOAD_INTERVAL_IN_SECS = 5
  private val DEFAULT_LAYOUT = "%d{yy/MM/dd HH:mm:ss.SSS} %t %p %c{1}: %m%n%ex"
  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  private val localLogFile: String = FileUtils.getFile(
    Utils.getLocalDir(conf),
    DriverLogger.DRIVER_LOG_DIR,
    DriverLogger.DRIVER_LOG_FILE).getAbsolutePath()
  private var writer: Option[DfsAsyncWriter] = None

  addLogAppender()

  private def addLogAppender(): Unit = {
    val logger = LogManager.getRootLogger().asInstanceOf[Logger]
    val layout = if (conf.contains(DRIVER_LOG_LAYOUT)) {
      PatternLayout.newBuilder().withPattern(conf.get(DRIVER_LOG_LAYOUT).get).build()
    } else {
      PatternLayout.newBuilder().withPattern(DEFAULT_LAYOUT).build()
    }
    val config = logger.getContext.getConfiguration()
    def log4jFileAppender() = {
      // SPARK-37853: We can't use the chained API invocation mode because
      // `AbstractFilterable.Builder.asBuilder()` method will return `Any` in Scala.
      val builder: Log4jFileAppender.Builder[_] = Log4jFileAppender.newBuilder()
      builder.withAppend(false)
      builder.setBufferedIo(false)
      builder.setConfiguration(config)
      builder.withFileName(localLogFile)
      builder.setIgnoreExceptions(false)
      builder.setLayout(layout)
      builder.setName(DriverLogger.APPENDER_NAME)
      builder.build()
    }
    val fa = log4jFileAppender()
    logger.addAppender(fa)
    fa.start()
    logInfo(s"Added a local log appender at: $localLogFile")
  }

  def startSync(hadoopConf: Configuration): Unit = {
    try {
      // Setup a writer which moves the local file to hdfs continuously
      val appId = Utils.sanitizeDirName(conf.getAppId)
      writer = Some(new DfsAsyncWriter(appId, hadoopConf))
    } catch {
      case e: Exception =>
        logError(s"Could not persist driver logs to dfs", e)
    }
  }

  def stop(): Unit = {
    try {
      val logger = LogManager.getRootLogger().asInstanceOf[Logger]
      val fa = logger.getAppenders.get(DriverLogger.APPENDER_NAME)
      logger.removeAppender(fa)
      Utils.tryLogNonFatalError(fa.stop())
      writer.foreach(_.closeWriter())
    } catch {
      case e: Exception =>
        logError(s"Error in persisting driver logs", e)
    } finally {
      Utils.tryLogNonFatalError {
        JavaUtils.deleteRecursively(FileUtils.getFile(localLogFile).getParentFile())
      }
    }
  }

  // Visible for testing
  private[spark] class DfsAsyncWriter(appId: String, hadoopConf: Configuration) extends Runnable
      with Logging {

    private var streamClosed = false
    private var inStream: InputStream = null
    private var outputStream: FSDataOutputStream = null
    private val tmpBuffer = new Array[Byte](UPLOAD_CHUNK_SIZE)
    private var threadpool: ScheduledExecutorService = _
    init()

    private def init(): Unit = {
      val rootDir = conf.get(DRIVER_LOG_DFS_DIR).get
      val fileSystem: FileSystem = new Path(rootDir).getFileSystem(hadoopConf)
      if (!fileSystem.exists(new Path(rootDir))) {
        throw new RuntimeException(s"${rootDir} does not exist." +
          s" Please create this dir in order to persist driver logs")
      }
      val dfsLogFile: Path = fileSystem.makeQualified(new Path(rootDir, appId
        + DriverLogger.DRIVER_LOG_FILE_SUFFIX))
      try {
        inStream = new BufferedInputStream(new FileInputStream(localLogFile))
        outputStream = SparkHadoopUtil.createFile(fileSystem, dfsLogFile,
          conf.get(DRIVER_LOG_ALLOW_EC))
        fileSystem.setPermission(dfsLogFile, LOG_FILE_PERMISSIONS)
      } catch {
        case e: Exception =>
          JavaUtils.closeQuietly(inStream)
          JavaUtils.closeQuietly(outputStream)
          throw e
      }
      threadpool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("dfsSyncThread")
      threadpool.scheduleWithFixedDelay(this, UPLOAD_INTERVAL_IN_SECS, UPLOAD_INTERVAL_IN_SECS,
        TimeUnit.SECONDS)
      logInfo(s"Started driver log file sync to: ${dfsLogFile}")
    }

    def run(): Unit = {
      if (streamClosed) {
        return
      }
      try {
        var remaining = inStream.available()
        val hadData = remaining > 0
        while (remaining > 0) {
          val read = inStream.read(tmpBuffer, 0, math.min(remaining, UPLOAD_CHUNK_SIZE))
          outputStream.write(tmpBuffer, 0, read)
          remaining -= read
        }
        if (hadData) {
          outputStream match {
            case hdfsStream: HdfsDataOutputStream =>
              hdfsStream.hsync(EnumSet.allOf(classOf[HdfsDataOutputStream.SyncFlag]))
            case other =>
              other.hflush()
          }
        }
      } catch {
        case e: Exception => logError("Failed writing driver logs to dfs", e)
      }
    }

    private def close(): Unit = {
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
          case e: Exception =>
            logError("Error in closing driver log input/output stream", e)
        }
      }
    }

    def closeWriter(): Unit = {
      try {
        threadpool.execute(() => DfsAsyncWriter.this.close())
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
  val DRIVER_LOG_DIR = "__driver_logs__"
  val DRIVER_LOG_FILE = "driver.log"
  val DRIVER_LOG_FILE_SUFFIX = "_" + DRIVER_LOG_FILE
  val APPENDER_NAME = "_DriverLogAppender"

  def apply(conf: SparkConf): Option[DriverLogger] = {
    if (conf.get(DRIVER_LOG_PERSISTTODFS) && Utils.isClientMode(conf)) {
      if (conf.contains(DRIVER_LOG_DFS_DIR)) {
        try {
          Some(new DriverLogger(conf))
        } catch {
          case e: Exception =>
            logError("Could not add driver logger", e)
            None
        }
      } else {
        logWarning(s"Driver logs are not persisted because" +
          s" ${DRIVER_LOG_DFS_DIR.key} is not configured")
        None
      }
    } else {
      None
    }
  }
}
