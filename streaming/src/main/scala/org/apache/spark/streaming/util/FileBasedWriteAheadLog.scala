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
package org.apache.spark.streaming.util

import java.nio.ByteBuffer
import java.util.{Iterator => JIterator}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.util.ThreadUtils
import org.apache.spark.{Logging, SparkConf}

/**
 * This class manages write ahead log files.
 * - Writes records (bytebuffers) to periodically rotating log files.
 * - Recovers the log files and the reads the recovered records upon failures.
 * - Cleans up old log files.
 *
 * Uses [[org.apache.spark.streaming.util.FileBasedWriteAheadLogWriter]] to write
 * and [[org.apache.spark.streaming.util.FileBasedWriteAheadLogReader]] to read.
 *
 * @param logDirectory Directory when rotating log files will be created.
 * @param hadoopConf Hadoop configuration for reading/writing log files.
 */
private[streaming] class FileBasedWriteAheadLog(
    conf: SparkConf,
    logDirectory: String,
    hadoopConf: Configuration,
    rollingIntervalSecs: Int,
    maxFailures: Int
  ) extends WriteAheadLog with Logging {

  import FileBasedWriteAheadLog._

  private val pastLogs = new ArrayBuffer[LogInfo]
  private val callerNameTag = getCallerName.map(c => s" for $c").getOrElse("")

  private val threadpoolName = s"WriteAheadLogManager $callerNameTag"
  implicit private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor(threadpoolName))
  override protected val logName = s"WriteAheadLogManager $callerNameTag"

  private var currentLogPath: Option[String] = None
  private var currentLogWriter: FileBasedWriteAheadLogWriter = null
  private var currentLogWriterStartTime: Long = -1L
  private var currentLogWriterStopTime: Long = -1L

  initializeOrRecover()

  /**
   * Write a byte buffer to the log file. This method synchronously writes the data in the
   * ByteBuffer to HDFS. When this method returns, the data is guaranteed to have been flushed
   * to HDFS, and will be available for readers to read.
   */
  def write(byteBuffer: ByteBuffer, time: Long): FileBasedWriteAheadLogSegment = synchronized {
    var fileSegment: FileBasedWriteAheadLogSegment = null
    var failures = 0
    var lastException: Exception = null
    var succeeded = false
    while (!succeeded && failures < maxFailures) {
      try {
        fileSegment = getLogWriter(time).write(byteBuffer)
        succeeded = true
      } catch {
        case ex: Exception =>
          lastException = ex
          logWarning("Failed to write to write ahead log")
          resetWriter()
          failures += 1
      }
    }
    if (fileSegment == null) {
      logError(s"Failed to write to write ahead log after $failures failures")
      throw lastException
    }
    fileSegment
  }

  def read(segment: WriteAheadLogRecordHandle): ByteBuffer = {
    val fileSegment = segment.asInstanceOf[FileBasedWriteAheadLogSegment]
    var reader: FileBasedWriteAheadLogRandomReader = null
    var byteBuffer: ByteBuffer = null
    try {
      reader = new FileBasedWriteAheadLogRandomReader(fileSegment.path, hadoopConf)
      byteBuffer = reader.read(fileSegment)
    } finally {
      reader.close()
    }
    byteBuffer
  }

  /**
   * Read all the existing logs from the log directory.
   *
   * Note that this is typically called when the caller is initializing and wants
   * to recover past state from the write ahead logs (that is, before making any writes).
   * If this is called after writes have been made using this manager, then it may not return
   * the latest the records. This does not deal with currently active log files, and
   * hence the implementation is kept simple.
   */
  def readAll(): JIterator[ByteBuffer] = synchronized {
    import scala.collection.JavaConversions._
    val logFilesToRead = pastLogs.map{ _.path} ++ currentLogPath
    logInfo("Reading from the logs: " + logFilesToRead.mkString("\n"))

    logFilesToRead.iterator.map { file =>
      logDebug(s"Creating log reader with $file")
      new FileBasedWriteAheadLogReader(file, hadoopConf)
    } flatMap { x => x }
  }

  /**
   * Delete the log files that are older than the threshold time.
   *
   * Its important to note that the threshold time is based on the time stamps used in the log
   * files, which is usually based on the local system time. So if there is coordination necessary
   * between the node calculating the threshTime (say, driver node), and the local system time
   * (say, worker node), the caller has to take account of possible time skew.
   *
   * If waitForCompletion is set to true, this method will return only after old logs have been
   * deleted. This should be set to true only for testing. Else the files will be deleted
   * asynchronously.
   */
  def clean(threshTime: Long, waitForCompletion: Boolean): Unit = {
    val oldLogFiles = synchronized { pastLogs.filter { _.endTime < threshTime } }
    logInfo(s"Attempting to clear ${oldLogFiles.size} old log files in $logDirectory " +
      s"older than $threshTime: ${oldLogFiles.map { _.path }.mkString("\n")}")

    def deleteFiles() {
      oldLogFiles.foreach { logInfo =>
        try {
          val path = new Path(logInfo.path)
          val fs = HdfsUtils.getFileSystemForPath(path, hadoopConf)
          fs.delete(path, true)
          synchronized { pastLogs -= logInfo }
          logDebug(s"Cleared log file $logInfo")
        } catch {
          case ex: Exception =>
            logWarning(s"Error clearing write ahead log file $logInfo", ex)
        }
      }
      logInfo(s"Cleared log files in $logDirectory older than $threshTime")
    }
    if (!executionContext.isShutdown) {
      val f = Future { deleteFiles() }
      if (waitForCompletion) {
        import scala.concurrent.duration._
        Await.ready(f, 1 second)
      }
    }
  }


  /** Stop the manager, close any open log writer */
  def close(): Unit = synchronized {
    if (currentLogWriter != null) {
      currentLogWriter.close()
    }
    executionContext.shutdown()
    logInfo("Stopped write ahead log manager")
  }

  /** Get the current log writer while taking care of rotation */
  private def getLogWriter(currentTime: Long): FileBasedWriteAheadLogWriter = synchronized {
    if (currentLogWriter == null || currentTime > currentLogWriterStopTime) {
      resetWriter()
      currentLogPath.foreach {
        pastLogs += LogInfo(currentLogWriterStartTime, currentLogWriterStopTime, _)
      }
      currentLogWriterStartTime = currentTime
      currentLogWriterStopTime = currentTime + (rollingIntervalSecs * 1000)
      val newLogPath = new Path(logDirectory,
        timeToLogFile(currentLogWriterStartTime, currentLogWriterStopTime))
      currentLogPath = Some(newLogPath.toString)
      currentLogWriter = new FileBasedWriteAheadLogWriter(currentLogPath.get, hadoopConf)
    }
    currentLogWriter
  }

  /** Initialize the log directory or recover existing logs inside the directory */
  private def initializeOrRecover(): Unit = synchronized {
    val logDirectoryPath = new Path(logDirectory)
    val fileSystem =  HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)

    if (fileSystem.exists(logDirectoryPath) && fileSystem.getFileStatus(logDirectoryPath).isDir) {
      val logFileInfo = logFilesTologInfo(fileSystem.listStatus(logDirectoryPath).map { _.getPath })
      pastLogs.clear()
      pastLogs ++= logFileInfo
      logInfo(s"Recovered ${logFileInfo.size} write ahead log files from $logDirectory")
      logDebug(s"Recovered files are:\n${logFileInfo.map(_.path).mkString("\n")}")
    }
  }

  private def resetWriter(): Unit = synchronized {
    if (currentLogWriter != null) {
      currentLogWriter.close()
      currentLogWriter = null
    }
  }
}

private[streaming] object FileBasedWriteAheadLog {

  case class LogInfo(startTime: Long, endTime: Long, path: String)

  val logFileRegex = """log-(\d+)-(\d+)""".r

  def timeToLogFile(startTime: Long, stopTime: Long): String = {
    s"log-$startTime-$stopTime"
  }

  def getCallerName(): Option[String] = {
    val stackTraceClasses = Thread.currentThread.getStackTrace().map(_.getClassName)
    stackTraceClasses.find(!_.contains("WriteAheadLog")).flatMap(_.split(".").lastOption)
  }

  /** Convert a sequence of files to a sequence of sorted LogInfo objects */
  def logFilesTologInfo(files: Seq[Path]): Seq[LogInfo] = {
    files.flatMap { file =>
      logFileRegex.findFirstIn(file.getName()) match {
        case Some(logFileRegex(startTimeStr, stopTimeStr)) =>
          val startTime = startTimeStr.toLong
          val stopTime = stopTimeStr.toLong
          Some(LogInfo(startTime, stopTime, file.toString))
        case None =>
          None
      }
    }.sortBy { _.startTime }
  }
}
