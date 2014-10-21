package org.apache.spark.streaming.storage

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.Logging
import org.apache.spark.streaming.storage.WriteAheadLogManager._
import org.apache.spark.streaming.util.{Clock, SystemClock}
import org.apache.spark.util.Utils

private[streaming] class WriteAheadLogManager(
    logDirectory: String,
    hadoopConf: Configuration,
    rollingIntervalSecs: Int = 60,
    maxFailures: Int = 3,
    callerName: String = "",
    clock: Clock = new SystemClock
  ) extends Logging {

  private val pastLogs = new ArrayBuffer[LogInfo]
  private val callerNameTag =
    if (callerName != null && callerName.nonEmpty) s" for $callerName" else ""
  private val threadpoolName = s"WriteAheadLogManager $callerNameTag"
  implicit private val executionContext = ExecutionContext.fromExecutorService(
    Utils.newDaemonFixedThreadPool(1, threadpoolName))
  override protected val logName = s"WriteAheadLogManager $callerNameTag"

  private var currentLogPath: String = null
  private var currentLogWriter: WriteAheadLogWriter = null
  private var currentLogWriterStartTime: Long = -1L
  private var currentLogWriterStopTime: Long = -1L

  initializeOrRecover()

  def writeToLog(byteBuffer: ByteBuffer): FileSegment = synchronized {
    var fileSegment: FileSegment = null
    var failures = 0
    var lastException: Exception = null
    var succeeded = false
    while (!succeeded && failures < maxFailures) {
      try {
        fileSegment = getLogWriter(clock.currentTime).write(byteBuffer)
        succeeded = true
      } catch {
        case ex: Exception =>
          lastException = ex
          logWarning("Failed to ...")
          resetWriter()
          failures += 1
      }
    }
    if (fileSegment == null) {
      throw lastException
    }
    fileSegment
  }

  def readFromLog(): Iterator[ByteBuffer] = synchronized {
    val logFilesToRead = pastLogs.map{ _.path} ++ Option(currentLogPath)
    logInfo("Reading from the logs: " + logFilesToRead.mkString("\n"))
    logFilesToRead.iterator.map { file =>
        logDebug(s"Creating log reader with $file")
        new WriteAheadLogReader(file, hadoopConf)
    } flatMap { x => x }
  }

  /**
   * Delete the log files that are older than the threshold time.
   *
   * Its important to note that the threshold time is based on the time stamps used in the log
   * files, and is therefore based on the local system time. So if there is coordination necessary
   * between the node calculating the threshTime (say, driver node), and the local system time
   * (say, worker node), the caller has to take account of possible time skew.
   */
  def cleanupOldLogs(threshTime: Long): Unit = {
    val oldLogFiles = synchronized { pastLogs.filter { _.endTime < threshTime } }
    logInfo(s"Attempting to clear ${oldLogFiles.size} old log files in $logDirectory " +
      s"older than $threshTime: ${oldLogFiles.map { _.path }.mkString("\n")}")

    def deleteFiles() {
      oldLogFiles.foreach { logInfo =>
        try {
          val path = new Path(logInfo.path)
          val fs = hadoopConf.synchronized { path.getFileSystem(hadoopConf) }
          fs.delete(path, true)
          synchronized { pastLogs -= logInfo }
          logDebug(s"Cleared log file $logInfo")
        } catch {
          case ex: Exception =>
            logWarning(s"Error clearing log file $logInfo", ex)
        }
      }
      logInfo(s"Cleared log files in $logDirectory older than $threshTime")
    }
    if (!executionContext.isShutdown) {
      Future { deleteFiles() }
    }
  }

  def stop(): Unit = synchronized {
    if (currentLogWriter != null) {
      currentLogWriter.close()
    }
    executionContext.shutdown()
    logInfo("Stopped log manager")
  }

  private def getLogWriter(currentTime: Long): WriteAheadLogWriter = synchronized {
    if (currentLogWriter == null || currentTime > currentLogWriterStopTime) {
      resetWriter()
      if (currentLogPath != null) {
        pastLogs += LogInfo(currentLogWriterStartTime, currentLogWriterStopTime, currentLogPath)
      }
      currentLogWriterStartTime = currentTime
      currentLogWriterStopTime = currentTime + (rollingIntervalSecs * 1000)
      val newLogPath = new Path(logDirectory,
        timeToLogFile(currentLogWriterStartTime, currentLogWriterStopTime))
      currentLogPath = newLogPath.toString
      currentLogWriter = new WriteAheadLogWriter(currentLogPath, hadoopConf)
    }
    currentLogWriter
  }

  private def initializeOrRecover(): Unit = synchronized {
    val logDirectoryPath = new Path(logDirectory)
    val fileSystem = logDirectoryPath.getFileSystem(hadoopConf)

    if (fileSystem.exists(logDirectoryPath) && fileSystem.getFileStatus(logDirectoryPath).isDir) {
      val logFileInfo = logFilesTologInfo(fileSystem.listStatus(logDirectoryPath).map { _.getPath })
      pastLogs.clear()
      pastLogs ++= logFileInfo
      logInfo(s"Recovered ${logFileInfo.size} log files from $logDirectory")
      logDebug(s"Recovered files are:\n${logFileInfo.map(_.path).mkString("\n")}")
    } else {
      fileSystem.mkdirs(logDirectoryPath,
        FsPermission.createImmutable(Integer.parseInt("770", 8).toShort))
      logInfo(s"Created ${logDirectory} for log files")
    }
  }

  private def resetWriter(): Unit = synchronized {
    if (currentLogWriter != null) {
      currentLogWriter.close()
      currentLogWriter = null
    }
  }
}

private[storage] object WriteAheadLogManager {

  case class LogInfo(startTime: Long, endTime: Long, path: String)

  val logFileRegex = """log-(\d+)-(\d+)""".r

  def timeToLogFile(startTime: Long, stopTime: Long): String = {
    s"log-$startTime-$stopTime"
  }

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
