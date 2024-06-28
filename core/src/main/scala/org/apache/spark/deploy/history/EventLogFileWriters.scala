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

package org.apache.spark.deploy.history

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets

import org.apache.commons.io.output.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils

/**
 * The base class of writer which will write event logs into file.
 *
 * The following configurable parameters are available to tune the behavior of writing:
 *   spark.eventLog.compress - Whether to compress logged events
 *   spark.eventLog.compression.codec - The codec to compress logged events
 *   spark.eventLog.overwrite - Whether to overwrite any existing files
 *   spark.eventLog.buffer.kb - Buffer size to use when writing to output streams
 *
 * Note that descendant classes can maintain its own parameters: refer the javadoc of each class
 * for more details.
 *
 * NOTE: CountingOutputStream being returned by "initLogFile" counts "non-compressed" bytes.
 */
abstract class EventLogFileWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends Logging {

  protected val shouldCompress = sparkConf.get(EVENT_LOG_COMPRESS) &&
      !sparkConf.get(EVENT_LOG_COMPRESSION_CODEC).equalsIgnoreCase("none")
  protected val shouldOverwrite = sparkConf.get(EVENT_LOG_OVERWRITE)
  protected val outputBufferSize = sparkConf.get(EVENT_LOG_OUTPUT_BUFFER_SIZE).toInt * 1024
  protected val fileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)
  protected val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf, sparkConf.get(EVENT_LOG_COMPRESSION_CODEC)))
    } else {
      None
    }

  private[history] val compressionCodecName = compressionCodec.map { c =>
    CompressionCodec.getShortName(c.getClass.getName)
  }

  // Only defined if the file system scheme is not local
  protected var hadoopDataStream: Option[FSDataOutputStream] = None
  protected var writer: Option[PrintWriter] = None

  protected def requireLogBaseDirAsDirectory(): Unit = {
    if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDirectory) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir is not a directory.")
    }
  }

  protected def initLogFile(path: Path)(fnSetupWriter: OutputStream => PrintWriter): Unit = {
    if (shouldOverwrite && fileSystem.delete(path, true)) {
      logWarning(log"Event log ${MDC(LogKeys.PATH, path)} already exists. Overwriting...")
    }

    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"
    val uri = path.toUri

    // The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
    // Therefore, for local files, use FileOutputStream instead.
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        hadoopDataStream = Some(
          SparkHadoopUtil.createFile(fileSystem, path, sparkConf.get(EVENT_LOG_ALLOW_EC)))
        hadoopDataStream.get
      }

    try {
      val cstream = compressionCodec.map(_.compressedContinuousOutputStream(dstream))
        .getOrElse(dstream)
      val bstream = new BufferedOutputStream(cstream, outputBufferSize)
      fileSystem.setPermission(path, EventLogFileWriter.LOG_FILE_PERMISSIONS)
      logInfo(log"Logging events to ${MDC(PATH, path)}")
      writer = Some(fnSetupWriter(bstream))
    } catch {
      case e: Exception =>
        dstream.close()
        throw e
    }
  }

  protected def writeLine(line: String, flushLogger: Boolean = false): Unit = {
    // scalastyle:off println
    writer.foreach(_.println(line))
    // scalastyle:on println
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(_.hflush())
    }
  }

  protected def closeWriter(): Unit = {
    writer.foreach(_.close())
  }

  protected def renameFile(src: Path, dest: Path, overwrite: Boolean): Unit = {
    if (fileSystem.exists(dest)) {
      if (overwrite) {
        logWarning(log"Event log ${MDC(EVENT_LOG_DESTINATION, dest)} already exists. " +
          log"Overwriting...")
        if (!fileSystem.delete(dest, true)) {
          logWarning(log"Error deleting ${MDC(EVENT_LOG_DESTINATION, dest)}")
        }
      } else {
        throw new IOException(s"Target log file already exists ($dest)")
      }
    }
    fileSystem.rename(src, dest)
    // touch file to ensure modtime is current across those filesystems where rename()
    // does not set it but support setTimes() instead; it's a no-op on most object stores
    try {
      fileSystem.setTimes(dest, System.currentTimeMillis(), -1)
    } catch {
      case e: Exception => logDebug(s"failed to set time of $dest", e)
    }
  }

  /** initialize writer for event logging */
  def start(): Unit

  /** writes JSON format of event to file */
  def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit

  /** stops writer - indicating the application has been completed */
  def stop(): Unit

  /** returns representative path of log. for tests only. */
  def logPath: String
}

object EventLogFileWriter {
  // Suffix applied to the names of files still being written by applications.
  val IN_PROGRESS = ".inprogress"
  val COMPACTED = ".compact"

  val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("660", 8).toShort)
  val LOG_FOLDER_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  def apply(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    if (sparkConf.get(EVENT_LOG_ENABLE_ROLLING)) {
      new RollingEventLogFilesWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
    } else {
      new SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
    }
  }

  def nameForAppAndAttempt(appId: String, appAttemptId: Option[String]): String = {
    val base = Utils.sanitizeDirName(appId)
    if (appAttemptId.isDefined) {
      base + "_" + Utils.sanitizeDirName(appAttemptId.get)
    } else {
      base
    }
  }

  def codecName(log: Path): Option[String] = {
    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(COMPACTED).stripSuffix(IN_PROGRESS)
    logName.split("\\.").tail.lastOption
  }

  def isCompacted(log: Path): Boolean = log.getName.endsWith(COMPACTED)
}

/**
 * The writer to write event logs into single file.
 */
class SingleEventLogFileWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) {

  override val logPath: String = SingleEventLogFileWriter.getLogPath(logBaseDir, appId,
    appAttemptId, compressionCodecName)

  protected def inProgressPath = logPath + EventLogFileWriter.IN_PROGRESS

  override def start(): Unit = {
    requireLogBaseDirAsDirectory()

    initLogFile(new Path(inProgressPath)) { os =>
      new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))
    }
  }

  override def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit = {
    writeLine(eventJson, flushLogger)
  }

  /**
   * Stop logging events. The event log file will be renamed so that it loses the
   * ".inprogress" suffix.
   */
  override def stop(): Unit = {
    closeWriter()
    renameFile(new Path(inProgressPath), new Path(logPath), shouldOverwrite)
  }
}

object SingleEventLogFileWriter {
  /**
   * Return a file-system-safe path to the log file for the given application.
   *
   * Note that because we currently only create a single log file for each application,
   * we must encode all the information needed to parse this event log in the file name
   * instead of within the file itself. Otherwise, if the file is compressed, for instance,
   * we won't know which codec to use to decompress the metadata needed to open the file in
   * the first place.
   *
   * The log file name will identify the compression codec used for the contents, if any.
   * For example, app_123 for an uncompressed log, app_123.lzf for an LZF-compressed log.
   *
   * @param logBaseDir Directory where the log file will be written.
   * @param appId A unique app ID.
   * @param appAttemptId A unique attempt id of appId. May be the empty string.
   * @param compressionCodecName Name to identify the codec used to compress the contents
   *                             of the log, or None if compression is not enabled.
   * @return A path which consists of file-system-safe characters.
   */
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String] = None): String = {
    val codec = compressionCodecName.map("." + _).getOrElse("")
    new Path(logBaseDir).toString.stripSuffix("/") + "/" +
      EventLogFileWriter.nameForAppAndAttempt(appId, appAttemptId) + codec
  }
}

/**
 * The writer to write event logs into multiple log files, rolled over via configured size.
 *
 * The class creates one directory per application, and stores event log files as well as
 * metadata files. The name of directory and files in the directory would follow:
 *
 * - The name of directory: eventlog_v2_appId(_[appAttemptId])
 * - The prefix of name on event files: events_[index]_[appId](_[appAttemptId])(.[codec])
 *   - "index" would be monotonically increasing value (say, sequence)
 * - The name of metadata (app. status) file name: appstatus_[appId](_[appAttemptId])(.inprogress)
 *
 * The writer will roll over the event log file when configured size is reached. Note that the
 * writer doesn't check the size on file being open for write: the writer tracks the count of bytes
 * written before compression is applied.
 *
 * For metadata files, the class will leverage zero-byte file, as it provides minimized cost.
 */
class RollingEventLogFilesWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) {

  import RollingEventLogFilesWriter._

  private val eventFileMaxLength = sparkConf.get(EVENT_LOG_ROLLING_MAX_FILE_SIZE)

  private val logDirForAppPath = getAppEventLogDirPath(logBaseDir, appId, appAttemptId)

  private var countingOutputStream: Option[CountingOutputStream] = None

  // index and event log path will be updated soon in rollEventLogFile, which `start` will call
  private var index: Long = 0L
  private var currentEventLogFilePath: Path = _

  override def start(): Unit = {
    requireLogBaseDirAsDirectory()

    if (fileSystem.exists(logDirForAppPath) && shouldOverwrite) {
      fileSystem.delete(logDirForAppPath, true)
    }

    if (fileSystem.exists(logDirForAppPath)) {
      throw new IOException(s"Target log directory already exists ($logDirForAppPath)")
    }

    // SPARK-30860: use the class method to avoid the umask causing permission issues
    FileSystem.mkdirs(fileSystem, logDirForAppPath, EventLogFileWriter.LOG_FOLDER_PERMISSIONS)
    createAppStatusFile(inProgress = true)
    rollEventLogFile()
  }

  override def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit = {
    writer.foreach { w =>
      val currentLen = countingOutputStream.get.getByteCount
      if (currentLen + eventJson.length > eventFileMaxLength) {
        rollEventLogFile()
      }
    }

    writeLine(eventJson, flushLogger)
  }

  /** exposed for testing only */
  private[history] def rollEventLogFile(): Unit = {
    closeWriter()

    index += 1
    currentEventLogFilePath = getEventLogFilePath(logDirForAppPath, appId, appAttemptId, index,
      compressionCodecName)

    initLogFile(currentEventLogFilePath) { os =>
      countingOutputStream = Some(new CountingOutputStream(os))
      new PrintWriter(
        new OutputStreamWriter(countingOutputStream.get, StandardCharsets.UTF_8))
    }
  }

  override def stop(): Unit = {
    closeWriter()
    val appStatusPathIncomplete = getAppStatusFilePath(logDirForAppPath, appId, appAttemptId,
      inProgress = true)
    val appStatusPathComplete = getAppStatusFilePath(logDirForAppPath, appId, appAttemptId,
      inProgress = false)
    renameFile(appStatusPathIncomplete, appStatusPathComplete, overwrite = true)
  }

  override def logPath: String = logDirForAppPath.toString

  private def createAppStatusFile(inProgress: Boolean): Unit = {
    val appStatusPath = getAppStatusFilePath(logDirForAppPath, appId, appAttemptId, inProgress)
    // SPARK-30860: use the class method to avoid the umask causing permission issues
    val outputStream = FileSystem.create(fileSystem, appStatusPath,
      EventLogFileWriter.LOG_FILE_PERMISSIONS)
    // we intentionally create zero-byte file to minimize the cost
    outputStream.close()
  }
}

object RollingEventLogFilesWriter {
  private[history] val EVENT_LOG_DIR_NAME_PREFIX = "eventlog_v2_"
  private[history] val EVENT_LOG_FILE_NAME_PREFIX = "events_"
  private[history] val APPSTATUS_FILE_NAME_PREFIX = "appstatus_"

  def getAppEventLogDirPath(logBaseDir: URI, appId: String, appAttemptId: Option[String]): Path =
    new Path(new Path(logBaseDir), EVENT_LOG_DIR_NAME_PREFIX +
      EventLogFileWriter.nameForAppAndAttempt(appId, appAttemptId))

  def getAppStatusFilePath(
      appLogDir: Path,
      appId: String,
      appAttemptId: Option[String],
      inProgress: Boolean): Path = {
    val base = APPSTATUS_FILE_NAME_PREFIX +
      EventLogFileWriter.nameForAppAndAttempt(appId, appAttemptId)
    val name = if (inProgress) base + EventLogFileWriter.IN_PROGRESS else base
    new Path(appLogDir, name)
  }

  def getEventLogFilePath(
      appLogDir: Path,
      appId: String,
      appAttemptId: Option[String],
      index: Long,
      codecName: Option[String]): Path = {
    val base = s"${EVENT_LOG_FILE_NAME_PREFIX}${index}_" +
      EventLogFileWriter.nameForAppAndAttempt(appId, appAttemptId)
    val codec = codecName.map("." + _).getOrElse("")
    new Path(appLogDir, base + codec)
  }

  def isEventLogDir(status: FileStatus): Boolean = {
    status.isDirectory && status.getPath.getName.startsWith(EVENT_LOG_DIR_NAME_PREFIX)
  }

  def isEventLogFile(fileName: String): Boolean = {
    fileName.startsWith(EVENT_LOG_FILE_NAME_PREFIX)
  }

  def isEventLogFile(status: FileStatus): Boolean = {
    status.isFile && isEventLogFile(status.getPath.getName)
  }

  def isAppStatusFile(status: FileStatus): Boolean = {
    status.isFile && status.getPath.getName.startsWith(APPSTATUS_FILE_NAME_PREFIX)
  }

  def getEventLogFileIndex(eventLogFileName: String): Long = {
    require(isEventLogFile(eventLogFileName), "Not an event log file!")
    val index = eventLogFileName.stripPrefix(EVENT_LOG_FILE_NAME_PREFIX).split("_")(0)
    index.toLong
  }
}
