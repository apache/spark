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

import java.io.{BufferedInputStream, InputStream}
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.google.common.io.ByteStreams
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.DFSInputStream

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.EventLogFileWriter.codecName
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils

/** The base class of reader which will read the information of event log file(s). */
abstract class EventLogFileReader(
    protected val fileSystem: FileSystem,
    val rootPath: Path) {

  protected def fileSizeForDFS(path: Path): Option[Long] = {
    Utils.tryWithResource(fileSystem.open(path)) { in =>
      in.getWrappedStream match {
        case dfsIn: DFSInputStream => Some(dfsIn.getFileLength)
        case _ => None
      }
    }
  }

  protected def addFileAsZipEntry(
      zipStream: ZipOutputStream,
      path: Path,
      entryName: String): Unit = {
    Utils.tryWithResource(fileSystem.open(path, 1 * 1024 * 1024)) { inputStream =>
      zipStream.putNextEntry(new ZipEntry(entryName))
      ByteStreams.copy(inputStream, zipStream)
      zipStream.closeEntry()
    }
  }

  /** Returns the last index of event log files. None for single event log file. */
  def lastIndex: Option[Long]

  /**
   * Returns the size of file for the last index of event log files. Returns its size for
   * single event log file.
   */
  def fileSizeForLastIndex: Long

  /** Returns whether the application is completed. */
  def completed: Boolean

  /**
   * Returns the size of file for the last index (itself for single event log file) of event log
   * files, only when underlying input stream is DFSInputStream. Otherwise returns None.
   */
  def fileSizeForLastIndexForDFS: Option[Long]

  /**
   * Returns the modification time for the last index (itself for single event log file)
   * of event log files.
   */
  def modificationTime: Long

  /**
   * This method compresses the files passed in, and writes the compressed data out into the
   * ZipOutputStream passed in. Each file is written as a new ZipEntry with its name being
   * the name of the file being compressed.
   */
  def zipEventLogFiles(zipStream: ZipOutputStream): Unit

  /** Returns all available event log files. */
  def listEventLogFiles: Seq[FileStatus]

  /** Returns the short compression name if being used. None if it's uncompressed. */
  def compressionCodec: Option[String]

  /** Returns the size of all event log files. */
  def totalSize: Long
}

object EventLogFileReader extends Logging {
  // A cache for compression codecs to avoid creating the same codec many times
  private val codecMap = new ConcurrentHashMap[String, CompressionCodec]()

  def apply(
      fs: FileSystem,
      path: Path,
      lastIndex: Option[Long]): EventLogFileReader = {
    lastIndex match {
      case Some(_) => new RollingEventLogFilesFileReader(fs, path)
      case None => new SingleFileEventLogFileReader(fs, path)
    }
  }

  def apply(fs: FileSystem, path: Path): Option[EventLogFileReader] = {
    apply(fs, fs.getFileStatus(path))
  }

  def apply(fs: FileSystem, status: FileStatus): Option[EventLogFileReader] = {
    if (isSingleEventLog(status)) {
      Some(new SingleFileEventLogFileReader(fs, status.getPath, Option(status)))
    } else if (isRollingEventLogs(status)) {
      if (fs.listStatus(status.getPath).exists(RollingEventLogFilesWriter.isEventLogFile)) {
        Some(new RollingEventLogFilesFileReader(fs, status.getPath))
      } else {
        logDebug(s"Rolling event log directory have no event log file at ${status.getPath}")
        None
      }
    } else {
      None
    }
  }

  /**
   * Opens an event log file and returns an input stream that contains the event data.
   *
   * @return input stream that holds one JSON record per line.
   */
  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    val in = new BufferedInputStream(fs.open(log))
    try {
      val codec = codecName(log).map { c =>
        codecMap.computeIfAbsent(c, CompressionCodec.createCodec(new SparkConf, _))
      }
      codec.map(_.compressedContinuousInputStream(in)).getOrElse(in)
    } catch {
      case e: Throwable =>
        in.close()
        throw e
    }
  }

  private def isSingleEventLog(status: FileStatus): Boolean = {
    !status.isDirectory &&
      // FsHistoryProvider used to generate a hidden file which can't be read.  Accidentally
      // reading a garbage file is safe, but we would log an error which can be scary to
      // the end-user.
      !status.getPath.getName.startsWith(".")
  }

  private def isRollingEventLogs(status: FileStatus): Boolean = {
    RollingEventLogFilesWriter.isEventLogDir(status)
  }
}

/**
 * The reader which will read the information of single event log file.
 *
 * This reader gets the status of event log file only once when required;
 * It may not give "live" status of file that could be changing concurrently, and
 * FileNotFoundException could occur if the log file is renamed before getting the
 * status of log file.
 */
private[history] class SingleFileEventLogFileReader(
    fs: FileSystem,
    path: Path,
    maybeStatus: Option[FileStatus]) extends EventLogFileReader(fs, path) {
  private lazy val status = maybeStatus.getOrElse(fileSystem.getFileStatus(rootPath))

  def this(fs: FileSystem, path: Path) = this(fs, path, None)

  override def lastIndex: Option[Long] = None

  override def fileSizeForLastIndex: Long = status.getLen

  override def completed: Boolean = !rootPath.getName.stripSuffix(EventLogFileWriter.COMPACTED)
    .endsWith(EventLogFileWriter.IN_PROGRESS)

  override def fileSizeForLastIndexForDFS: Option[Long] = {
    if (completed) {
      Some(fileSizeForLastIndex)
    } else {
      fileSizeForDFS(rootPath)
    }
  }

  override def modificationTime: Long = status.getModificationTime

  override def zipEventLogFiles(zipStream: ZipOutputStream): Unit = {
    addFileAsZipEntry(zipStream, rootPath, rootPath.getName)
  }

  override def listEventLogFiles: Seq[FileStatus] = Seq(status)

  override def compressionCodec: Option[String] = EventLogFileWriter.codecName(rootPath)

  override def totalSize: Long = fileSizeForLastIndex
}

/**
 * The reader which will read the information of rolled multiple event log files.
 *
 * This reader lists the files only once; if caller would like to play with updated list,
 * it needs to create another reader instance.
 */
private[history] class RollingEventLogFilesFileReader(
    fs: FileSystem,
    path: Path) extends EventLogFileReader(fs, path) {
  import RollingEventLogFilesWriter._

  private lazy val files: Seq[FileStatus] = {
    val ret = fs.listStatus(rootPath).toSeq
    require(ret.exists(isEventLogFile), "Log directory must contain at least one event log file!")
    require(ret.exists(isAppStatusFile), "Log directory must contain an appstatus file!")
    ret
  }

  private lazy val appStatusFile = files.find(isAppStatusFile).get

  private lazy val eventLogFiles: Seq[FileStatus] = {
    val eventLogFiles = files.filter(isEventLogFile).sortBy { status =>
      val filePath = status.getPath
      var idx = getEventLogFileIndex(filePath.getName).toDouble
      // trick to place compacted file later than normal file if index is same.
      if (EventLogFileWriter.isCompacted(filePath)) {
        idx += 0.1
      }
      idx
    }
    val filesToRead = dropBeforeLastCompactFile(eventLogFiles)
    val indices = filesToRead.map { file => getEventLogFileIndex(file.getPath.getName) }
    require((indices.head to indices.last) == indices, "Found missing event log file, expected" +
      s" indices: ${indices.head to indices.last}, actual: ${indices}")
    filesToRead
  }

  override def lastIndex: Option[Long] = Some(
    getEventLogFileIndex(lastEventLogFile.getPath.getName))

  override def fileSizeForLastIndex: Long = lastEventLogFile.getLen

  override def completed: Boolean = {
    !appStatusFile.getPath.getName.endsWith(EventLogFileWriter.IN_PROGRESS)
  }

  override def fileSizeForLastIndexForDFS: Option[Long] = {
    if (completed) {
      Some(fileSizeForLastIndex)
    } else {
      fileSizeForDFS(lastEventLogFile.getPath)
    }
  }

  override def modificationTime: Long = lastEventLogFile.getModificationTime

  override def zipEventLogFiles(zipStream: ZipOutputStream): Unit = {
    val dirEntryName = rootPath.getName + "/"
    zipStream.putNextEntry(new ZipEntry(dirEntryName))
    files.foreach { file =>
      addFileAsZipEntry(zipStream, file.getPath, dirEntryName + file.getPath.getName)
    }
  }

  override def listEventLogFiles: Seq[FileStatus] = eventLogFiles

  override def compressionCodec: Option[String] = {
    EventLogFileWriter.codecName(eventLogFiles.head.getPath)
  }

  override def totalSize: Long = eventLogFiles.map(_.getLen).sum

  private def lastEventLogFile: FileStatus = eventLogFiles.last

  private def dropBeforeLastCompactFile(eventLogFiles: Seq[FileStatus]): Seq[FileStatus] = {
    val lastCompactedFileIdx = eventLogFiles.lastIndexWhere { fs =>
      EventLogFileWriter.isCompacted(fs.getPath)
    }
    eventLogFiles.drop(lastCompactedFileIdx)
  }
}
