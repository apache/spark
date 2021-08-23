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
import java.util.zip.GZIPOutputStream

import com.google.common.io.Files
import org.apache.commons.io.IOUtils

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.network.util.JavaUtils

/**
 * Continuously appends data from input stream into the given file, and rolls
 * over the file after the given interval. The rolled over files are named
 * based on the given pattern.
 *
 * @param inputStream             Input stream to read data from
 * @param activeFile              File to write data to
 * @param rollingPolicy           Policy based on which files will be rolled over.
 * @param conf                    SparkConf that is used to pass on extra configurations
 * @param bufferSize              Optional buffer size. Used mainly for testing.
 * @param closeStreams            Option flag: whether to close the inputStream at the end.
 */
private[spark] class RollingFileAppender(
    inputStream: InputStream,
    activeFile: File,
    val rollingPolicy: RollingPolicy,
    conf: SparkConf,
    bufferSize: Int = RollingFileAppender.DEFAULT_BUFFER_SIZE,
    closeStreams: Boolean = false
  ) extends FileAppender(inputStream, activeFile, bufferSize, closeStreams) {

  private val maxRetainedFiles = conf.get(config.EXECUTOR_LOGS_ROLLING_MAX_RETAINED_FILES)
  private val enableCompression = conf.get(config.EXECUTOR_LOGS_ROLLING_ENABLE_COMPRESSION)

  /** Stop the appender */
  override def stop(): Unit = {
    super.stop()
  }

  /** Append bytes to file after rolling over is necessary */
  override protected def appendToFile(bytes: Array[Byte], len: Int): Unit = {
    if (rollingPolicy.shouldRollover(len)) {
      rollover()
      rollingPolicy.rolledOver()
    }
    super.appendToFile(bytes, len)
    rollingPolicy.bytesWritten(len)
  }

  /** Rollover the file, by closing the output stream and moving it over */
  private def rollover(): Unit = {
    try {
      closeFile()
      moveFile()
      openFile()
      if (maxRetainedFiles > 0) {
        deleteOldFiles()
      }
    } catch {
      case e: Exception =>
        logError(s"Error rolling over $activeFile", e)
    }
  }

  // Roll the log file and compress if enableCompression is true.
  private def rotateFile(activeFile: File, rolloverFile: File): Unit = {
    if (enableCompression) {
      val gzFile = new File(rolloverFile.getAbsolutePath + RollingFileAppender.GZIP_LOG_SUFFIX)
      var gzOutputStream: GZIPOutputStream = null
      var inputStream: InputStream = null
      try {
        inputStream = new FileInputStream(activeFile)
        gzOutputStream = new GZIPOutputStream(new FileOutputStream(gzFile))
        IOUtils.copy(inputStream, gzOutputStream)
        inputStream.close()
        gzOutputStream.close()
        activeFile.delete()
      } finally {
        JavaUtils.closeQuietly(inputStream)
        JavaUtils.closeQuietly(gzOutputStream)
      }
    } else {
      Files.move(activeFile, rolloverFile)
    }
  }

  // Check if the rollover file already exists.
  private def rolloverFileExist(file: File): Boolean = {
    file.exists || new File(file.getAbsolutePath + RollingFileAppender.GZIP_LOG_SUFFIX).exists
  }

  /** Move the active log file to a new rollover file */
  private def moveFile(): Unit = {
    val rolloverSuffix = rollingPolicy.generateRolledOverFileSuffix()
    val rolloverFile = new File(
      activeFile.getParentFile, activeFile.getName + rolloverSuffix).getAbsoluteFile
    logDebug(s"Attempting to rollover file $activeFile to file $rolloverFile")
    if (activeFile.exists) {
      if (!rolloverFileExist(rolloverFile)) {
        rotateFile(activeFile, rolloverFile)
        logInfo(s"Rolled over $activeFile to $rolloverFile")
      } else {
        // In case the rollover file name clashes, make a unique file name.
        // The resultant file names are long and ugly, so this is used only
        // if there is a name collision. This can be avoided by the using
        // the right pattern such that name collisions do not occur.
        var i = 0
        var altRolloverFile: File = null
        do {
          altRolloverFile = new File(activeFile.getParent,
            s"${activeFile.getName}$rolloverSuffix--$i").getAbsoluteFile
          i += 1
        } while (i < 10000 && rolloverFileExist(altRolloverFile))

        logWarning(s"Rollover file $rolloverFile already exists, " +
          s"rolled over $activeFile to file $altRolloverFile")
        rotateFile(activeFile, altRolloverFile)
      }
    } else {
      logWarning(s"File $activeFile does not exist")
    }
  }

  /** Retain only last few files */
  private[util] def deleteOldFiles(): Unit = {
    try {
      val rolledoverFiles = activeFile.getParentFile.listFiles(new FileFilter {
        def accept(f: File): Boolean = {
          f.getName.startsWith(activeFile.getName) && f != activeFile
        }
      }).sorted
      val filesToBeDeleted = rolledoverFiles.take(
        math.max(0, rolledoverFiles.length - maxRetainedFiles))
      filesToBeDeleted.foreach { file =>
        logInfo(s"Deleting file executor log file ${file.getAbsolutePath}")
        file.delete()
      }
    } catch {
      case e: Exception =>
        logError("Error cleaning logs in directory " + activeFile.getParentFile.getAbsolutePath, e)
    }
  }
}

/**
 * Companion object to [[org.apache.spark.util.logging.RollingFileAppender]]. Defines
 * names of configurations that configure rolling file appenders.
 */
private[spark] object RollingFileAppender {
  val DEFAULT_BUFFER_SIZE = 8192

  val GZIP_LOG_SUFFIX = ".gz"

  /**
   * Get the sorted list of rolled over files. This assumes that the all the rolled
   * over file names are prefixed with the `activeFileName`, and the active file
   * name has the latest logs. So it sorts all the rolled over logs (that are
   * prefixed with `activeFileName`) and appends the active file
   */
  def getSortedRolledOverFiles(directory: String, activeFileName: String): Seq[File] = {
    val rolledOverFiles = new File(directory).getAbsoluteFile.listFiles.filter { file =>
      val fileName = file.getName
      fileName.startsWith(activeFileName) && fileName != activeFileName
    }.sorted
    val activeFile = {
      val file = new File(directory, activeFileName).getAbsoluteFile
      if (file.exists) Some(file) else None
    }
    rolledOverFiles.sortBy(_.getName.stripSuffix(GZIP_LOG_SUFFIX)) ++ activeFile
  }
}
