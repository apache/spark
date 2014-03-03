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

package org.apache.spark.util

import java.io._
import java.text.SimpleDateFormat
import java.net.URI
import java.util.Date

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * A generic class for logging information to file
 * @param logBaseDir Path to the directory in which files are logged
 * @param name An identifier of each FileLogger instance
 * @param overwriteExistingFiles Whether to overwrite existing files
 */
class FileLogger(
    logBaseDir: String,
    name: String = String.valueOf(System.currentTimeMillis()),
    overwriteExistingFiles: Boolean = true)
  extends Logging {

  private val logDir = logBaseDir.stripSuffix("/") + "/" + name
  private val DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  private var fileIndex = 0

  private var writer: Option[PrintWriter] = {
    createLogDir()
    createWriter()
  }

  /** Create a logging directory with the given path */
  private def createLogDir() = {
    val dir = new File(logDir)
    if (dir.exists) {
      logWarning("Logging directory already exists: " + logDir)
    }
    if (!dir.exists && !dir.mkdirs()) {
      // Logger should throw a exception rather than continue to construct this object
      throw new IOException("Error in creating log directory:" + logDir)
    }
  }

  /**
   * Create a new writer to the file identified with the given path file.
   * File systems currently supported include hdfs, s3, and the local file system.
   */
  private def createWriter(): Option[PrintWriter] = {
    val logPath = logDir + "/" + fileIndex
    val uri = new URI(logPath)
    val fileStream = uri.getScheme match {
      case "hdfs" | "s3" =>
        val conf = SparkHadoopUtil.get.newConfiguration()
        val fs = FileSystem.get(uri, conf)
        val path = new Path(logPath)
        fs.create(path, overwriteExistingFiles)
      case "file" | null =>
        // org.apache.hadoop.fs.FileSystem (r1.0.4) does not flush on local files
        // Second parameter is whether to append
        new FileOutputStream(logPath, !overwriteExistingFiles)
      case _ =>
        logWarning("Given logging directory is invalid: %s".format(logDir))
        return None
    }
    val bufferedStream = new BufferedOutputStream(fileStream)
    Some(new PrintWriter(bufferedStream))
  }

  /**
   * Log the message to the given writer
   * @param msg The message to be logged
   * @param withTime Whether to prepend message with a timestamp
   */
  def log(msg: String, withTime: Boolean = false) = {
    var writeInfo = msg
    if (withTime) {
      val date = new Date(System.currentTimeMillis())
      writeInfo = DATE_FORMAT.format(date) + ": " + msg
    }
    writer.foreach(_.print(writeInfo))
  }

  /**
   * Log the message to the given writer as a new line
   * @param msg The message to be logged
   * @param withTime Whether to prepend message with a timestamp
   */
  def logLine(msg: String, withTime: Boolean = false) = log(msg + "\n", withTime)

  /** Flush the writer to disk manually */
  def flush() = writer.foreach(_.flush())

  /** Close the writer. Any subsequent calls to log or flush will have no effect. */
  def close() = {
    writer.foreach(_.close())
    writer = None
  }

  /** Start a new writer (for a new file) if there does not already exist one */
  def start() = {
    writer.getOrElse {
      fileIndex += 1
      writer = createWriter()
    }
  }
}
