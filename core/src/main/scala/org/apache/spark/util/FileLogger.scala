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
import java.util.Date

import org.apache.spark.Logging

/**
 * A generic class for logging information to file
 * @param user User identifier if SPARK_LOG_DIR is not set, in which case log directory
 *             defaults to /tmp/spark-[user]
 * @param name Name of logger, also the base name of the log files
 * @param flushPeriod How many writes until the results are flushed to disk. By default,
 *                    only flush manually
 */
class FileLogger(user: String, name: String, flushPeriod: Int = Integer.MAX_VALUE) extends Logging {
  private val DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  private var logCount = 0
  private var fileIndex = 0

  private val logDir =
    if (System.getenv("SPARK_LOG_DIR") != null) {
      "%s/%s/".format(System.getenv("SPARK_LOG_DIR"), name)
    } else {
      "/tmp/spark-%s/%s/".format(user, name)
    }

  private var writer: Option[PrintWriter] = {
    createLogDir()
    Some(createWriter())
  }

  def this() = this(System.getProperty("user.name", "<Unknown>"),
    String.valueOf(System.currentTimeMillis()))

  def this(_name: String) = this(System.getProperty("user.name", "<Unknown>"), _name)

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

  /** Create a new writer to the file identified with the given path */
  private def createWriter() = {
    // Overwrite any existing file
    val fileWriter = new FileWriter(logDir + fileIndex)
    val bufferedWriter = new BufferedWriter(fileWriter)
    new PrintWriter(bufferedWriter)
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
    logCount += 1
    if (logCount % flushPeriod == 0) {
      flush()
      logCount = 0
    }
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
      writer = Some(createWriter())
    }
  }
}
