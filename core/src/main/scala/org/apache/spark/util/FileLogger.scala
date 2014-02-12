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

import java.io.{IOException, File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark._

/**
 * A generic class for logging information to file
 */

class FileLogger(user: String, name: String, flushFrequency: Int = 1) extends Logging {

  private val DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  private var logCount = 0

  private val logDir =
    if (System.getenv("SPARK_LOG_DIR") != null) {
      System.getenv("SPARK_LOG_DIR")
    } else {
      "/tmp/spark-%s".format(user)
    }

  private val logFile = logDir + "/" + name

  private val writer: PrintWriter = {
    createLogDir()
    new PrintWriter(logFile)
  }

  def this() = this(System.getProperty("user.name", "<Unknown>"),
    String.valueOf(System.currentTimeMillis()))

  def this(_name: String) = this(System.getProperty("user.name", "<Unknown>"), _name)

  /** Create a logging directory with the given path */
  private def createLogDir() {
    val dir = new File(logDir)
    if (!dir.exists && !dir.mkdirs()) {
      // Logger should throw a exception rather than continue to construct this object
      throw new IOException("create log directory error:" + logDir)
    }
    val file = new File(logFile)
    if (file.exists) {
      logWarning("Overwriting existing log file at %s".format(logFile))
    }
  }

  /** Log the message to the given writer if it exists, optionally including the time */
  def log(msg: String, withTime: Boolean = false) = {
    var writeInfo = msg
    if (withTime) {
      val date = new Date(System.currentTimeMillis())
      writeInfo = DATE_FORMAT.format(date) + ": " + msg
    }
    writer.print(writeInfo)
    logCount += 1
    if (logCount % flushFrequency == 0) {
      writer.flush()
      logCount = 0
    }
  }

  /**
   * Log the message as a new line to the given writer if it exists, optionally including the time
   */
  def logLine(msg: String, withTime: Boolean = false) = log(msg + "\n", withTime)

  /** Close the writer, if it exists */
  def close() = writer.close()
}
