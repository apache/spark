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

import java.io.{File, FileOutputStream, InputStream, IOException}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.util.{IntParam, Utils}

/**
 * Continuously appends the data from an input stream into the given file.
 */
private[spark] class FileAppender(
  inputStream: InputStream,
  file: File,
  bufferSize: Int = 8192,
  closeStreams: Boolean = false
) extends Logging {
  @volatile private var outputStream: FileOutputStream = null
  @volatile private var markedForStop = false     // has the appender been asked to stopped

  // Thread that reads the input stream and writes to file
  private val writingThread = new Thread("File appending thread for " + file) {
    setDaemon(true)
    override def run(): Unit = {
      Utils.logUncaughtExceptions {
        appendStreamToFile()
      }
    }
  }
  writingThread.start()

  /**
   * Wait for the appender to stop appending, either because input stream is closed
   * or because of any error in appending
   */
  def awaitTermination(): Unit = {
    writingThread.join()
  }

  /** Stop the appender */
  def stop(): Unit = {
    markedForStop = true
  }

  /** Continuously read chunks from the input stream and append to the file */
  protected def appendStreamToFile(): Unit = {
    try {
      logDebug("Started appending thread")
      Utils.tryWithSafeFinally {
        openFile()
        val buf = new Array[Byte](bufferSize)
        var n = 0
        while (!markedForStop && n != -1) {
          try {
            n = inputStream.read(buf)
          } catch {
            // An InputStream can throw IOException during read if the stream is closed
            // asynchronously, so once appender has been flagged to stop these will be ignored
            case _: IOException if markedForStop =>  // do nothing and proceed to stop appending
          }
          if (n > 0) {
            appendToFile(buf, n)
          }
        }
      } {
        try {
          if (closeStreams) {
            inputStream.close()
          }
        } finally {
          closeFile()
        }
      }
    } catch {
      case e: Exception =>
        logError(log"Error writing stream to file ${MDC(PATH, file)}", e)
    }
  }

  /** Append bytes to the file output stream */
  protected def appendToFile(bytes: Array[Byte], len: Int): Unit = {
    if (outputStream == null) {
      openFile()
    }
    outputStream.write(bytes, 0, len)
  }

  /** Open the file output stream */
  protected def openFile(): Unit = {
    outputStream = new FileOutputStream(file, true)
    logDebug(s"Opened file $file")
  }

  /** Close the file output stream */
  protected def closeFile(): Unit = {
    outputStream.flush()
    outputStream.close()
    logDebug(s"Closed file $file")
  }
}

/**
 * Companion object to [[org.apache.spark.util.logging.FileAppender]] which has helper
 * functions to choose the correct type of FileAppender based on SparkConf configuration.
 */
private[spark] object FileAppender extends Logging {

  /** Create the right appender based on Spark configuration */
  def apply(
    inputStream: InputStream,
    file: File,
    conf: SparkConf,
    closeStreams: Boolean = false
  ) : FileAppender = {

    val rollingStrategy = conf.get(config.EXECUTOR_LOGS_ROLLING_STRATEGY)
    val rollingSizeBytes = conf.get(config.EXECUTOR_LOGS_ROLLING_MAX_SIZE)
    val rollingInterval = conf.get(config.EXECUTOR_LOGS_ROLLING_TIME_INTERVAL)

    def createTimeBasedAppender(): FileAppender = {
      val validatedParams: Option[(Long, String)] = rollingInterval match {
        case "daily" =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)} with daily rolling")
          Some((24 * 60 * 60 * 1000L, "--yyyy-MM-dd"))
        case "hourly" =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)}" +
            log" with hourly rolling")
          Some((60 * 60 * 1000L, "--yyyy-MM-dd--HH"))
        case "minutely" =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)}" +
            log" with rolling every minute")
          Some((60 * 1000L, "--yyyy-MM-dd--HH-mm"))
        case IntParam(seconds) =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)}" +
            log" with rolling ${MDC(TIME_UNITS, seconds)} seconds")
          Some((seconds * 1000L, "--yyyy-MM-dd--HH-mm-ss"))
        case _ =>
          logWarning(log"Illegal interval for rolling executor logs [" +
            log"${MDC(TIME_UNITS, rollingInterval)}], " +
            log"rolling logs not enabled")
          None
      }
      validatedParams.map {
        case (interval, pattern) =>
          new RollingFileAppender(
            inputStream, file, new TimeBasedRollingPolicy(interval, pattern), conf,
            closeStreams = closeStreams)
      }.getOrElse {
        new FileAppender(inputStream, file, closeStreams = closeStreams)
      }
    }

    def createSizeBasedAppender(): FileAppender = {
      rollingSizeBytes match {
        case IntParam(bytes) =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)}" +
            log" with rolling every ${MDC(NUM_BYTES, bytes)} bytes")
          new RollingFileAppender(
            inputStream, file, new SizeBasedRollingPolicy(bytes), conf, closeStreams = closeStreams)
        case _ =>
          logWarning(
            log"Illegal size [${MDC(NUM_BYTES, rollingSizeBytes)}] " +
              log"for rolling executor logs, rolling logs not enabled")
          new FileAppender(inputStream, file, closeStreams = closeStreams)
      }
    }

    rollingStrategy match {
      case "" =>
        new FileAppender(inputStream, file, closeStreams = closeStreams)
      case "time" =>
        createTimeBasedAppender()
      case "size" =>
        createSizeBasedAppender()
      case _ =>
        logWarning(
          s"Illegal strategy [$rollingStrategy] for rolling executor logs, " +
            s"rolling logs not enabled")
        new FileAppender(inputStream, file, closeStreams = closeStreams)
    }
  }
}


