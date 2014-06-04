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

import java.io.{File, FileFilter, FileOutputStream, InputStream}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.{Executors, ThreadFactory}

import org.apache.commons.io.FileUtils
import org.apache.spark.{Logging, SparkConf}

/**
 * Continuously appends the data from an input stream into the given file.
 */
private[spark] class FileAppender(inputStream: InputStream, file: File, bufferSize: Int = 8192)
  extends Logging {
  @volatile private var outputStream: FileOutputStream = null
  @volatile private var markedForStop = false     // has the appender been asked to stopped
  @volatile private var stopped = false           // has the appender stopped

  // Thread that reads the input stream and writes to file
  private val writingThread = new Thread("File appending thread for " + file) {
    setDaemon(true)
    override def run() {
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
  def awaitTermination() {
    synchronized {
      if (!stopped) {
        wait()
      }
    }
  }

  /** Stop the appender */
  def stop() {
    markedForStop = true
  }

  /** Continuously read chunks from the input stream and append to the file */
  protected def appendStreamToFile() {
    try {
      logDebug("Started appending thread")
      openFile()
      val buf = new Array[Byte](bufferSize)
      var n = 0
      while (!markedForStop && n != -1) {
        n = inputStream.read(buf)
        if (n != -1) {
          appendToFile(buf, n)
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Error writing stream to file $file", e)
    } finally {
      closeFile()
      synchronized {
        stopped = true
        notifyAll()
      }
    }
  }

  /** Append bytes to the file output stream */
  protected def appendToFile(bytes: Array[Byte], len: Int) {
    if (outputStream == null) {
      openFile()
    }
    outputStream.write(bytes, 0, len)
  }

  /** Open the file output stream */
  protected def openFile() {
    outputStream = new FileOutputStream(file, true)
    logDebug(s"Opened file $file")
  }

  /** Close the file output stream */
  protected def closeFile() {
    outputStream.flush()
    outputStream.close()
    logDebug(s"Closed file $file")
  }
}

/**
 * Companion object to [[org.apache.log4j.FileAppender]] which has helper
 * functions to choose the write type of FileAppender based on SparkConf configuration.
 */
private[spark] object FileAppender extends Logging {

  /** Create the right appender based on Spark configuration */
  def apply(inputStream: InputStream, file: File, conf: SparkConf): FileAppender = {

    import RollingFileAppender._

    val rolloverEnabled = conf.getBoolean(ENABLE_PROPERTY, false)
    logDebug("Log rollover enabled = " + rolloverEnabled)
    if (rolloverEnabled) {

      val rolloverSizeOption = conf.getOption(SIZE_PROPERTY)
      val rolloverIntervalOption = conf.getOption(INTERVAL_PROPERTY)

      (rolloverIntervalOption, rolloverSizeOption) match {

        case (Some(rolloverInterval), Some(rolloverSize)) =>              // if both size and interval have been set
          logWarning(s"Rollover interval [$rolloverInterval] and size [$rolloverSize] " +
            s"both set for executor logs, rolling logs not enabled")
          new FileAppender(inputStream, file)

        case (Some(rolloverInterval), None) =>  // if interval has been set
          rolloverInterval match {
            case "daily" =>
              logInfo(s"Rolling executor logs enabled for $file with daily rolling")
              new DailyRollingFileAppender(inputStream, file, conf)
            case "hourly" =>
              logInfo(s"Rolling executor logs enabled for $file with hourly rolling")
              new HourlyRollingFileAppender(inputStream, file, conf)
            case "minutely" =>
              logInfo(s"Rolling executor logs enabled for $file with rolling every minute")
              new MinutelyRollingFileAppender(inputStream, file, conf)
            case IntParam(millis) =>
              logInfo(s"Rolling executor logs enabled for $file with rolling every minute")
              new RollingFileAppender(inputStream, file, new TimeBasedRollingPolicy(millis),
                s"--YYYY-MM-dd--HH-mm-ss-SSSS", conf)
            case _ =>
              logWarning(
                s"Illegal interval for rolling executor logs [$rolloverInterval], " +
                  s"rolling logs not enabled")
              new FileAppender(inputStream, file)
          }

        case (None, Some(rolloverSize)) =>    // if size has been set
          rolloverSize match {
            case IntParam(bytes) =>
              logInfo(s"Rolling executor logs enabled for $file with rolling every $bytes bytes")
              new RollingFileAppender(inputStream, file, new SizeBasedRollingPolicy(bytes),
                s"--YYYY-MM-dd--HH-mm-ss-SSSS", conf)
            case _ =>
              logWarning(
                s"Illegal size for rolling executor logs [$rolloverSize], " +
                  s"rolling logs not enabled")
              new FileAppender(inputStream, file)
          }

        case (None, None) =>                // if neither size nor interval has been set
          logWarning(s"Interval and size for rolling executor logs not set, " +
            s"rolling logs enabled with daily rolling.")
          new DailyRollingFileAppender(inputStream, file, conf)
      }
    } else {
      new FileAppender(inputStream, file)
    }
  }
}

/**
 * Defines the policy based on which [[org.apache.log4j.RollingFileAppender]] will
 * generate rolling files.
 */
private[spark] trait RollingPolicy {

  /** Whether rollover should be initiated at this moment */
  def shouldRollover(bytesToBeWritten: Long): Boolean

  /** Notify that rollover has occurred */
  def rolledOver()

  /** Notify that bytes have been written */
  def bytesWritten(bytes: Long)
}

/**
 * Defines a [[org.apache.spark.util.RollingPolicy]] by which files will be rolled
 * over at a fixed interval.
 */
private[spark] class TimeBasedRollingPolicy(val rolloverIntervalMillis: Long)
  extends RollingPolicy with Logging {

  require(rolloverIntervalMillis >= 100)

  @volatile private var nextRolloverTime = calculateNextRolloverTime()

  /** Should rollover if current time has exceeded next rollover time */
  def shouldRollover(bytesToBeWritten: Long): Boolean = {
    System.currentTimeMillis > nextRolloverTime
  }

  /** Rollover has occurred, so find the next time to rollover */
  def rolledOver() {
    nextRolloverTime = calculateNextRolloverTime()
    logDebug(s"Current time: ${System.currentTimeMillis}, next rollover time: " + nextRolloverTime)
  }

  def bytesWritten(bytes: Long) { }  // nothing to do

  private def calculateNextRolloverTime(): Long = {
    val now = System.currentTimeMillis()
    val targetTime = math.ceil(now.toDouble / rolloverIntervalMillis) * rolloverIntervalMillis
    targetTime.toLong + 1  // +1 to make sure this falls in the next interval
  }
}

/**
 * Defines a [[org.apache.spark.util.RollingPolicy]] bywhich files will be rolled
 * over after reaching a particular size.
 */
private[spark] class SizeBasedRollingPolicy(val rolloverSizeBytes: Long)
  extends RollingPolicy with Logging {

  require(rolloverSizeBytes >= 1000)

  @volatile private var bytesWrittenTillNow = 0L

  /** Should rollover if the next set of bytes is going to exceed the size limit */
  def shouldRollover(bytesToBeWritten: Long): Boolean = {
    bytesToBeWritten + bytesWrittenTillNow > rolloverSizeBytes
  }

  /** Rollover has occurred, so reset the counter */
  def rolledOver() {
    bytesWrittenTillNow = 0
  }

  /** Increment the bytes that have been written in the current file */
  def bytesWritten(bytes: Long) {
    bytesWrittenTillNow += bytes
  }
}

/**
 * Continuously appends data from input stream into the given file, and rolls
 * over the file after the given interval. The rolled over files are named
 * based on the given pattern.
 *
 * @param inputStream             Input stream to read data from
 * @param activeFile              File to write data to
 * @param rollingPolicy           Policy based on which files will be rolled over.
 * @param rollingFilePattern      Pattern based on which the rolled over files will be named.
 *                                Uses SimpleDataFormat pattern.
 * @param conf                    SparkConf that is used to pass on extra configurations
 * @param bufferSize              Optional buffer size. Used mainly for testing.
 */
private[spark] class RollingFileAppender(
    inputStream: InputStream,
    activeFile: File,
    val rollingPolicy: RollingPolicy,
    rollingFilePattern: String,
    conf: SparkConf,
    bufferSize: Int = 8192
  ) extends FileAppender(inputStream, activeFile, bufferSize) {

  import RollingFileAppender._

  private val retainCount = conf.getInt(KEEP_LAST_PROPERTY, -1)
  private val formatter = new SimpleDateFormat(rollingFilePattern)

  private val executor = Executors.newFixedThreadPool(1, new ThreadFactory{
    def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t.setDaemon(true)
      t.setName(s"Threadpool of ${RollingFileAppender.this.getClass.getSimpleName} for $activeFile")
      t
    }
  })

  /** Stop the appender */
  override def stop() {
    super.stop()
    //cleanupTask.cancel(true)
    executor.shutdownNow()
  }

  /** Append bytes to file after rolling over is necessary */
  override protected def appendToFile(bytes: Array[Byte], len: Int) {
    if (rollingPolicy.shouldRollover(len)) {
      rollover()
      rollingPolicy.rolledOver()
    }
    super.appendToFile(bytes, len)
    rollingPolicy.bytesWritten(len)
  }

  /** Rollover the file, by closing the output stream and moving it over */
  private def rollover() {
    val rolloverSuffix = formatter.format(Calendar.getInstance.getTime)
    val rolloverFile = new File(
      activeFile.getParentFile, activeFile.getName + rolloverSuffix).getAbsoluteFile
    logDebug("Attempting to rollover at " + System.currentTimeMillis + " to file " + rolloverFile)

    try {
      closeFile()
      if (activeFile.exists) {
        if (!rolloverFile.exists) {
          FileUtils.moveFile(activeFile, rolloverFile)
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
          } while (i < 10000 && altRolloverFile.exists)

          logWarning(s"Rollover file $rolloverFile already exists, " +
            s"rolled over $activeFile to file $altRolloverFile")
          logWarning(s"Make sure that the given file name pattern [$rollingFilePattern]")
          FileUtils.moveFile(activeFile, altRolloverFile)
        }
      } else {
        logWarning(s"File $activeFile does not exist")
      }
      openFile()
      scheduleCleanup()
    } catch {
      case e: Exception =>
        logError(s"Error rolling over $activeFile to $rolloverFile", e)
    }
  }

  /** Schedule cleaning up for older rolled over files */
  private def scheduleCleanup() {
    if (retainCount > 0) {
      executor.submit(new Runnable() { override def run() { cleanup() }})
    }
  }

  /** Retain only last few files */
  private[util] def cleanup() {
    try {
      val rolledoverFiles = activeFile.getParentFile.listFiles(new FileFilter {
        def accept(f: File): Boolean = {
          f.getName.startsWith(activeFile.getName) && f != activeFile
        }
      }).sorted
      val filesToBeDeleted = rolledoverFiles.take(
        math.max(0, rolledoverFiles.size - retainCount))
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
 * Companion object to [[org.apache.spark.util.RollingFileAppender]]. Defines
 * names of configurations that configure rolling file appenders.
 */
private[spark] object RollingFileAppender {
  val ENABLE_PROPERTY = "spark.executor.rollingLogs.enabled"
  val INTERVAL_PROPERTY = "spark.executor.rollingLogs.interval"
  val SIZE_PROPERTY = "spark.executor.rollingLogs.size"
  val KEEP_LAST_PROPERTY = "spark.executor.rollingLogs.keepLastN"

}

/** RollingFileAppender that rolls over every minute */
private[spark] class MinutelyRollingFileAppender(
    inputStream: InputStream,
    file: File,
    conf: SparkConf
  ) extends RollingFileAppender(
    inputStream,
    file,
    new TimeBasedRollingPolicy(60 * 1000),
    s"--YYYY-MM-dd--HH-mm",
    conf
  )

/** RollingFileAppender that rolls over every hour */
private[spark] class HourlyRollingFileAppender(
    inputStream: InputStream,
    file: File,
    conf: SparkConf
  ) extends RollingFileAppender(
    inputStream,
    file,
    new TimeBasedRollingPolicy(60 * 60 * 1000),
    s"--YYYY-MM-dd--HH",
    conf
  )

/** RollingFileAppender that rolls over every day */
private[spark] class DailyRollingFileAppender(
    inputStream: InputStream,
    file: File,
    conf: SparkConf
  ) extends RollingFileAppender(
    inputStream,
    file,
    new TimeBasedRollingPolicy(24 * 60 * 60 * 1000),
    s"--YYYY-MM-dd",
    conf
  )
