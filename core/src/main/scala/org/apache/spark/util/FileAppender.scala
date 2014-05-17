package org.apache.spark.util

import java.io.{FileFilter, File, FileOutputStream, InputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Timer, TimerTask}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.{IntParam, Utils}
import org.apache.commons.io.FileUtils
import java.util.concurrent.{TimeUnit, ThreadFactory, Executors}

/**
 * Continuously appends the data from an input stream into the given file
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
        logError(s"Error writing stream to file $file")
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

  /** Open theh file output stream */
  protected def openFile() {
    outputStream = new FileOutputStream(file, true)
    logDebug(s"Opened file $file")
  }

  protected def closeFile() {
    outputStream.flush()
    outputStream.close()
    logDebug(s"Closed file $file")
  }
}

private[spark] object FileAppender extends Logging {

  /** Create the right appender based on the configuration */
  def apply(inputStream: InputStream, file: File, conf: SparkConf): FileAppender = {
    val rolloverEnabled = conf.getBoolean("spark.executor.rolloverLogs.enabled", false)
    if (rolloverEnabled) {
      val rolloverInterval = conf.get("spark.executor.rolloverLogs.interval", "daily")
      rolloverInterval match {
        case "daily" =>
          logDebug(s"Using DailyRollingFileAppender for $file")
          new DailyRollingFileAppender(inputStream, file, conf)
        case "hourly" =>
          logDebug(s"Using HourlyRollingFileAppender for $file")
          new HourlyRollingFileAppender(inputStream, file, conf)
        case "minutely" =>
          logInfo(s"Using MinutelyRollingFileAppender for $file")
          new MinutelyRollingFileAppender(inputStream, file, conf)
        case IntParam(millis) =>
          logInfo(s"Using RollingFileAppender with interval of $millis ")
          new RollingFileAppender(
            inputStream, file, millis, s"'${file.getName}'--YYYY-MM-dd--HH-mm-ss-SSSS", conf)
        case _ =>
          logWarning(
            s"Illegal rollover interval for executor logs [$rolloverInterval], disabling rolling")
          new FileAppender(inputStream, file)
      }
    } else {
      new FileAppender(inputStream, file)
    }
  }
}

/**
 * Continuously appends data from input stream into the given file, and rolls
 * over the file after the given interval. The rolled over files are named
 * based on the given pattern.
 *
 * @param inputStream             Input stream to read data from
 * @param file                    File to write data to
 * @param rolloverIntervalMillis  Interval at which the file is rolled over
 * @param rolloverFilePattern     Pattern based on which the rolled over files will be named.
 *                                Uses SimpleDataFormat pattern.
 * @param conf                    SparkConf that is used to pass on extra configurations
 * @param bufferSize              Optional buffer size. Used mainly for testing.
 */
private[spark] class RollingFileAppender(
    inputStream: InputStream,
    file: File,
    rolloverIntervalMillis: Long,
    rolloverFilePattern: String,
    conf: SparkConf,
    bufferSize: Int = 8192
  ) extends FileAppender(inputStream, file, bufferSize) {

  require(rolloverIntervalMillis >= 100)

  private val cleanupTtlMillis = conf.getLong(
    "spark.executor.rolloverLogs.cleanupTtl", 60 * 60 * 24 * 7) * 1000  // 7 days
  private val cleanupIntervalMillis = cleanupTtlMillis / 10
  private val formatter = new SimpleDateFormat(rolloverFilePattern)
  @volatile private var shouldRollover = false

  private val firstRolloverDelayMillis = {
    val now = currentTime()
    val targetTime = math.ceil(now / rolloverIntervalMillis) * rolloverIntervalMillis
    targetTime.toLong - now + 10
  }

  private val executor = Executors.newScheduledThreadPool(1, new ThreadFactory{
    def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t.setDaemon(true)
      t.setName(s"Threadpool of ${RollingFileAppender.this.getClass.getSimpleName} for $file")
      t
    }
  })

  private val rolloverTask = executor.scheduleAtFixedRate(
    new Runnable {
      def run() {
        shouldRollover = true;
        logDebug("Marked for rollover")
      }
    },
    firstRolloverDelayMillis,
    rolloverIntervalMillis,
    TimeUnit.MILLISECONDS
  )
  logDebug(s"Rollover task scheduled: $firstRolloverDelayMillis ms, $rolloverIntervalMillis ms")

  private val cleanupTask = executor.scheduleWithFixedDelay(
    new Runnable {
      def run() { cleanup() }
    },
    cleanupIntervalMillis,
    cleanupIntervalMillis,
    TimeUnit.MILLISECONDS
  )

  logDebug(s"Cleanup task scheduled: $cleanupIntervalMillis sec, $cleanupIntervalMillis sec")

  /** Stop the appender */
  override def stop() {
    super.stop()
    rolloverTask.cancel(true)
    cleanupTask.cancel(true)
    executor.shutdownNow()
  }

  /** Append bytes to file after rolling over is necessary */
  override protected def appendToFile(bytes: Array[Byte], len: Int) {
    if (shouldRollover) {
      rollover()
      shouldRollover = false
    }
    super.appendToFile(bytes, len)
  }

  /** Rollover the file, by closing the output stream and moving it over */
  private def rollover() {
    val rolloverFileName = formatter.format(Calendar.getInstance.getTime)
    val rolloverFile = new File(file.getParentFile, rolloverFileName).getAbsoluteFile
    logDebug("Attempting to rollover at " + System.currentTimeMillis + " to file " + rolloverFile)

    try {
      closeFile()
      if (file.exists) {
        if (!rolloverFile.exists) {
          FileUtils.moveFile(file, rolloverFile)
          logInfo(s"Rolled over $file to $rolloverFile")
        } else {
          // In case the rollover file name clashes, make a unique file name.
          // The resultant file names are long and ugly, so this is used only
          // if there is a name collision. This can be avoided by the using
          // the right pattern.
          val altRolloverFile = new File(
            file.getParent, s"$rolloverFileName-${System.currentTimeMillis}}").getAbsoluteFile
          logWarning(s"Rollover file $rolloverFile already exists, " +
            s"rolled over $file to file $altRolloverFile")
          logWarning(s"Make sure that the given file name pattern [$rolloverFilePattern] " +
            s"generates using file names for the given interval [$rolloverIntervalMillis ms]")
          FileUtils.moveFile(file, altRolloverFile)
        }
      } else {
        logWarning(s"File $file does not exist")
      }
      openFile()
    } catch {
      case e: Exception =>
        logError(s"Error rolling over $file to $rolloverFile", e)
    }
  }

  /** Delete files older than certain interval */
  private[util] def cleanup() {
    try {
      val modTimeThreshold = System.currentTimeMillis - (cleanupTtlMillis * 1000)

      val oldFiles = file.getParentFile.listFiles(new FileFilter {
        def accept(f: File): Boolean = {
          f.getName.contains(file.getName) && f != file &&
              f.lastModified() < modTimeThreshold
        }
      })
      oldFiles.foreach { _.delete() }
    } catch {
      case e: Exception =>
        logError("Error remove old logs in directory " + file.getParentFile.getAbsolutePath, e)
    }
  }

  private def currentTime(): Long = {
    System.currentTimeMillis / 1000
  }
}

/** RollingFileAppender that rolls over every minute */
private[spark] class MinutelyRollingFileAppender(
    inputStream: InputStream,
    file: File,
    conf: SparkConf
  ) extends RollingFileAppender(
    inputStream,
    file,
    60 * 1000,
    s"'${file.getName}'--YYYY-MM-dd--HH-mm",
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
    60 * 60 * 1000,
    s"'${file.getName}'--YYYY-MM-dd--HH",
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
    24 * 60 * 60 * 1000,
    s"'${file.getName}'--YYYY-MM-dd",
    conf
  )