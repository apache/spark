package org.apache.spark.streaming.util

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkException}

object WriteAheadLogUtils extends Logging {
  private def createLog(
      confKeyName: String,
      sparkConf: SparkConf,
      logDirectory: String,
      fileWalHadoopConf: Configuration,
      fileWalRollingIntervalSecs: Int,
      fileWalMaxFailures: Int): WriteAheadLog = {
    try {
      val classNameOption = sparkConf.getOption(confKeyName)
      classNameOption.map { className =>
        val cls = Utils.classForName(className)
        try {
          cls.getConstructor(classOf[SparkConf], classOf[String])
             .newInstance(sparkConf, logDirectory)
             .asInstanceOf[WriteAheadLog]
        } catch {
          case nsme: NoSuchMethodException =>
            cls.getConstructor(classOf[SparkConf])
               .newInstance(sparkConf)
               .asInstanceOf[WriteAheadLog]
        }
      }.getOrElse {

        import FileBasedWriteAheadLog._
        val walConf = sparkConf.clone
        walConf.set(ROLLING_INTERVAL_SECS_CONF_KEY, fileWalRollingIntervalSecs.toString)
        walConf.set(MAX_FAILURES_CONF_KEY, fileWalMaxFailures.toString)
        new FileBasedWriteAheadLog(walConf, logDirectory, fileWalHadoopConf)
      }
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Could not instantiate Write Ahead Log class", e)
    }
  }

  def createLogForDriver(
      sparkConf: SparkConf,
      logDirectory: String,
      fileWalHadoopConf: Configuration,
      fileWalRollingIntervalSecs: Int = 60,
      fileWalMaxFailures: Int = 3): WriteAheadLog = {

    createLog(
      "spark.streaming.driver.writeAheadLog.class",
      sparkConf, logDirectory, fileWalHadoopConf,
      fileWalRollingIntervalSecs, fileWalMaxFailures
    )
  }

  def createLogForReceiver(
      sparkConf: SparkConf,
      logDirectory: String,
      fileWalHadoopConf: Configuration,
      fileWalRollingIntervalSecs: Int = 60,
      fileWalMaxFailures: Int = 3): WriteAheadLog = {
    createLog(
      "spark.streaming.receiver.writeAheadLog.class",
      sparkConf, logDirectory, fileWalHadoopConf,
      fileWalRollingIntervalSecs, fileWalMaxFailures
    )
  }
}
