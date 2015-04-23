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

package org.apache.spark.streaming.util

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkException}

/** A helper class with utility functions related to the WriteAheadLog interface */
private[streaming] object WriteAheadLogUtils extends Logging {

  /**
   * Create a WriteAheadLog based on the value of the given config key. The config key is used
   * to get the class name from the SparkConf. If the class is configured, it will try to
   * create instance of that class by first trying `new CustomWAL(sparkConf, logDir)` then trying
   * `new CustomWAL(sparkConf)`. If either fails, it will fail. If no class is configured, then
   * it will create the default FileBasedWriteAheadLog.
   */
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

  /**
   * Create a WriteAheadLog for the driver. If configured with custom WAL class, it will try
   * to create instance of that class, otherwise it will create the default FileBasedWriteAheadLog.
   */
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

  /**
   * Create a WriteAheadLog for the receiver. If configured with custom WAL class, it will try
   * to create instance of that class, otherwise it will create the default FileBasedWriteAheadLog.
   */
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
