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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingConf._
import org.apache.spark.util.Utils

/** A helper class with utility functions related to the WriteAheadLog interface */
private[streaming] object WriteAheadLogUtils extends Logging {

  def enableReceiverLog(conf: SparkConf): Boolean = {
    conf.get(RECEIVER_WAL_ENABLE_CONF_KEY)
  }

  def getRollingIntervalSecs(conf: SparkConf, isDriver: Boolean): Int = {
    if (isDriver) {
      conf.get(DRIVER_WAL_ROLLING_INTERVAL_CONF_KEY)
    } else {
      conf.get(RECEIVER_WAL_ROLLING_INTERVAL_CONF_KEY)
    }
  }

  def getMaxFailures(conf: SparkConf, isDriver: Boolean): Int = {
    if (isDriver) {
      conf.get(DRIVER_WAL_MAX_FAILURES_CONF_KEY)
    } else {
      conf.get(RECEIVER_WAL_MAX_FAILURES_CONF_KEY)
    }
  }

  def isBatchingEnabled(conf: SparkConf, isDriver: Boolean): Boolean = {
    isDriver && conf.get(DRIVER_WAL_BATCHING_CONF_KEY)
  }

  /**
   * How long we will wait for the wrappedLog in the BatchedWriteAheadLog to write the records
   * before we fail the write attempt to unblock receivers.
   */
  def getBatchingTimeout(conf: SparkConf): Long = {
    conf.get(DRIVER_WAL_BATCHING_TIMEOUT_CONF_KEY)
  }

  def shouldCloseFileAfterWrite(conf: SparkConf, isDriver: Boolean): Boolean = {
    if (isDriver) {
      conf.get(DRIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY)
    } else {
      conf.get(RECEIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY)
    }
  }

  /**
   * Create a WriteAheadLog for the driver. If configured with custom WAL class, it will try
   * to create instance of that class, otherwise it will create the default FileBasedWriteAheadLog.
   */
  def createLogForDriver(
      sparkConf: SparkConf,
      fileWalLogDirectory: String,
      fileWalHadoopConf: Configuration
    ): WriteAheadLog = {
    createLog(true, sparkConf, fileWalLogDirectory, fileWalHadoopConf)
  }

  /**
   * Create a WriteAheadLog for the receiver. If configured with custom WAL class, it will try
   * to create instance of that class, otherwise it will create the default FileBasedWriteAheadLog.
   */
  def createLogForReceiver(
      sparkConf: SparkConf,
      fileWalLogDirectory: String,
      fileWalHadoopConf: Configuration
    ): WriteAheadLog = {
    createLog(false, sparkConf, fileWalLogDirectory, fileWalHadoopConf)
  }

  /**
   * Create a WriteAheadLog based on the value of the given config key. The config key is used
   * to get the class name from the SparkConf. If the class is configured, it will try to
   * create instance of that class by first trying `new CustomWAL(sparkConf, logDir)` then trying
   * `new CustomWAL(sparkConf)`. If either fails, it will fail. If no class is configured, then
   * it will create the default FileBasedWriteAheadLog.
   */
  private def createLog(
      isDriver: Boolean,
      sparkConf: SparkConf,
      fileWalLogDirectory: String,
      fileWalHadoopConf: Configuration
    ): WriteAheadLog = {

    val classNameOption = if (isDriver) {
      sparkConf.get(DRIVER_WAL_CLASS_CONF_KEY)
    } else {
      sparkConf.get(RECEIVER_WAL_CLASS_CONF_KEY)
    }
    val wal = classNameOption.map { className =>
      try {
        instantiateClass(Utils.classForName[WriteAheadLog](className), sparkConf)
      } catch {
        case NonFatal(e) =>
          throw new SparkException(s"Could not create a write ahead log of class $className", e)
      }
    }.getOrElse {
      new FileBasedWriteAheadLog(sparkConf, fileWalLogDirectory, fileWalHadoopConf,
        getRollingIntervalSecs(sparkConf, isDriver), getMaxFailures(sparkConf, isDriver),
        shouldCloseFileAfterWrite(sparkConf, isDriver))
    }
    if (isBatchingEnabled(sparkConf, isDriver)) {
      new BatchedWriteAheadLog(wal, sparkConf)
    } else {
      wal
    }
  }

  /** Instantiate the class, either using single arg constructor or zero arg constructor */
  private def instantiateClass(cls: Class[_ <: WriteAheadLog], conf: SparkConf): WriteAheadLog = {
    try {
      cls.getConstructor(classOf[SparkConf]).newInstance(conf)
    } catch {
      case nsme: NoSuchMethodException =>
        cls.getConstructor().newInstance()
    }
  }
}
