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

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._

/**
 * Defines the policy based on which [[org.apache.spark.util.logging.RollingFileAppender]] will
 * generate rolling files.
 */
private[spark] trait RollingPolicy {

  /** Whether rollover should be initiated at this moment */
  def shouldRollover(bytesToBeWritten: Long): Boolean

  /** Notify that rollover has occurred */
  def rolledOver(): Unit

  /** Notify that bytes have been written */
  def bytesWritten(bytes: Long): Unit

  /** Get the desired name of the rollover file */
  def generateRolledOverFileSuffix(): String
}

/**
 * Defines a [[org.apache.spark.util.logging.RollingPolicy]] by which files will be rolled
 * over at a fixed interval.
 */
private[spark] class TimeBasedRollingPolicy(
    var rolloverIntervalMillis: Long,
    rollingFileSuffixPattern: String,
    checkIntervalConstraint: Boolean = true   // set to false while testing
  ) extends RollingPolicy with Logging {

  import TimeBasedRollingPolicy._
  if (checkIntervalConstraint && rolloverIntervalMillis < MINIMUM_INTERVAL_SECONDS * 1000L) {
    logWarning(log"Rolling interval [${MDC(TIME_UNITS, rolloverIntervalMillis)} " +
      log"ms] is too small. Setting the interval to the acceptable minimum of " +
      log"${MDC(MIN_TIME, MINIMUM_INTERVAL_SECONDS * 1000)} ms.")
    rolloverIntervalMillis = MINIMUM_INTERVAL_SECONDS * 1000L
  }

  @volatile private var nextRolloverTime = calculateNextRolloverTime()
  private val formatter = new SimpleDateFormat(rollingFileSuffixPattern, Locale.US)

  /** Should rollover if current time has exceeded next rollover time */
  def shouldRollover(bytesToBeWritten: Long): Boolean = {
    System.currentTimeMillis > nextRolloverTime
  }

  /** Rollover has occurred, so find the next time to rollover */
  def rolledOver(): Unit = {
    nextRolloverTime = calculateNextRolloverTime()
    logDebug(s"Current time: ${System.currentTimeMillis}, next rollover time: " + nextRolloverTime)
  }

  def bytesWritten(bytes: Long): Unit = { }  // nothing to do

  private def calculateNextRolloverTime(): Long = {
    val now = System.currentTimeMillis()
    val targetTime = (
      math.ceil(now.toDouble / rolloverIntervalMillis) * rolloverIntervalMillis
    ).toLong
    logDebug(s"Next rollover time is $targetTime")
    targetTime
  }

  def generateRolledOverFileSuffix(): String = {
    formatter.format(Calendar.getInstance.getTime)
  }
}

private[spark] object TimeBasedRollingPolicy {
  val MINIMUM_INTERVAL_SECONDS = 60L  // 1 minute
}

/**
 * Defines a [[org.apache.spark.util.logging.RollingPolicy]] by which files will be rolled
 * over after reaching a particular size.
 */
private[spark] class SizeBasedRollingPolicy(
    var rolloverSizeBytes: Long,
    checkSizeConstraint: Boolean = true     // set to false while testing
  ) extends RollingPolicy with Logging {

  import SizeBasedRollingPolicy._
  if (checkSizeConstraint && rolloverSizeBytes < MINIMUM_SIZE_BYTES) {
    logWarning(log"Rolling size [${MDC(NUM_BYTES, rolloverSizeBytes)} bytes] is too small. " +
      log"Setting the size to the acceptable minimum of ${MDC(MIN_SIZE, MINIMUM_SIZE_BYTES)} " +
      log"bytes.")
    rolloverSizeBytes = MINIMUM_SIZE_BYTES
  }

  @volatile private var bytesWrittenSinceRollover = 0L
  val formatter = new SimpleDateFormat("--yyyy-MM-dd--HH-mm-ss--SSSS", Locale.US)

  /** Should rollover if the next set of bytes is going to exceed the size limit */
  def shouldRollover(bytesToBeWritten: Long): Boolean = {
    logDebug(s"$bytesToBeWritten + $bytesWrittenSinceRollover > $rolloverSizeBytes")
    bytesToBeWritten + bytesWrittenSinceRollover > rolloverSizeBytes
  }

  /** Rollover has occurred, so reset the counter */
  def rolledOver(): Unit = {
    bytesWrittenSinceRollover = 0
  }

  /** Increment the bytes that have been written in the current file */
  def bytesWritten(bytes: Long): Unit = {
    bytesWrittenSinceRollover += bytes
  }

  /** Get the desired name of the rollover file */
  def generateRolledOverFileSuffix(): String = {
    formatter.format(Calendar.getInstance.getTime)
  }
}

private[spark] object SizeBasedRollingPolicy {
  val MINIMUM_SIZE_BYTES = RollingFileAppender.DEFAULT_BUFFER_SIZE * 10
}

