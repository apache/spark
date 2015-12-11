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

package org.apache.spark.deploy.history.yarn.integration

import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.yarn.api.records.ApplicationReport

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._

/**
 * A subclass of the history provider which uses the `time` field rather than
 * the current clock. This is needed to reliably test windowed operations
 * and other actions in which the clock is checked.
 *
 * It also stubs the YARN client calls of the [[YarnHistoryProvider]], and doesn't need
 * a YARN cluster connection to work. It also permits tests to alter the list of running apps
 * to simulate failures.
 *
 * @param sparkConf configuration of the provider
 * @param startTime the start time (millis)
 * @param tickInterval amount to increase on a `tick()`
 * @param _livenessChecksEnabled are liveness checks enabled
 */
class TimeManagedHistoryProvider(
    sparkConf: SparkConf,
    var startTime: Long = 0L,
    var tickInterval: Long = 1000L,
    _livenessChecksEnabled: Boolean = false)
    extends YarnHistoryProvider(sparkConf){

  private val time = new AtomicLong(startTime)

  /**
   * Is the timeline service (and therefore this provider) enabled.
   * @return true : always
   */
  override def enabled: Boolean = {
    true
  }

  /**
   * Return the current time
   * @return
   */
  override def now(): Long = {
    time.get()
  }

  def setTime(t: Long): Unit = {
    time.set(t)
  }

  /**
   * Increase the time by one tick
   * @return the new value
   */
  def tick(): Long = {
    incrementTime(tickInterval)
  }

  def incrementTime(t: Long): Long = {
    time.addAndGet(t)
  }

  private var runningApps: Seq[ApplicationReport] = List()

  override protected def initYarnClient(): Unit = {}

  override def livenessChecksEnabled: Boolean = {
    _livenessChecksEnabled
  }

  /**
   * List spark applications
   * @return the list of running spark applications, which can then be filtered against
   */
  override def listYarnSparkApplications(): Map[String, ApplicationReport] = {
    synchronized {
      reportsToMap(runningApps)
    }
  }

  /**
   * List spark applications
   * @return the list of running spark applications, which can then be filtered against
   */
  def setRunningApplications(apps: Seq[ApplicationReport]): Unit = {
    synchronized {
      runningApps = apps
    }
  }
}
