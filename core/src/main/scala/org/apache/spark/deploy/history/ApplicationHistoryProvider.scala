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

package org.apache.spark.deploy.history

import java.util.zip.ZipOutputStream

import org.apache.spark.SparkException
import org.apache.spark.ui.SparkUI

private[spark] case class ApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String,
    completed: Boolean = false)

private[spark] case class ApplicationHistoryInfo(
    id: String,
    name: String,
    attempts: List[ApplicationAttemptInfo]) {

  /**
   * Has this application completed?
   * @return true if the most recent attempt has completed
   */
  def completed: Boolean = {
    attempts.nonEmpty && attempts.head.completed
  }
}

/**
 * Case class which can be subclasses by any provider to store information which
 * can then be used to determine whether an application attempt has been updated
 * since the last time it was retrieved.
 *
 * Examples include: a timestamp, a counter or a checksum.
 */
private[history] case class HistoryProviderUpdateState()

/**
 * All the information returned from a call to getAppUI
 * @param ui Spark UI
 * @param timestamp timestamp of the loaded data
 * @param updateState any provider-specific update state
 */
private[history] case class LoadedAppUI(ui: SparkUI,
    timestamp: Long,
    updateState: Option[HistoryProviderUpdateState])

private[history] abstract class ApplicationHistoryProvider {

  /**
   * Returns a list of applications available for the history server to show.
   *
   * @return List of all know applications.
   */
  def getListing(): Iterable[ApplicationHistoryInfo]

  /**
   * Returns the Spark UI for a specific application.
   *
   * @param appId The application ID.
   * @param attemptId The application attempt ID (or None if there is no attempt ID).
   * @return The application's UI, or None if application is not found.
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI]

  /**
   * Called when the server is shutting down.
   */
  def stop(): Unit = { }

  /**
   * Returns configuration data to be shown in the History Server home page.
   *
   * @return A map with the configuration data. Data is show in the order returned by the map.
   */
  def getConfig(): Map[String, String] = Map()

  /**
   * Writes out the event logs to the output stream provided. The logs will be compressed into a
   * single zip file and written out.
   * @throws SparkException if the logs for the app id cannot be found.
   */
  @throws(classOf[SparkException])
  def writeEventLogs(appId: String, attemptId: Option[String], zipStream: ZipOutputStream): Unit

  /**
   * Probe for an update to an (incompleted) application
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @param updateTimeMillis time in milliseconds to use as the threshold for an update.
   * @param data any other data the operations implementation can use to determine age
   * @return true if the application was updated since `updateTimeMillis`
   */
  def isUpdated(appId: String, attemptId: Option[String], updateTimeMillis: Long,
      data: Option[HistoryProviderUpdateState]): Boolean = {
    false
  }

}
