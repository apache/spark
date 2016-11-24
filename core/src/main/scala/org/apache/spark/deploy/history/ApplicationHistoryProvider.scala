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
 *  A probe which can be invoked to see if a loaded Web UI has been updated.
 *  The probe is expected to be relative purely to that of the UI returned
 *  in the same [[LoadedAppUI]] instance. That is, whenever a new UI is loaded,
 *  the probe returned with it is the one that must be used to check for it
 *  being out of date; previous probes must be discarded.
 */
private[history] abstract class HistoryUpdateProbe {
  /**
   * Return true if the history provider has a later version of the application
   * attempt than the one against this probe was constructed.
   * @return
   */
  def isUpdated(): Boolean
}

/**
 * All the information returned from a call to `getAppUI()`: the new UI
 * and any required update state.
 * @param ui Spark UI
 * @param updateProbe probe to call to check on the update state of this application attempt
 */
private[history] case class LoadedAppUI(
    ui: SparkUI,
    updateProbe: () => Boolean)

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
   * @return a [[LoadedAppUI]] instance containing the application's UI and any state information
   *         for update probes, or `None` if the application/attempt is not found.
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
   * @return the [[ApplicationHistoryInfo]] for the appId if it exists.
   */
  def getApplicationInfo(appId: String): Option[ApplicationHistoryInfo]

}
