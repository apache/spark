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

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.zip.ZipOutputStream

import scala.xml.Node

import org.apache.spark.SparkException
import org.apache.spark.status.api.v1.ApplicationInfo
import org.apache.spark.ui.SparkUI

/**
 * A loaded UI for a Spark application.
 *
 * Loaded UIs are valid once created, and can be invalidated once the history provider detects
 * changes in the underlying app data (e.g. an updated event log). Invalidating a UI does not
 * unload it; it just signals the [[ApplicationCache]] that the UI should not be used to serve
 * new requests.
 *
 * Reloading of the UI with new data requires collaboration between the cache and the provider;
 * the provider invalidates the UI when it detects updated information, and the cache invalidates
 * the cache entry when it detects the UI has been invalidated. That will trigger a callback
 * on the provider to finally clean up any UI state. The cache should hold read locks when
 * using the UI, and the provider should grab the UI's write lock before making destructive
 * operations.
 *
 * Note that all this means that an invalidated UI will still stay in-memory, and any resources it
 * references will remain open, until the cache either sees that it's invalidated, or evicts it to
 * make room for another UI.
 *
 * @param ui Spark UI
 */
private[history] case class LoadedAppUI(ui: SparkUI) {

  val lock = new ReentrantReadWriteLock()

  @volatile private var _valid = true

  def valid: Boolean = _valid

  def invalidate(): Unit = {
    lock.writeLock().lock()
    try {
      _valid = false
    } finally {
      lock.writeLock().unlock()
    }
  }

}

private[history] abstract class ApplicationHistoryProvider {

  /**
   * Returns the count of application event logs that the provider is currently still processing.
   * History Server UI can use this to indicate to a user that the application listing on the UI
   * can be expected to list additional known applications once the processing of these
   * application event logs completes.
   *
   * A History Provider that does not have a notion of count of event logs that may be pending
   * for processing need not override this method.
   *
   * @return Count of application event logs that are currently under process
   */
  def getEventLogsUnderProcess(): Int = {
    0
  }

  /**
   * Returns the time the history provider last updated the application history information
   *
   * @return 0 if this is undefined or unsupported, otherwise the last updated time in millis
   */
  def getLastUpdatedTime(): Long = {
    0
  }

  /**
   * Returns a list of applications available for the history server to show.
   *
   * @return List of all know applications.
   */
  def getListing(): Iterator[ApplicationInfo]

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
   * @return the [[ApplicationInfo]] for the appId if it exists.
   */
  def getApplicationInfo(appId: String): Option[ApplicationInfo]

  /**
   * @return html text to display when the application list is empty
   */
  def getEmptyListingHtml(): Seq[Node] = Seq.empty

  /**
   * Called when an application UI is unloaded from the history server.
   */
  def onUIDetached(appId: String, attemptId: Option[String], ui: SparkUI): Unit = { }

}
