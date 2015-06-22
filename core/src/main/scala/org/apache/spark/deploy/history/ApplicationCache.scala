/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.history

import java.util.NoSuchElementException

import scala.util.control.NonFatal

import com.google.common.base.Ticker
import com.google.common.cache.{CacheBuilder, RemovalListener, RemovalNotification, CacheLoader}
import scala.collection.JavaConversions._
import org.apache.spark.Logging
import org.apache.spark.ui.SparkUI

/**
 * Cache for applications.
 * Completed applications are cached for as long as there is capacity for them.
 * Incompleted applications have their update time checked on every
 * retrieval; if the cached entry is out of date, it is refreshed.
 * @param refreshInterval interval between refreshes in nanoseconds.
 * @param retainedApplications number of retained applications
 */
private[history] class ApplicationCache(operations: ApplicationCacheOperations,
    val refreshInterval: Long,
    val retainedApplications: Int,
    time: Ticker)
    extends RemovalListener[String, CacheEntry] with Logging {

  private val appLoader = new CacheLoader[String, CacheEntry] {
    override def load(key: String): CacheEntry = {
      loadEntry(key)
    }
  }

  private val appCache = CacheBuilder.newBuilder()
      .maximumSize(retainedApplications)
      .removalListener(this)
      .build(appLoader)


  def loadEntry(key: String): CacheEntry = {
    val parts = key.split("/")
    require(parts.length == 1 || parts.length == 2, s"Invalid app key $key")
    val appId = parts(0)
    val attemptId = if (parts.length > 1) Some(parts(1)) else None
    operations.getAppUI(appId, attemptId) match {
      case Some(ui) =>
        val completed = ui.getApplicationInfoList.exists(_.attempts.last.completed)
        // attach the spark UI
        operations.attachSparkUI(ui, completed)
        // build the cache entry
        CacheEntry(ui, completed, time.read())
      case None =>
        throw new NoSuchElementException(s"no app with key $key")
    }
  }

  /**
   * Get the entry. Cache fetch/refresh will have taken place by
   * the time this method returns
   * @param key key to retrieve
   * @return the entry
   */
  def get(key: String): Option[CacheEntry] = {
    try {
      val entry = appCache.get(key)
      if (!entry.completed &&
          (time.read() - entry.timestamp) > refreshInterval) {
        // trigger refresh
        logDebug(s"refreshing $key")
        operations.refreshTriggered(key, entry.ui)
        appCache.invalidate(key)
        get(key)
      }
      Some(entry)
    } catch {
      case NonFatal(e) => e.getCause() match {
        case nsee: NoSuchElementException =>
          None

        case cause: Exception => throw cause
      }
    }
  }

  /**
   * Removal event notifies the provider to detach the UI
   * @param rm removal notification
   */
  override def onRemoval(rm: RemovalNotification[String, CacheEntry]): Unit = {
    operations.detachSparkUI(rm.getValue().ui)
  }


  override def toString: String = {
    val sb = new StringBuilder(
      s"ApplicationCache($refreshInterval, $retainedApplications) size ${appCache.size() }")
    for (elt <- appCache.asMap().values()) {
      sb.append(s" ${elt.ui.appName}->$elt")
    }
    sb.toString()
  }
}

/**
 * An entry in the cache
 * @param ui Spark UI
 * @param completed: flag to indicated that the application has completed (and so
 *                 does not need refreshing)
 * @param timestamp timestamp in nanos
 */
case class CacheEntry(ui: SparkUI, completed: Boolean, timestamp: Long);

/**
 * Callbacks for cache events
 */
private[history] trait ApplicationCacheOperations {

  /**
   * Get the application UI
   * @param appId application ID
   * @param attemptId attempt ID
   * @return The Spark UI
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[SparkUI]

  /** Attach a reconstructed UI  */
  def attachSparkUI(ui: SparkUI, completed: Boolean): Unit;


  /**
   *  Detach a reconstructed UI
   *
   * @param ui Spark UI
   */
  def detachSparkUI(ui: SparkUI): Unit;


  /**
   * Notification of a refresh. This will be followed by the normal
   * detach/attach operations
   * @param key
   * @param ui
   */
  def refreshTriggered(key: String, ui: SparkUI): Unit;
}
