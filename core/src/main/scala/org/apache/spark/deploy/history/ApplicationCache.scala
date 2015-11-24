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

import java.util.NoSuchElementException

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}

import org.apache.spark.Logging
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Clock

/**
 * Cache for applications.
 *
 * Completed applications are cached for as long as there is capacity for them.
 * Incompleted applications have their update time checked on every
 * retrieval; if the cached entry is out of date, it is refreshed.
 *
 * @param operations implementation of record access operations
 * @param refreshInterval interval between refreshes in nanoseconds.
 * @param retainedApplications number of retained applications
 * @param time time source
 */
private[history] class ApplicationCache(operations: ApplicationCacheOperations,
    val refreshInterval: Long,
    val retainedApplications: Int,
    time: Clock) extends RemovalListener[String, CacheEntry] with Logging {

  /**
   * Services the load request from the cache.
   */
  private val appLoader = new CacheLoader[String, CacheEntry] {
    override def load(key: String): CacheEntry = {
      loadEntry(key)
    }
  }

  /**
   * The cache of applications.
   */
  private val appCache: LoadingCache[String, CacheEntry] = CacheBuilder.newBuilder()
      .maximumSize(retainedApplications)
      .removalListener(this)
      .build(appLoader)

  /**
   * Load a cache entry, including registering the UI
   *
   * @param appAndAttempt combined app/attempt key
   * @return the cache entry
   */
  def loadEntry(appAndAttempt: String): CacheEntry = {
    val parts = splitAppAndAttemptKey(appAndAttempt)
    loadApplicationEntry(parts._1, parts._2)
  }

  /**
   * Load a cache entry, including registering the UI
   *
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the cache entry
   */
  def loadApplicationEntry(appId: String, attemptId: Option[String]): CacheEntry = {
    operations.getAppUI(appId, attemptId) match {
      case Some(ui) =>
        val completed = ui.getApplicationInfoList.exists(_.attempts.last.completed)
        // attach the spark UI
        operations.attachSparkUI(ui, completed)
        // build the cache entry
        CacheEntry(ui, completed, time.getTimeMillis())
      case None =>
        throw new NoSuchElementException(s"no application $appId attempt $attemptId")
    }
  }

  /**
   * Split up an `applicationId/attemptId` or `applicationId` key into the separate pieces.
   *
   * @param appAndAttempt combined key
   * @return a tuple of the application ID and, if present, the attemptID
   */
  def splitAppAndAttemptKey(appAndAttempt: String): (String, Option[String]) = {
    val parts = appAndAttempt.split("/")
    require(parts.length == 1 || parts.length == 2, s"Invalid app key $appAndAttempt")
    val appId = parts(0)
    val attemptId = if (parts.length > 1) Some(parts(1)) else None
    (appId, attemptId)
  }

  /**
   * Merge an appId and optional attempt ID into a key of the form `applicationId/attemptId`
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return
   */
  def mergeAppAndAttemptToKey(appId: String, attemptId: Option[String]) : String = {
    appId + attemptId.map { id => s"/$id" }.getOrElse("")
  }

  /**
   * Get the entry. Cache fetch/refresh will have taken place by
   * the time this method returns
   * @param appAndAttempt application to look up
   * @return the entry
   */
  @Deprecated
  def get(appAndAttempt: String): CacheEntry = {
    val parts = splitAppAndAttemptKey(appAndAttempt)
    get(parts._1, parts._2)
  }

  /**
   * Get the entry. Cache fetch/refresh will have taken place by
   * the time this method returns
   * @return the entry
   */
  def get(appId: String, attemptId: Option[String]): CacheEntry = {
    val appAndAttempt = mergeAppAndAttemptToKey(appId, attemptId)
    val entry = appCache.get(appAndAttempt)
    if (!entry.completed &&
        (time.getTimeMillis() - entry.timestamp) > refreshInterval
        && operations.isUpdated(appId, attemptId, entry.timestamp)) {
        logDebug(s"refreshing $appAndAttempt")
        appCache.invalidate(appAndAttempt)
        appCache.get(appAndAttempt)
    } else {
      entry
    }
  }

  /**
   * Removal event notifies the provider to detach the UI
   * @param rm removal notification
   */
  override def onRemoval(rm: RemovalNotification[String, CacheEntry]): Unit = {
    operations.detachSparkUI(rm.getValue().ui)
  }

  /**
   * String operator dumps the cache entries.
   *
   * @return
   */
  override def toString: String = {
    val sb = new StringBuilder(
      s"ApplicationCache($refreshInterval, $retainedApplications) size ${appCache.size() }")
    for (elt <- appCache.asMap().values().asScala) {
      sb.append(s" ${elt.ui.appName}->$elt")
    }
    sb.toString()
  }
}

/**
 * An entry in the cache.
 *
 * @param ui Spark UI
 * @param completed: flag to indicated that the application has completed (and so
 *                 does not need refreshing)
 * @param timestamp timestamp in nanos
 */
private[history] case class CacheEntry(ui: SparkUI, completed: Boolean, timestamp: Long)

private[history] case class CacheKey(appId: String, attemptId: Option[String]) {
  override def hashCode(): Int = appId.hashCode()
    + attemptId.map(_.hashCode).getOrElse(0)

  override def equals(obj: scala.Any): Boolean = {
    val that = obj.asInstanceOf[CacheKey]
    that.appId == appId && that.attemptId == attemptId
  }
}

/**
 * API for cache events
 */
private[history] trait ApplicationCacheOperations {

  /**
   * Get the application UI
   * @param appId application ID
   * @param attemptId attempt ID
   * @return The Spark UI
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[SparkUI]

  /**
   *  Attach a reconstructed UI.
   *
   * @param ui UI
   * @param completed flag to indicate that the UI has completed
   */
  def attachSparkUI(ui: SparkUI, completed: Boolean): Unit

  /**
   *  Detach a reconstructed UI
   *
   * @param ui Spark UI
   */
  def detachSparkUI(ui: SparkUI): Unit

  /**
   * Probe for an update to an (incompleted) application
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @param updateTimeMillis time in milliseconds to use as the threshold for an update.
   * @return true if the application was updated since `updateTimeMillis`
   */
  def isUpdated(appId: String, attemptId: Option[String], updateTimeMillis: Long): Boolean
}
