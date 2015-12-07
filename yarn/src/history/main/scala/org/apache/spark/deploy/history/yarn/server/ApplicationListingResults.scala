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

package org.apache.spark.deploy.history.yarn.server

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._

/**
 * (Immutable) results of a list operation.
 *
 * @param timestamp timestamp in milliseconds
 * @param applications application listing. These must be pre-sorted
 * @param failureCause optional exception raised (implies operation was a failure)
 */
private[spark] class ApplicationListingResults(
    val timestamp: Long,
    val applications: Seq[TimelineApplicationHistoryInfo],
    val failureCause: Option[Throwable]) extends Logging {

  /**
   * Predicate which is true if the listing failed; that there
   * is a failure cause value.
   *
   * @return true if the listing failed
   */
  def failed: Boolean = { failureCause.isDefined }

  /**
   * Did the listing operation succeed? The opposite of [[failed]]
   * @return true if the last operation did not fail
   */
  def succeeded: Boolean = { !failed }

  /**
   * Get an updated time for display.
   * @return a printable date/time value or or "never"
   */
  def updated: String = {
    humanDateCurrentTZ(timestamp, YarnHistoryProvider.TEXT_NEVER_UPDATED)
  }

  /**
   * The number of applications in the list.
   *
   * @return the list size
   */
  def size: Int = {
    applications.size
  }

  /**
   * Look up an application by its ID
   * @param applicationId application ID
   * @return (app, attempt) options.
   */
  def lookup(applicationId: String): Option[TimelineApplicationHistoryInfo] = {
    applications.find(_.id == applicationId)
  }

  /**
   * Look up an attempt from the attempt ID passed down in the spark history
   * server `getSparkUI()` operations; return the tuple of `(app, attempt, attempts)`.
   *
   * `None` for app means: the application wasn't found; in this case
   * `attempt` is always `None`, `attempts` empty.
   *
   * If the application is set, but the attempt undefined, then the application could be found,
   * but not the specific attempt. In this case, the `attempts` sequence will list all known
   * attempts.
   * @param appId application ID
   * @param attemptId attempt ID
   * @return (app, attempt, attempt) options.
   */
  def lookupAttempt(appId: String, attemptId: Option[String]):
  (Option[TimelineApplicationHistoryInfo], Option[TimelineApplicationAttemptInfo],
      List[TimelineApplicationAttemptInfo] ) = {
    val foundApp = lookup(appId)
    if (foundApp.isEmpty) {
      return (None, None, Nil)
    }
    val attempts = foundApp.get.attempts
    if (attempts.isEmpty) {
      return (foundApp, None, attempts)
    }
    // henceforth attempts is always non-empty.
    // look to see if there's an attempt ID

    if (attemptId.isEmpty) {
      return (foundApp, Some(attempts.head), attempts)
    }

    // here there i
    val entityId = attemptId.get

    // scan the list of app attempts to ensure that the attempt is associated
    // with the app; return no match if it is not
    val attemptInfo = attempts.find( _.attemptId.get == entityId)
    (foundApp, attemptInfo, attempts)
  }
}
