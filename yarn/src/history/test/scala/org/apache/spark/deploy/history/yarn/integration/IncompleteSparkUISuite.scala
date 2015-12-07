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

import java.net.URL

import org.apache.spark.deploy.history.yarn.YarnEventListener
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.util.Utils

/**
 * test to see that incomplete spark UIs are handled
 */
class IncompleteSparkUISuite extends AbstractHistoryIntegrationTests {

  test("incomplete UI must not be cached") {
    def submitAndCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {
      val connector = createUrlConnector()
      val incompleteAppsURL = new URL(webUI, "/?" + incomplete_flag)
      def listIncompleteApps: String = {
        connector.execHttpOperation("GET", incompleteAppsURL, null, "").responseBody
      }
      historyService = startHistoryService(sc)
      val listener = new YarnEventListener(sc, historyService)
      // initial view has no incomplete applications
      assertContains(listIncompleteApps, no_incomplete_applications)

      val startTime = now()

      val started = appStartEvent(startTime,
        sc.applicationId,
        Utils.getCurrentUserName(),
        None)
      listener.onApplicationStart(started)
      val jobId = 2
      listener.onJobStart(jobStartEvent(startTime + 1 , jobId))
      awaitEventsProcessed(historyService, 2, 2000)
      flushHistoryServiceToSuccess()

      // listing
      awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY)

      // here we can do a GET of the application history and expect to see something incomplete

      // check for work in progress
      awaitURLDoesNotContainText(connector, incompleteAppsURL,
        no_completed_applications, TEST_STARTUP_DELAY, "expecting incomplete app listed")

      val yarnAttemptId = attemptId.toString
      val webAttemptId = "1"
      val webAppId = historyService.applicationId.toString

      val attemptPath = s"/history/$webAppId/$webAttemptId"
      // GET the app
      val attemptURL = new URL(webUI, attemptPath)
      val appUIBody = connector.execHttpOperation("GET", attemptURL).responseBody
      assertContains(appUIBody, APP_NAME, s"application name in $attemptURL")
      // look for active jobs marker
      assertContains(appUIBody, activeJobsMarker, s"active jobs string in $attemptURL")

      logInfo("Ending job and application")
      // job completion event
      listener.onJobEnd(jobSuccessEvent(startTime + 1, jobId))
      // stop the app
      historyService.stop()
      awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)

      flushHistoryServiceToSuccess()

      // spin for a refresh event
      awaitRefreshExecuted(provider, true, TEST_STARTUP_DELAY)

      // root web UI declares it complete
      awaitURLDoesNotContainText(connector, webUI,
        no_completed_applications, TEST_STARTUP_DELAY,
        s"expecting application listed as completed")

      // final view has no incomplete applications
      assertContains(listIncompleteApps, no_incomplete_applications,
        "incomplete applications still in list view")

      // the underlying timeline entity
      val entity = provider.getTimelineEntity(yarnAttemptId)

      val history = awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY).head

      val historyDescription = describeApplicationHistoryInfo(history)
      assert(1 === history.attempts.size, "wrong number of app attempts ")
      val attempt1 = history.attempts.head
      assert(attempt1.completed,
        s"application attempt considered incomplete: $historyDescription")

      // get the final app UI
      val finalAppUIPage = connector.execHttpOperation("GET", attemptURL, null, "").responseBody
      assertContains(finalAppUIPage, APP_NAME, s"Application name $APP_NAME not found" +
          s" at $attemptURL")

/* DISABLED. SPARK-7889
      // the active jobs section must no longer exist
      assertDoesNotContain(finalAppUIPage, activeJobsMarker,
        s"Web UI $attemptURL still declared active")

      // look for the completed job
      assertContains(finalAppUIPage, completedJobsMarker,
        s"Web UI $attemptURL does not declare completed jobs")
*/
    }
    webUITest("submit and check", submitAndCheck)
  }

}
