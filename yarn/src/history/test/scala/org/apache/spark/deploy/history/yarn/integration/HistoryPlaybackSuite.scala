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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryService
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.failures.FailingYarnHistoryProvider
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding
import org.apache.spark.deploy.history.yarn.server.{TimelineQueryClient, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

/**
 * Plays back in a real history from a resource
 */
class HistoryPlaybackSuite extends AbstractHistoryIntegrationTests {

  /** path to the resource with the history */
  val History1 = "org/apache/spark/deploy/history/yarn/integration/history-1.json"
  val EntityCount = 2
  var historyProvider: FailingYarnHistoryProvider = _

  /**
   * Create a history provider bonded to the resource entity list
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    val client = createTimelineQueryClient()
    historyProvider = new FailingYarnHistoryProvider(client, true, client.getTimelineURI(),
      conf, true)
    historyProvider
  }

  protected override def createTimelineQueryClient(): TimelineQueryClient = {
    new ResourceDrivenTimelineQueryClient(
      History1,
      new URL(getTimelineEndpoint(sc.hadoopConfiguration).toURL, "/").toURI,
      sc.hadoopConfiguration,
      JerseyBinding.createClientConfig())
  }

  test("Publish Events and GET the web UI") {
    def examineHistory(webUI: URL, provider: YarnHistoryProvider): Unit = {

      val connector = createUrlConnector()
      val queryClient = historyProvider.getTimelineQueryClient

      val timelineEntities =
        queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE)
      assert(EntityCount === timelineEntities.size,
        s"entities listed count = ${timelineEntities.size}")
      val yarnAppId = "application_1443668830514_0008"

      assertNotNull(yarnAppId, s"Null entityId from $yarnAppId")
      val entity = queryClient.getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, yarnAppId)
      assertNotNull(entity, s"Null entity from $yarnAppId")

      // listing
      awaitApplicationListingSize(provider, EntityCount, TEST_STARTUP_DELAY)

      // resolve to entry
      provider.getAppUI(yarnAppId, Some(yarnAppId)) match {
        case Some(yarnAppUI) =>
        // success
        case None => fail(s"Did not get a UI for $yarnAppId")
      }

      // and look for the complete app
      awaitURL(webUI, TEST_STARTUP_DELAY)
      val completeBody = awaitURLDoesNotContainText(connector, webUI,
        no_completed_applications, TEST_STARTUP_DELAY)
      logInfo(s"GET /\n$completeBody")
      // look for the link
      assertContains(completeBody,
        "<a href=\"/history/application_1443668830514_0008/application_1443668830514_0008\">")

      val appPath = s"/history/$yarnAppId/$yarnAppId"
      // GET the app
      val appURL = new URL(webUI, appPath)
      val appUI = connector.execHttpOperation("GET", appURL, null, "")
      val appUIBody = appUI.responseBody
      logInfo(s"Application\n$appUIBody")
      assertContains(appUIBody, "SparkSQL::10.0.0.143")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/jobs"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/stages"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/storage"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/environment"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/executors"), null, "")
    }

    webUITest("submit and check", examineHistory)
  }

}
