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

package org.apache.spark.deploy.history.yarn.failures

import java.net.{URI, URL}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.integration.AbstractHistoryIntegrationTests
import org.apache.spark.deploy.history.yarn.rest.{HttpRequestException, JerseyBinding}
import org.apache.spark.deploy.history.yarn.server.{TimelineQueryClient, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

/**
 * Test reporting of connectivity problems to the caller, specifically how
 * the `YarnHistoryProvider` handles the initial binding & reporting of problems.
 *
 */
class WebsiteDiagnosticsSuite extends AbstractHistoryIntegrationTests {

  var failingHistoryProvider: FailingYarnHistoryProvider = _

  /**
   * Create a failing history provider instance, with the flag set to say "the initial
   * endpoint check" has not been executed.
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    val yarnConf = sc.hadoopConfiguration

    val client = new TimelineQueryClient(timelineRootEndpoint(),
                             yarnConf, JerseyBinding.createClientConfig())
    failingHistoryProvider = new
            FailingYarnHistoryProvider(client, false, client.getTimelineURI(), conf)
    failingHistoryProvider
  }

  def timelineRootEndpoint(): URI = {
    val realTimelineEndpoint = getTimelineEndpoint(sc.hadoopConfiguration).toURL
    new URL(realTimelineEndpoint, "/").toURI
  }

  /**
   * Issue a GET request against the Web UI and expect it to fail with an error
   * message indicating that `text/html` is not a supported type.
   * with error text indicating it was in the endpoint check
   * @param webUI URL to the web UI
   * @param provider the provider
   */
  def expectApplicationLookupToFailInEndpointCheck(webUI: URL,
      provider: YarnHistoryProvider): Unit = {
    val connector = createUrlConnector()
    val appURL = new URL(webUI, "/history/app-0001")
    describe(s"Expecting endpoint checks to fail while retrieving $appURL")
    awaitURL(webUI, TEST_STARTUP_DELAY)
    try {
      assert(!failingHistoryProvider.endpointCheckSuccess())
      val body = getHtmlPage(appURL, Nil)
      fail(s"Expected a failure from GET $appURL -but got\n$body")
    } catch {
      case ex: HttpRequestException =>
        assertContains(ex.toString, TimelineQueryClient.MESSAGE_CHECK_URL)
    }
  }

  test("Probe UI with Endpoint check") {
    def probeUIWithFailureCaught(webUI: URL, provider: YarnHistoryProvider): Unit = {
      awaitURL(webUI, TEST_STARTUP_DELAY)
      getHtmlPage(webUI, YarnHistoryProvider.TEXT_NEVER_UPDATED :: Nil)
    }
    webUITest("Probe UI with Endpoint check", probeUIWithFailureCaught)
  }

  test("Probe App ID with Endpoint check") {
    def expectAppIdToFail(webUI: URL, provider: YarnHistoryProvider): Unit = {
      expectApplicationLookupToFailInEndpointCheck(webUI, provider)
    }
    webUITest("Probe App ID with Endpoint check", expectAppIdToFail)
  }

}
