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

import java.net.{NoRouteToHostException, URI}

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryService
import org.apache.spark.deploy.history.yarn.integration.AbstractHistoryIntegrationTests
import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, UnauthorizedRequestException}
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

class TimelineQueryFailureSuite extends AbstractHistoryIntegrationTests {

  /**
   * Create the client and the app server
   * @param conf the hadoop configuration
   */
  override protected def startTimelineClientAndAHS(conf: Configuration): Unit = {
  }

  /**
   * Create a history provider instance
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
     FailingYarnHistoryProvider.createFailingProvider(conf, false)
  }

  /**
   * Verifies that failures are propagated
   */
  test("ClientGETFails") {
    val failingClient = FailingYarnHistoryProvider.createQueryClient()
    intercept[NoRouteToHostException] {
      failingClient.get(new URI("http://localhost:80/"),
                         () => "failed" )
    }
  }

  test("ClientListFails") {
    val failingClient = FailingYarnHistoryProvider.createQueryClient()
    intercept[NoRouteToHostException] {
      failingClient.listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    }
  }

  test("UnauthedClientListFails") {
    val failingClient = new ClientResponseTimelineQueryClient(
      401, "401",
      new URI("http://localhost:80/"),
      new Configuration(),
      JerseyBinding.createClientConfig())

    intercept[UnauthorizedRequestException] {
      failingClient.listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    }
  }

  test("getTimelineEntity to fail") {
    describe("getTimelineEntity to fail")

    // not using intercept[] for better diagnostics on failure (i.e. rethrow the unwanted
    // exception
    var provider: FailingYarnHistoryProvider = null
    try {
      provider = createHistoryProvider(new SparkConf()).asInstanceOf[FailingYarnHistoryProvider]
      provider.setEndpointChecked(true)
      val entity = provider.getTimelineEntity("app1")
      fail(s"Expected failure, got $entity")
    } catch {
      case ioe: NoRouteToHostException =>
        logInfo(s"expected exception caught: $ioe")

    } finally {
      if (provider != null) {
        provider.stop()
      }
    }
  }

}
