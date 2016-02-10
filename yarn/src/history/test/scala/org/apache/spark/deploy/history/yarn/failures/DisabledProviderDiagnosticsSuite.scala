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
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding
import org.apache.spark.deploy.history.yarn.server.{TimelineQueryClient, YarnHistoryProvider}

class DisabledProviderDiagnosticsSuite extends AbstractHistoryIntegrationTests {

  /**
   * Create a history provider instance.
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    val yarnConf = sc.hadoopConfiguration

    val client = new TimelineQueryClient(timelineRootEndpoint(),
      yarnConf,
      JerseyBinding.createClientConfig())
    new DisabledFailingYarnHistoryProvider(client, false, client.getTimelineURI(), conf)
  }

  def timelineRootEndpoint(): URI = {
    val realTimelineEndpoint = getTimelineEndpoint(sc.hadoopConfiguration).toURL
    new URL(realTimelineEndpoint, "/").toURI
  }

  /**
   * When the UI is disabled, the GET works but there's an error message
   * warning of the fact. The endpoint check is <i>not</i> reached.
   */
  test("Probe Disabled UI") {
    def probeDisabledUI(webUI: URL, provider: YarnHistoryProvider): Unit = {
      val fp = provider.asInstanceOf[FailingYarnHistoryProvider]
      probeEmptyWebUI(webUI, provider)
      val body = getHtmlPage(webUI, YarnHistoryProvider.TEXT_SERVICE_DISABLED :: Nil)
    }
    webUITest("Probe Disabled UI", probeDisabledUI)
  }

}
