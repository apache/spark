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

import java.net.URI

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding
import org.apache.spark.deploy.history.yarn.server.{TimelineQueryClient, YarnHistoryProvider}

/**
 * This is a YARN history provider that can be given
 * a (possibly failing) query client, and can be configured
 * as to whether to start with an endpoint check.
 * @param queryClient query client
 * @param skipEndpointCheck should the initial endpoint
 *                             check be skipped? It will if this
 *                             is true
 * @param endpoint URI of the service.
 */
class FailingYarnHistoryProvider(
    queryClient: TimelineQueryClient,
    skipEndpointCheck: Boolean,
    endpoint: URI,
    sparkConf: SparkConf,
    refreshEnabled: Boolean = false) extends YarnHistoryProvider(sparkConf) with Logging {

  init()

  /**
   * Is the timeline service (and therefore this provider) enabled.
   * @return true : always
   */
  override def enabled: Boolean = {
    true
  }

  /**
   * Any initialization logic
   */
  private def init(): Unit = {
    setEndpointChecked(skipEndpointCheck)
  }

  /**
   *
   * @return the endpoint
   */
  override def getEndpointURI: URI = {
    endpoint
  }

  /**
   * @return the `queryClient` field.
   */
  override protected def createTimelineQueryClient(): TimelineQueryClient = {
    queryClient
  }

  /**
   * @return the `queryClient` field.
   */
  override def getTimelineQueryClient: TimelineQueryClient = {
    queryClient
  }

  /**
   * Set the endpoint checked flag to the desired value
   * @param b new value
   */
  def setEndpointChecked(b: Boolean): Unit = {
    endpointCheckExecuted.set(b)
  }


  /**
   * export the endpoint check for testing
   */
  override def maybeCheckEndpoint(): Boolean = {
    super.maybeCheckEndpoint()
  }

  /**
   * Start the refresh thread with the given interval.
   *
   * When this thread exits, it will close the `timelineQueryClient`
   * instance
   */
  override def startRefreshThread(): Unit = {
    if (refreshEnabled) {
      super.startRefreshThread()
    }
  }
}

/**
 * A failing yarn history provider that returns enabled=false, always
 * @param queryClient query client
 * @param endpointCheckExecuted should the initial endpoint
 *                             check be skipped? It will if this
 *                             is true
 * @param endpoint URI of the service.
 * @param sparkConf spark configuration to use
 */
class DisabledFailingYarnHistoryProvider(queryClient: TimelineQueryClient,
    endpointCheckExecuted: Boolean,
    endpoint: URI,
    sparkConf: SparkConf) extends FailingYarnHistoryProvider(
    queryClient, endpointCheckExecuted, endpoint, sparkConf) {

  /**
   * false -always
   */
  override def enabled: Boolean = {
    false
  }
}

/**
 * Some operations to help the failure tests
 */
object FailingYarnHistoryProvider extends Logging {

  def createQueryClient(): FailingTimelineQueryClient = {
    new FailingTimelineQueryClient(new URI("http://localhost:80/"),
      new Configuration(), JerseyBinding.createClientConfig())
  }

  /**
   * This inner provider calls most of its internal methods.
   * @return a failing instance
   */
  def createFailingProvider(sparkConf: SparkConf,
      endpointCheckExecuted: Boolean = false,
      refreshEnabled: Boolean = false): YarnHistoryProvider = {
    new FailingYarnHistoryProvider(createQueryClient(),
      endpointCheckExecuted,
      new URI("http://localhost:80/"),
      sparkConf,
      refreshEnabled)
  }
}
