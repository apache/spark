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

package org.apache.spark.deploy.history.yarn.unit

import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import org.apache.spark.SparkContext
import org.apache.spark.deploy.history.yarn.YarnHistoryService
import org.apache.spark.deploy.history.yarn.testtools.{AbstractYarnHistoryTests, TimelineServiceEnabled}
import org.apache.spark.scheduler.cluster.SchedulerExtensionServiceBinding

/**
 * Mock histories have the timeline service enabled by default -it is
 * a mock one though.
 */
class AbstractMockHistorySuite() extends AbstractYarnHistoryTests
    with TimelineServiceEnabled with MockitoSugar {

  protected var timelineClient: TimelineClient = _

  protected var response: TimelinePutResponse = _

  /**
   * Set up the mock timeline client and response instances.
   */
  override protected def setup(): Unit = {
    super.setup()
    timelineClient = mock[TimelineClient]
    response = mock[TimelinePutResponse]
    when(timelineClient.putEntities(any(classOf[TimelineEntity]))).thenReturn(response)
  }

  override def afterEach(): Unit = {
    ServiceOperations.stopQuietly(timelineClient)
    super.afterEach()
  }

  /**
   * Create and start a history service.
   * @param sc context
   * @param id application ID
   * @param attemptId
   * @return the instantiated service
   */
  override protected def startHistoryService(
      sc: SparkContext,
      id: ApplicationId,
      attemptId: Option[ApplicationAttemptId] = None): YarnHistoryService = {
    val service = spy(new YarnHistoryService())
    assertNotNull(timelineClient, "timeline client")
    doReturn(timelineClient).when(service).createTimelineClient()
    service.start(SchedulerExtensionServiceBinding(sc, id, attemptId))
    service
  }

}
