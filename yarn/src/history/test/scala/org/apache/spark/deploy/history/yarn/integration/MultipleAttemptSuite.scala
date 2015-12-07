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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding._
import org.apache.spark.deploy.history.yarn.server.{TimelineApplicationAttemptInfo, TimelineQueryClient, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.testtools.{HistoryServiceNotListeningToSparkContext, TimelineSingleEntryBatchSize}
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

/**
 * Test handling of multiple attempts in timeline
 */
class MultipleAttemptSuite
    extends AbstractHistoryIntegrationTests
    with HistoryServiceNotListeningToSparkContext
    with TimelineSingleEntryBatchSize {

  var provider: YarnHistoryProvider = null

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    // no window limit, so no windowing of requests
    sparkConf.set(YarnHistoryProvider.OPTION_WINDOW_LIMIT, "0")
  }

  test("Multiple Attempts") {
    describe("Multiple Attempts")

    postMultipleAttempts()
    val hadoopConfiguration = sc.hadoopConfiguration
    val sparkConf = sc.conf

    stopContextAndFlushHistoryService()

    val timeline = historyService.timelineWebappAddress
    val queryClient = new TimelineQueryClient(timeline,
      hadoopConfiguration,
      createClientConfig())

    describe("Querying history service via REST API")

    val entities = awaitListSize(2,
      s"number of entities of type $SPARK_EVENT_ENTITY_TYPE",
      TEST_STARTUP_DELAY,
      () => queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE))

    // now expect/require there to be > 1 job attempt in there
    val timelineEntity = entities.head
    val timelineEntityDescription = describeEntityVerbose(timelineEntity)
    // verify that the first entity has 4 events, that is: a single lifecycle
    val timelineEvents = timelineEntity.getEvents
    assertResult(4,
      s"Number of timeline events in $timelineEntityDescription") {
      timelineEvents.size
    }

    describe("Building attempt history")

    // now read it in via history provider
    provider = new YarnHistoryProvider(sparkConf)
    val history = awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY)
    val info = history.head
    val attempts = info.attempts.map(_.asInstanceOf[TimelineApplicationAttemptInfo])
    assertListSize(attempts, 2, s"number of attempts in $info")
    val attemptListAsText = attempts.mkString("[", ", ", "]")
    val (elt1 :: elt2 :: _) = attempts
    assertCompletedAttempt(elt1)
    assert(elt1.attemptId !== elt2.attemptId)
    // we expect the events to be sorted
    assert(attemptId2.toString === elt1.entityId,
      s"elt1 ID in $attemptListAsText wrong -sort order?")
    assert(attemptId1.toString === elt2.entityId, s"elt2 ID in $attemptListAsText")

    // verify this is picked up
    assertAppCompleted(info, "retrieved info")
    getAppUI(provider, info.id, elt1.attemptId)

    // get a UI from an attempt that doesn't exist
    assertNone(provider.getAppUI(info.id, Some("Nonexistent attempt")), "UI of nonexistent attempt")
  }

  override def afterEach(): Unit = {
    if (provider != null) {
      provider.stop()
    }
    super.afterEach()
  }

}
