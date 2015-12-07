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

import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding._
import org.apache.spark.deploy.history.yarn.server.{TimelineQueryClient, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.testtools.{HistoryServiceListeningToSparkContext, TimelineSingleEntryBatchSize}
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.util.Utils

/**
 * full hookup from spark context to timeline then reread.
 */
class ContextToHistoryProviderSuite
    extends AbstractHistoryIntegrationTests
    with HistoryServiceListeningToSparkContext
    with TimelineSingleEntryBatchSize {

  test("Stop Event via Context") {
    describe("Stop Event via Context")
    var provider: YarnHistoryProvider = null
    try {
      // hook up to spark context
      historyService = startHistoryService(sc)
      assert(historyService.listening, s"listening $historyService")
      assertResult(1, s"batch size in $historyService") {
        historyService.batchSize
      }
      assert(historyService.bondedToATS, s"not bonded to ATS: $historyService")
      // post in an app start
      var flushes = 0
      logDebug("posting app start")
      val startTime = now()
      val event = appStartEvent(startTime, sc.applicationId, Utils.getCurrentUserName())
      enqueue(event)
      flushes += 1
      awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)
      // closing context generates an application stop
      describe("stopping context")
      sc.stop()
      flushHistoryServiceToSuccess()

      val timeline = historyService.timelineWebappAddress
      val queryClient = new TimelineQueryClient(timeline,
           sc.hadoopConfiguration, createClientConfig())
      val entities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE)
      logInfo(s"Entity listing returned ${entities.size} entities")
      entities.foreach { en =>
        logInfo(describeEntityVerbose(en))
      }
      assertResult(1, "number of listed entities (unfiltered)") {
        entities.size
      }
      assertResult(1, "entities listed by app end filter") {
        queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
            primaryFilter = Some((FILTER_APP_END, FILTER_APP_END_VALUE))).size
      }

      assertResult(1, "entities listed by app start filter") {
        queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
          primaryFilter = Some((FILTER_APP_START, FILTER_APP_START_VALUE))).size
      }

      // now read it in via history provider
      provider = new YarnHistoryProvider(sc.conf)
      val history = awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY)
      val info = history.head
      val attempt = info.attempts.head
      assert(attempt.completed, s"application not flagged as completed")
      provider.getAppUI(info.id, attempt.attemptId)
    } finally {
      describe("teardown")
      if (provider != null) {
        provider.stop()
      }
    }
  }

}
