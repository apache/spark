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

import scala.collection.JavaConverters._

import org.apache.spark.deploy.history.yarn.{YarnEventListener, YarnHistoryService}
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.server.{TimelineApplicationAttemptInfo, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.util.Utils

/**
 * Set up a listener and feed events in; verify they get through to ATS
 */
class TimelineListenerSuite extends AbstractHistoryIntegrationTests {

  private val appStartFilter = Some((FILTER_APP_START, FILTER_APP_START_VALUE))

  private val appEndFilter = Some((FILTER_APP_END, FILTER_APP_END_VALUE))

  test("Listener Events") {
    describe("Listener events pushed out")
    // listener is still not hooked up to spark context
    historyService = startHistoryService(sc)
    val listener = new YarnEventListener(sc, historyService)
    val startTime = now()
    val contextAppId = sc.applicationId
    val started = appStartEvent(startTime,
                               contextAppId,
                               Utils.getCurrentUserName())
    listener.onApplicationStart(started)
    awaitEventsProcessed(historyService, 1, TEST_STARTUP_DELAY)
    flushHistoryServiceToSuccess()
    historyService.stop()
    awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)
    describe("reading events back")

    val queryClient = createTimelineQueryClient()

    // list all entries
    val entities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE)
    assertResult(1, "number of listed entities") { entities.size }
    assertResult(1, "entities listed by app start filter") {
      queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                primaryFilter = appStartFilter).size
    }
    val timelineEntities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                                primaryFilter = appEndFilter)
    assert(1 === timelineEntities.size, "entities listed by app end filter")
    val expectedAppId = historyService.applicationId.toString
    val expectedEntityId = attemptId.toString
    val entry = timelineEntities.head
    val entryDetails = describeEntity(entry)
    assertResult(expectedEntityId,
      s"no entity of id $expectedEntityId - found $entryDetails") {
      entry.getEntityId
    }
    queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, expectedEntityId)

    // here the events should be in the system
    val provider = new YarnHistoryProvider(sc.conf)
    val history = awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY)
    val info = history.head
    logInfo(s"App history = $info")
    val attempt = info.attempts.head.asInstanceOf[TimelineApplicationAttemptInfo]
    // validate received data matches that saved
    assertResult(started.sparkUser, s"username in $info") {
      attempt.sparkUser
    }
    assertResult(startTime, s"started.time != startTime") {
      started.time
    }
    assertResult(started.time, s"info.startTime != started.time in $info") {
      attempt.startTime
    }
    assertResult(expectedAppId, s"info.id != expectedAppId in $info") {
      info.id
    }
    assert(attempt.endTime> 0, s"end time is ${attempt.endTime} in $info")
    // on a completed app, lastUpdated is the end time
    assert(attempt.lastUpdated >= attempt.endTime,
      s"attempt.lastUpdated  < attempt.endTime time in $info")
    assertResult(started.appName, s"info.name != started.appName in $info") {
      info.name
    }
    // fecth the Spark UI - no attempt ID
    provider.getAppUI(info.id, None)

    // hit the underlying attempt
    val timelineEntity = queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, attempt.entityId)
    val events = timelineEntity.getEvents.asScala.toList
    assertResult(2, s"number of events in ${describeEntity(timelineEntity)}") {
      events.size
    }
    // first event must be the start one
    val sparkListenerEvents = events.map(toSparkEvent).reverse
    val (firstEvent :: secondEvent :: Nil) = sparkListenerEvents
    val fetchedStartEvent = firstEvent.asInstanceOf[SparkListenerApplicationStart]
    assert(started.time === fetchedStartEvent.time, "start time")

    // direct retrieval using Spark context attempt
    queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, expectedEntityId)

  }

  /**
   * This test is like its predecessor except the application is queried while
   * it has not yet completed; we are looking at how that intermediate state is
   * described
   */
  test("Last-Updated time of incompleted app") {
    describe("Last-Updated time of incompleted app")
    // listener is still not hooked up to spark context
    historyService = startHistoryService(sc)
    val timeline = historyService.timelineWebappAddress
    val listener = new YarnEventListener(sc, historyService)
    val startTime = now()
    val userName = Utils.getCurrentUserName()
    val yarnAppId = applicationId.toString()
    val attemptId = attemptId1.toString
    val started = appStartEvent(startTime, appId = yarnAppId,
      user = userName, attempt = Some(attemptId))
    // initial checks to make sure the event is fully inited
    assert(userName === started.sparkUser, s"started.sparkUser")
    assert(Some(yarnAppId) === started.appId, s"started.appId")
    assert(APP_NAME === started.appName, s"started.appName")
    listener.onApplicationStart(started)
    awaitEventsProcessed(historyService, 1, TEST_STARTUP_DELAY)
    flushHistoryServiceToSuccess()

    awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)
    describe("reading events back")
    val queryClient = createTimelineQueryClient()

    // list all entries
    val entities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE)
    assert(1 === entities.size, "number of listed entities")
    val timelineEntities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
        primaryFilter = appStartFilter)
    assert(1 === timelineEntities.size, "entities listed by app start filter")
    assertResult(0, "entities listed by app end filter") {
      queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE, primaryFilter = appEndFilter).size
    }
    val headEntity = timelineEntities.head
    assertResult(attemptId, s"no entry of id $yarnAppId in ${describeEntity(headEntity)}") {
      headEntity.getEntityId
    }

    // first grab the initial entity and extract it manually
    // this helps isolate any unmarshalling problems
    val entity = queryClient.getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, attemptId)
    val entityDetails = describeEntity(entity)
    logInfo(s"Timeline Event = $entityDetails")
    logInfo(s"Timeline Event = ${eventDetails(entity) }")
    val unmarshalledEntity = toApplicationHistoryInfo(entity)
    assert(started.appName === unmarshalledEntity.name,
      s"unmarshalledEntity.name != started.appName in $unmarshalledEntity")
    assert(userName === unmarshalledEntity.attempts.head.sparkUser,
      s"unmarshalledEntity.sparkUser != username in $unmarshalledEntity")

    // here the events should be in the system
    val provider = new YarnHistoryProvider(sc.conf)
    val history = awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY)
    val info = history.head

    logInfo(s"App history = $info")
    val attempt = info.attempts.head

    // validate received data matches that saved

    assert(startTime === started.time, s"started.time != startTime")
    assert(started.time === attempt.startTime, s"attempt.startTime != started.time in $info")
    assert(yarnAppId === info.id, s"info.id != yarnAppId in $info")
    assert(Some(attemptId) === attempt.attemptId, s"Attempt ID in head attempt")
    assert(attempt.endTime === 0, s"end time must be zero in in incompete app $info")
    // on a completed app, lastUpdated is the end time
    assert(attempt.lastUpdated !== 0,
      s"attempt.lastUpdated must be non-zero in incompete app $info")
    assert(started.appName === info.name, s"info.name != started.appName in $info")
    assert(userName === attempt.sparkUser, s"attempt.sparkUser != username in $info")
    // now get the event.

    getAppUI(provider, info.id, attempt.attemptId)

    val timelineEntity = queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, attemptId)
    val events = timelineEntity.getEvents.asScala.toList
    assert(1 === events.size, s"number of events in ${describeEntity(timelineEntity) }")
    // first event must be the start one
    val sparkListenerEvents = events.map(toSparkEvent).reverse
    val (firstEvent :: Nil) = sparkListenerEvents
    val fetchedStartEvent = firstEvent.asInstanceOf[SparkListenerApplicationStart]
    assert(started.time === fetchedStartEvent.time, "start time")

    // finally, a couple of checks for invalid data
    assertNone(provider.getAppUI("unknown", attempt.attemptId), "Get UI with unknown app")
    assertNone(provider.getAppUI(info.id, Some("unknown Attempt")), "Get UI with unknown attempt")
  }

}
