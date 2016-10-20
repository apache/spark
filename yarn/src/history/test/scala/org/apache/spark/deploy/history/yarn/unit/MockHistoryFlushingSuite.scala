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

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.YarnEventListener
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.scheduler.SparkListenerUnpersistRDD

/**
 * Tests to verify that timeline operations happen even before services are closed.
 *
 * There's an async queue involved here, so the tests spin until a state is met or not.
 */
class MockHistoryFlushingSuite extends AbstractMockHistorySuite
    with BeforeAndAfter with Matchers with Logging {

  test("PostEventsNoServiceStop") {
    describe("verify that events are pushed on any triggered flush," +
        " even before a service is stopped")
    val service = startHistoryService(sc)
    try {
      assert(service.timelineServiceEnabled, s"no timeline service in $service")
      service.timelineClient
      service.createTimelineClient()
      val listener = new YarnEventListener(sc, service)
      listener.onApplicationStart(applicationStart)
      service.asyncFlush()
      awaitPostAttemptCount(service, 1)
      verify(timelineClient, times(1)).putEntities(any(classOf[TimelineEntity]))
    } finally {
      service.stop()
    }
  }

  test("PostEventsWithServiceStop") {
    describe("verify that events are pushed on service stop")
    val service = startHistoryService(sc)
    try {
      service.timelineClient
      service.createTimelineClient()
      val listener = new YarnEventListener(sc, service)
      listener.onApplicationStart(applicationStart)
      awaitPostAttemptCount(service, 1)
      verify(timelineClient, times(1)).putEntities(any(classOf[TimelineEntity]))
      listener.onUnpersistRDD(SparkListenerUnpersistRDD(1))
      // expecting two events
      awaitPostAttemptCount(service, 1)

      // now stop the service and await the final post
      service.stop()
      awaitServiceThreadStopped(service, TEST_STARTUP_DELAY)
      verify(timelineClient, times(2)).putEntities(any(classOf[TimelineEntity]))
    } finally {
      logDebug(s"teardown of $service")
      service.stop()
    }
  }

}
