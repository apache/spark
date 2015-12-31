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

import org.apache.spark.deploy.history.yarn.testtools.TimelineSingleEntryBatchSize
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

/**
 * Mock tests with Batch size 1.
 */
class MockBatchingTimelinePostSuite extends AbstractMockHistorySuite
    with TimelineSingleEntryBatchSize {

  test("retry upload on failure") {
    describe("mock failures, verify retry count incremented")
    // timeline client to throw an RTE on the first put
    when(timelineClient.putEntities(any(classOf[TimelineEntity])))
        .thenThrow(new RuntimeException("triggered"))
        .thenReturn(response)

    val (service, eventsPosted) = postEvents(sc)
    // now await some retries asynchronously
    awaitAtLeast(2, TEST_STARTUP_DELAY,
      () => service.postAttempts,
      () => s"Post count in $service")
    service.stop()
    awaitServiceThreadStopped(service, TEST_STARTUP_DELAY)
    // there should have been three flushed
    assert(eventsPosted === service.getFlushCount, s"expected $eventsPosted flushed for $service" )
    verify(timelineClient, times(service.postAttempts.toInt))
        .putEntities(any(classOf[TimelineEntity]))
  }
}
