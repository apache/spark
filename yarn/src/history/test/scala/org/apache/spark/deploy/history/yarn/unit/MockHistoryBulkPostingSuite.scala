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

import java.io.IOException

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.mockito.Matchers._
import org.mockito.Mockito._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnEventListener
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.scheduler.SparkListenerJobStart

/**
 * Mock event posting
 */
class MockHistoryBulkPostingSuite extends AbstractMockHistorySuite {

  val batchSize = 5
  val queueLimit = 5

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    val conf = super.setupConfiguration(sparkConf)
    conf.set(BATCH_SIZE, batchSize.toString())
    conf.set(POST_EVENT_LIMIT, queueLimit.toString)
    conf.set(POST_RETRY_INTERVAL, "0ms")
  }

  test("Massive Event Posting") {
    describe("Post many events to a failing")
    // timeline client to throw an exception on every POST

     when(timelineClient.putEntities(any(classOf[TimelineEntity])))
      .thenThrow(new IOException("triggered"))

    val service = startHistoryService(sc)
    try {
      val listener = new YarnEventListener(sc, service)
      // start
      listener.onApplicationStart(applicationStart)
      // post many more events
      1 to (batchSize * 2 * queueLimit ) foreach { t =>
        listener.onJobStart(new SparkListenerJobStart(1, t, Nil))
      }
      // events dropped
      awaitAtLeast(batchSize, TEST_STARTUP_DELAY,
        () => service.eventsDropped,
        () => service.toString())

      // posts failed
      awaitAtLeast(10, SERVICE_SHUTDOWN_DELAY,
        () => service.postFailures,
        () => service.toString())

      // now trigger a service shutdown with the blocking queue
      describe("Service stop")
      service.stop()
      awaitServiceThreadStopped(service, SERVICE_SHUTDOWN_DELAY)
      logDebug(s"$service")
    } finally {
      logDebug("stopping service in finally() clause")
      service.stop()
    }
  }

}
