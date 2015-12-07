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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelinePutResponse}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark.deploy.history.yarn.YarnEventListener
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

/**
 * Mock event posting.
 */
class MockHistoryEventPostingSuite extends AbstractMockHistorySuite {

  /*
   * Make sure the low-level stuff the other tests depend on is there
   */
  test("Timeline client") {
    describe("low-level timeline client test")
    assert(response === timelineClient.putEntities(new TimelineEntity))
    verify(timelineClient).putEntities(any(classOf[TimelineEntity]))
  }

  test("Event Queueing") {
    describe("event queueing")
    val (history, eventsPosted) = postEvents(sc)
    awaitEventsProcessed(history, eventsPosted, TEST_STARTUP_DELAY)
  }

  test("batch processing of Spark listener events") {
    val (historyService, _) = postEvents(sc)
    verify(timelineClient, times(historyService.postAttempts.toInt))
        .putEntities(any(classOf[TimelineEntity]))
  }

  test("PostEventsNoServiceStop") {
    describe("verify that events are pushed on any triggered flush," +
        " even before a service is stopped")
    val service = startHistoryService(sc)
    try {
      val listener = new YarnEventListener(sc, service)
      listener.onApplicationStart(applicationStart)
      service.asyncFlush()
      awaitEventsProcessed(service, 1, TEST_STARTUP_DELAY)
      awaitFlushCount(service, 1, TEST_STARTUP_DELAY)
      awaitPostAttemptCount(service, 1)
      logDebug(s"$service")
      verify(timelineClient, times(1)).putEntities(any(classOf[TimelineEntity]))
    } finally {
      logDebug("stopping service in finally() clause")
      service.stop()
    }
  }

  /**
   * This is a convoluted little test designed to verify something: stopping
   * the service while it is waiting for something to happen in the thread posting
   * service will cause the thread to complete successfully.
   *
   * That is: even though the YARN code may potentially swallow interrupted exceptions,
   * once it returns, the exit flag is picked up and the post thread switches
   * into fast shutdown mode, attempting to post any remaining threads.
   */
  test("PostEventsBlockingOperation") {
    describe("verify that events are pushed on any triggered flush," +
        " even before a service is stopped")
    val entered = new AtomicBoolean(false)
    val exit = new AtomicBoolean(false)

    /**
     * Set a flag ang then notify the listeners
     * @param b atomic bool flag to set
     */
    def setAndNotify(b: AtomicBoolean): Unit = {
      b.synchronized {
        b.set(true)
        b.notify()
      }
    }

    // wait for a boolean to be set; interrupts are discarded.
    def waitForSet(b: AtomicBoolean, timeout: Long): Unit = {
      b.synchronized {
        while (!b.get()) {
          try {
            b.wait(timeout)
            if (!b.get()) {
              fail("post operation never started")
            }
          } catch {
            case irq: InterruptedException =>
            case ex: RuntimeException => throw  ex
          }
        }
      }
    }

    // Mockito answer which doesn't return until the `exit` flag is set,
    // and which sets the `entered` flag on entry.
    // this is designed to synchronize the posting thread with the test runner thread
    class delayedAnswer extends Answer[TimelinePutResponse]() {


      override def answer(invocation: InvocationOnMock): TimelinePutResponse = {
        // flag the operation has started
        if (!exit.get()) {
          logDebug("setting `entered` flag")
          setAndNotify(entered)
          logDebug("waiting for `exit` flag")
          exit.synchronized {
            exit.wait(TEST_STARTUP_DELAY)
          }
        }
        new TimelinePutResponse
      }
    }
    when(timelineClient.putEntities(any(classOf[TimelineEntity])))
        .thenAnswer(new delayedAnswer())

    val service = startHistoryService(sc)
    try {
      val listener = new YarnEventListener(sc, service)
      listener.onApplicationStart(applicationStart)
      awaitPostAttemptCount(service, 1)
      setAndNotify(entered)
      entered.synchronized {
        if (!entered.get()) {
          logDebug("waiting for `entered` flag")
          entered.wait(TEST_STARTUP_DELAY)
          if (!entered.get()) {
            fail("post operation never started")
          }
        }
      }
      // trigger the stop process. The interrupt will be probably be lost
      logDebug("stopping service")
      service.stop()
      logDebug("setting `exit` flag")
      setAndNotify(exit)
      awaitServiceThreadStopped(service, SERVICE_SHUTDOWN_DELAY)
      logDebug(s"$service")
    } finally {
      setAndNotify(exit)
      logDebug("stopping service in finally() clause")
      service.stop()
    }
  }

}
