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

package org.apache.spark.deploy.history.yarn.testtools

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{LocalSparkContext, Logging, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{YarnEventListener, YarnHistoryService}
import org.apache.spark.scheduler.cluster.SchedulerExtensionServiceBinding

/**
 * This is a base class for the YARN history test suites, both mock and integration
 *
 * Subclasses are expected to use traits and/or overriding of
 * [[ContextSetup.setupConfiguration()]]
 * to tune the configuration of the instantiated context.
 *
 * To avoid any ambiguity about the ordering/invocation of
 * any before/after code, the operations are passed to
 * overriddeable `setup()` and `teardown()`
 * invocations. Subclasses must relay the method calls to their
 * superclasses to ensure correct setup and teardown.
 */
abstract class AbstractYarnHistoryTests
    extends SparkFunSuite
    with TimelineOptionsInContext
    with TimelineServiceDisabled
    with HistoryServiceNotListeningToSparkContext
    with LocalSparkContext
    with BeforeAndAfter
    with Logging
    with ExtraAssertions
    with Matchers {

  /*
   * Setup phase creates the spark context, and anything else which tests require
   */
  before {
    setup()
  }

  /*
  * Setup creates the spark context
  */
  protected def setup(): Unit = {
    val sparkConf = new SparkConf()

    if (sc != null) {
      fail("Spark Context is not null -a previous test did not clean up properly")
    }

    sc = createSparkContext(sparkConf)
    logDebug(s"Created context $sc")
  }

  /**
   * Create the spark context
   * @param sparkConf configuration to extend
   */
  protected def createSparkContext(sparkConf: SparkConf): SparkContext = {
    sparkConf.setMaster("local").setAppName("AbstractYarnHistoryTests")
    logInfo("Creating a new spark context")
    new SparkContext(setupConfiguration(sparkConf))
  }

  /**
   * Create and start a history service.
   *
   * @param sc context
   * @param id application ID
   * @param appAttemptId optional attempt ID
   * @return the instantiated service
   */
  protected def startHistoryService(
      sc: SparkContext,
      id: ApplicationId = applicationId,
      appAttemptId: Option[ApplicationAttemptId] = Some(attemptId)): YarnHistoryService = {
    assertNotNull(sc, "Spark context")
    val service = new YarnHistoryService()
    service.start(SchedulerExtensionServiceBinding(sc, id, appAttemptId))
    assert(YarnHistoryService.StartedState === service.serviceState, s"wrong state: $service")
    service
  }

  /**
   * Create a history service, post an event sequence, then stop the service.
   * The @ of attempts posted is returned in the response to stop tests being brittle
   * against the numbe of events posted automatically. This is the code which posts the events
   * -it's the code that can reliably announce this.
   * @param sc context
   * @return (the (now closed) history service, the count of events posted for use in assertions)
   */
  def postEvents(sc: SparkContext): (YarnHistoryService, Int) = {
    val service: YarnHistoryService = startHistoryService(sc)
    val listener = new YarnEventListener(sc, service)
    var eventsPosted = 0
    try {
      listener.onApplicationStart(applicationStart)
      listener.onEnvironmentUpdate(environmentUpdate)
      listener.onApplicationEnd(applicationEnd)
      eventsPosted += 3
      // wait for them all to be processed
      awaitEventsProcessed(service, eventsPosted, TEST_STARTUP_DELAY)
      // only 2 post events, as the update does not trigger a post unless batch size == 1
      awaitAtLeast(2, TEST_STARTUP_DELAY,
        () => service.postAttempts,
        () => s"Post count in $service")
    } finally {
      // because the events have been processed and waited for,
      // a duplicate end event will not be posted
      service.stop()
    }
    (service, eventsPosted)
  }

}
