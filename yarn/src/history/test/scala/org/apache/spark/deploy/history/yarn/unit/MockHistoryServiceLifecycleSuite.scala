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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryService
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.testtools.{ContextSetup, HistoryServiceListeningToSparkContext}
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.scheduler.cluster.SchedulerExtensionServiceBinding

class MockHistoryServiceLifecycleSuite
    extends AbstractMockHistorySuite
    with ContextSetup
    with HistoryServiceListeningToSparkContext {

  /**
   * Set the batch size to 2, purely so that we can trace its path through
   * the configuration system.
   */
   override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf).set(BATCH_SIZE, "2")
  }

  /*
   * Test service lifecycle ops and that stop() is re-entrant
   */
  test("Service Lifecycle") {
    describe("service lifecycle operations")

    assertResult("2", s"batch size in context") {
      sc.conf.get(BATCH_SIZE)
    }

    assertResult("true", s"listening flag") {
      sc.conf.get(REGISTER_LISTENER)
    }

    val service = startHistoryService(sc)
    assertResult(StartedState, "not started") {
      service.serviceState
    }
    assertResult(2, s"batch size in $service") {
      service.batchSize
    }
    assertResult(true, s"listen flag in $service") {
      service.listening
    }

    service.stop()
    assertResult(StoppedState, s"not stopped: $service") {
      service.serviceState
    }

    // and expect an attempt to start again to fail
    intercept[IllegalArgumentException] {
      service.start(SchedulerExtensionServiceBinding(sc, applicationId, None))
    }
    // repeated stop() is harmless
    service.stop()
  }

  test("ServiceStartArguments1") {
    val service = new YarnHistoryService()
    intercept[IllegalArgumentException] {
      service.start(SchedulerExtensionServiceBinding(null, applicationId, None))
    }
  }

  test("ServiceStartArguments2") {
    val service = new YarnHistoryService()
    intercept[IllegalArgumentException] {
      service.start(SchedulerExtensionServiceBinding(sc, null, None))
    }
  }

}
