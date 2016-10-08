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

package org.apache.spark.scheduler.cluster

import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging

/**
 * Test the integration with [[SchedulerExtensionServices]]
 */
class ExtensionServiceIntegrationSuite extends SparkFunSuite
  with LocalSparkContext with BeforeAndAfter
  with Logging {

  val applicationId = new StubApplicationId(0, 1111L)
  val attemptId = new StubApplicationAttemptId(applicationId, 1)

  /*
   * Setup phase creates the spark context
   */
  before {
    val sparkConf = new SparkConf()
    sparkConf.set(SCHEDULER_SERVICES, Seq(classOf[SimpleExtensionService].getName()))
    sparkConf.setMaster("local").setAppName("ExtensionServiceIntegrationSuite")
    sc = new SparkContext(sparkConf)
  }

  test("Instantiate") {
    val services = new SchedulerExtensionServices()
    assertResult(Nil, "non-nil service list") {
      services.getServices
    }
    services.start(SchedulerExtensionServiceBinding(sc, applicationId))
    services.stop()
  }

  test("Contains SimpleExtensionService Service") {
    val services = new SchedulerExtensionServices()
    try {
      services.start(SchedulerExtensionServiceBinding(sc, applicationId))
      val serviceList = services.getServices
      assert(serviceList.nonEmpty, "empty service list")
      val (service :: Nil) = serviceList
      val simpleService = service.asInstanceOf[SimpleExtensionService]
      assert(simpleService.started.get, "service not started")
      services.stop()
      assert(!simpleService.started.get, "service not stopped")
    } finally {
      services.stop()
    }
  }
}


