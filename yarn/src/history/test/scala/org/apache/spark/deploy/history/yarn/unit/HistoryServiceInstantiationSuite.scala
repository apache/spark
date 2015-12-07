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
import org.apache.spark.deploy.history.yarn.testtools.AbstractYarnHistoryTests
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.scheduler.cluster.{SchedulerExtensionServiceBinding, SchedulerExtensionServices}

/**
 * Test the integration with [[SchedulerExtensionServices]].
 */
class HistoryServiceInstantiationSuite extends AbstractYarnHistoryTests {

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(SchedulerExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
  }

  test("Contains History Service") {
    val services = new SchedulerExtensionServices
    try {
      services.start(SchedulerExtensionServiceBinding(sc, applicationId))
      val serviceList = services.getServices
      assert(serviceList.nonEmpty, "empty service list")
      val Seq(history) = serviceList
      val historyService = history.asInstanceOf[YarnHistoryService]
      assert(historyService.serviceState === YarnHistoryService.StartedState)
      services.stop()
      assert(historyService.serviceState === YarnHistoryService.StoppedState)
    } finally {
      services.stop()
    }
  }

}

