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

package org.apache.spark.scheduler.mesos

import org.scalatest.FunSuite
import org.apache.spark.{SparkConf, LocalSparkContext}
import org.scalatest.mock.MockitoSugar
import org.apache.spark.scheduler.cluster.mesos.MesosClusterScheduler
import org.apache.spark.deploy.{Command, DriverDescription}

class MesosClusterSchedulerSuite extends FunSuite with LocalSparkContext with MockitoSugar {
  def createCommand: Command = {
    new Command(
      "mainClass", Seq("arg"), null, null, null, null)
  }

  test("can queue drivers") {
    val conf = new SparkConf()
    conf.setMaster("mesos://localhost:5050")
    conf.setAppName("spark mesos")
    val scheduler = new MesosClusterScheduler(conf)
    val response =
      scheduler.submitDriver(new DriverDescription("jar", 1000, 1, true, createCommand))
    assert(response.success)

    val response2 =
      scheduler.submitDriver(new DriverDescription("jar", 1000, 1, true, createCommand))
    assert(response2.success)

    val state = scheduler.getState()
    assert(state.queuedDrivers.exists(d => d.submissionId == response.id))
    assert(state.queuedDrivers.exists(d => d.submissionId == response2.id))
  }

  test("can kill queued drivers") {
    val conf = new SparkConf()
    conf.setMaster("mesos://localhost:5050")
    conf.setAppName("spark mesos")
    val scheduler = new MesosClusterScheduler(conf)
    val response =
      scheduler.submitDriver(new DriverDescription("jar", 1000, 1, true, createCommand))
    assert(response.success)

    val killResponse = scheduler.killDriver(response.id)
    assert(killResponse.success)

    val state = scheduler.getState()
    assert(state.queuedDrivers.isEmpty)
  }
}
