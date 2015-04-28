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

import java.util.Date

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import org.apache.spark.deploy.Command
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.scheduler.cluster.mesos._
import org.apache.spark.{LocalSparkContext, SparkConf}


class MesosClusterSchedulerSuite extends FunSuite with LocalSparkContext with MockitoSugar {

  private val command = new Command("mainClass", Seq("arg"), null, null, null, null)

  test("can queue drivers") {
    val conf = new SparkConf()
    conf.setMaster("mesos://localhost:5050")
    conf.setAppName("spark mesos")
    val scheduler = new MesosClusterScheduler(
      new BlackHoleMesosClusterPersistenceEngineFactory, conf) {
      override def start(): Unit = { ready = true }
    }
    scheduler.start()
    val response = scheduler.submitDriver(
        new MesosDriverDescription("d1", "jar", 1000, 1, true,
          command, Map[String, String](), "s1", new Date()))
    assert(response.success)
    val response2 =
      scheduler.submitDriver(new MesosDriverDescription(
        "d1", "jar", 1000, 1, true, command, Map[String, String](), "s2", new Date()))
    assert(response2.success)
    val state = scheduler.getSchedulerState()
    val queuedDrivers = state.queuedDrivers.toList
    assert(queuedDrivers(0).submissionId == response.submissionId)
    assert(queuedDrivers(1).submissionId == response2.submissionId)
  }

  test("can kill queued drivers") {
    val conf = new SparkConf()
    conf.setMaster("mesos://localhost:5050")
    conf.setAppName("spark mesos")
    val scheduler = new MesosClusterScheduler(
      new BlackHoleMesosClusterPersistenceEngineFactory, conf) {
      override def start(): Unit = { ready = true }
    }
    scheduler.start()
    val response = scheduler.submitDriver(
        new MesosDriverDescription("d1", "jar", 1000, 1, true,
          command, Map[String, String](), "s1", new Date()))
    assert(response.success)
    val killResponse = scheduler.killDriver(response.submissionId)
    assert(killResponse.success)
    val state = scheduler.getSchedulerState()
    assert(state.queuedDrivers.isEmpty)
  }
}
