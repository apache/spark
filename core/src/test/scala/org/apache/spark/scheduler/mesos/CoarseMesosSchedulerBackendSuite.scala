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

import java.util
import java.util.Collections

import scala.collection.mutable

import akka.actor.ActorSystem

import com.typesafe.config.Config

import org.apache.mesos.Protos.Value.Scalar
import org.apache.mesos.Protos._
import org.apache.mesos.SchedulerDriver
import org.apache.mesos.MesosSchedulerDriver
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.mesos.{ CoarseMesosSchedulerBackend, MemoryUtils }
import org.apache.spark.{ LocalSparkContext, SparkConf, SparkEnv, SparkContext }

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{ ArgumentCaptor, Matchers }

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.scalatest.BeforeAndAfter

class CoarseMesosSchedulerBackendSuite extends FunSuite
    with LocalSparkContext
    with MockitoSugar
    with BeforeAndAfter {

  private def createOffer(offerId: String, slaveId: String, mem: Int, cpu: Int): Offer = {
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(mem))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpu))
    builder.setId(OfferID.newBuilder()
      .setValue(offerId).build())
      .setFrameworkId(FrameworkID.newBuilder()
        .setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))
      .setHostname(s"host${slaveId}")
      .build()
  }

  private def createSchedulerBackend(taskScheduler: TaskSchedulerImpl,
    driver: SchedulerDriver): CoarseMesosSchedulerBackend = {
    val backend = new CoarseMesosSchedulerBackend(taskScheduler, sc, "master") {
      override def driverURL = "driverURL"
      mesosDriver = driver
      markRegistered()
    }
    backend.start()
    backend
  }

  var sparkConf: SparkConf = _

  before {
    sparkConf = (new SparkConf)
      .setMaster("local[*]")
      .setAppName("test-mesos-dynamic-alloc")
      .setSparkHome("/path")

    sc = new SparkContext(sparkConf)
  }

  test("mesos supports killing and limiting executors") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)

    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("o1", "s1", minMem, minCpu))

    val taskID0 = TaskID.newBuilder().setValue("0").build()

    val backend = createSchedulerBackend(taskScheduler, driver)

    backend.resourceOffers(driver, mesosOffers)
    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
      any[util.Collection[TaskInfo]],
      any[Filters])

    // Calling doKillExecutors should invoke driver.killTask.
    assert(backend.doKillExecutors(Seq("s1/0")))
    verify(driver, times(1)).killTask(taskID0)
    assert(backend.executorLimit === 0)

    val mesosOffers2 = new java.util.ArrayList[Offer]
    mesosOffers2.add(createOffer("o2", "s2", minMem, minCpu))
    backend.resourceOffers(driver, mesosOffers2)

    verify(driver, times(1))
      .declineOffer(OfferID.newBuilder().setValue("o2").build())

    // Verify we didn't launch any new executor
    assert(backend.slaveIdsWithExecutors.size === 1)

    backend.doRequestTotalExecutors(2)
    backend.resourceOffers(driver, mesosOffers2)
    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(mesosOffers2.get(0).getId)),
      any[util.Collection[TaskInfo]],
      any[Filters])

    assert(backend.slaveIdsWithExecutors.size === 2)
    backend.slaveLost(driver, SlaveID.newBuilder().setValue("s1").build())
    assert(backend.slaveIdsWithExecutors.size === 1)
  }

  test("mesos supports killing and relaunching tasks with executors") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt + 1024
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    val offer1 = createOffer("o1", "s1", minMem, minCpu)
    mesosOffers.add(offer1)

    val offer2 = createOffer("o2", "s1", minMem, 1);

    val backend = createSchedulerBackend(taskScheduler, driver)

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(offer1.getId)),
      anyObject(),
      anyObject[Filters])

    // Simulate task killed, executor no longer running
    val status = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue("0").build())
      .setSlaveId(SlaveID.newBuilder().setValue("s1").build())
      .setState(TaskState.TASK_KILLED)
      .build

    backend.statusUpdate(driver, status)
    assert(!backend.slaveIdsWithExecutors.contains("s1"))

    mesosOffers.clear()
    mesosOffers.add(offer2)
    backend.resourceOffers(driver, mesosOffers)
    assert(backend.slaveIdsWithExecutors.contains("s1"))

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(offer2.getId)),
      anyObject(),
      anyObject[Filters])

    verify(driver, times(1)).reviveOffers()
  }
}
