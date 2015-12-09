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

package org.apache.spark.scheduler.cluster.mesos

import java.util
import java.util.Collections

import org.apache.mesos.Protos.Value.Scalar
import org.apache.mesos.Protos._
import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.{LocalSparkContext, SecurityManager, SparkConf, SparkContext, SparkFunSuite}
import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar

class CoarseMesosSchedulerBackendSuite extends SparkFunSuite
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

  private def createSchedulerBackend(
      taskScheduler: TaskSchedulerImpl,
      driver: SchedulerDriver): CoarseMesosSchedulerBackend = {
    val securityManager = mock[SecurityManager]
    val backend = new CoarseMesosSchedulerBackend(taskScheduler, sc, "master", securityManager) {
      override protected def createSchedulerDriver(
        masterUrl: String,
        scheduler: Scheduler,
        sparkUser: String,
        appName: String,
        conf: SparkConf,
        webuiUrl: Option[String] = None,
        checkpoint: Option[Boolean] = None,
        failoverTimeout: Option[Double] = None,
        frameworkId: Option[String] = None): SchedulerDriver = driver
      markRegistered()
    }
    backend.start()
    backend
  }

  private def buildLimitedBackend() = {
    sc.conf.set("spark.cores.mb.min", "10000")
    sc.conf.set("spark.cores.mb.max", "100000")
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)
    val backend = createSchedulerBackend(taskScheduler, driver)
    assert(backend.maxMBPerCore == 100000.0)
    assert(backend.minMBPerCore == 10000.0)
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

  test("coarse mesos backend correctly keep sufficient offer") {
    assert(
      buildLimitedBackend().calculateDesiredResources(sc, 1, 80000)
        .count(x => x._1 == 1 && x._2 == 80000) == 1
    )
  }
  test("coarse mesos backend correctly ignores insufficient offer") {
    val backend = buildLimitedBackend()
    assert(backend.calculateDesiredResources(sc, 1, 800).isEmpty)
  }

  test("coarse mesos backend correctly truncates CPU when too high") {
    assert(
      buildLimitedBackend().calculateDesiredResources(sc, 10, 80000)
        .count(x => x._1 == 8 &&x._2 == 80000) == 1
    )
  }

  test("coarse mesos backend correctly truncates MEM when too high") {
    assert(
      buildLimitedBackend().calculateDesiredResources(sc, 1, 800000)
        .count(x => x._1 == 1 && x._2 == 100000) == 1
    )
  }

  test("coarse mesos backend correctly handles zero cpu") {
    assert(buildLimitedBackend().calculateDesiredResources(sc, 0, 800000).isEmpty)
  }

  test("coarse mesos backend correctly handles zero mem") {
    assert(buildLimitedBackend().calculateDesiredResources(sc, 1, 0).isEmpty)
  }

  test("coarse mesos backend correctly handles zero everything") {
    assert(buildLimitedBackend().calculateDesiredResources(sc, 0, 0).isEmpty)
  }

  test("coarse mesos backend correctly handles default mem limit") {
    sc.conf.set("spark.cores.mb.min", "1")
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)
    val backend = createSchedulerBackend(taskScheduler, driver)
    assert(backend.maxMBPerCore == Double.MaxValue)
    assert(backend.minMBPerCore == 1.0)
    val minimumMem = backend.calculateTotalMemory(sc)
    val minimumOffer = backend.calculateDesiredResources(sc, 1, minimumMem)
    assert(minimumOffer.isDefined)
    assert(minimumOffer.get._1 == 1)
    assert(minimumOffer.get._2 == minimumMem)
    assert(
      backend.calculateDesiredResources(sc, 10, minimumMem)
        .count(x => x._1 == 10 && x._2 == minimumMem) == 1
    )
    assert(
      backend.calculateDesiredResources(sc, minimumMem, minimumMem)
        .count(x => x._1 == minimumMem && x._2 == minimumMem) == 1
    )
    assert(
      backend.calculateDesiredResources(sc, minimumMem + 1, minimumMem)
        .count(x => x._1 == minimumMem && x._2 == minimumMem) == 1
    )
    assert(backend.calculateDesiredResources(sc, 1, minimumMem - 1).isEmpty)
  }

  test("coarse mesos backend correctly handles unset mem limit") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)
    val backend = createSchedulerBackend(taskScheduler, driver)
    assert(backend.maxMBPerCore == Double.MaxValue)
    assert(backend.minMBPerCore == 0.0)
    assert(backend.calculateDesiredResources(sc, 1, 1).isEmpty)
    assert(
      backend.calculateDesiredResources(sc, 1, 10000)
        .count(x => x._1 == 1 && x._2 == 10000) == 1
    )
    assert(
      backend.calculateDesiredResources(sc, 1, backend.calculateTotalMemory(sc))
        .count(x => x._1 == 1 && x._2 == backend.calculateTotalMemory(sc)) == 1
    )
    assert(
      backend.calculateDesiredResources(sc, 1, Integer.MAX_VALUE)
        .count(x => x._1 == 1 && x._2 == Integer.MAX_VALUE) == 1
    )
    assert(
      backend.calculateDesiredResources(sc, Integer.MAX_VALUE, 10000)
        .count(x => x._1 == Integer.MAX_VALUE && x._2 == 10000) == 1
    )
  }


  test("mesos supports killing and limiting executors") {
    val driver = mock[SchedulerDriver]
    when(driver.start()).thenReturn(Protos.Status.DRIVER_RUNNING)
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)

    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val backend = createSchedulerBackend(taskScheduler, driver)
    val minMem = backend.calculateTotalMemory(sc)
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("o1", "s1", minMem, minCpu))

    val taskID0 = TaskID.newBuilder().setValue("0").build()

    backend.resourceOffers(driver, mesosOffers)
    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
      any[util.Collection[TaskInfo]],
      any[Filters])

    // simulate the allocation manager down-scaling executors
    backend.doRequestTotalExecutors(0)
    assert(backend.doKillExecutors(Seq("s1/0")))
    verify(driver, times(1)).killTask(taskID0)

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
    when(driver.start()).thenReturn(Protos.Status.DRIVER_RUNNING)
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)

    val backend = createSchedulerBackend(taskScheduler, driver)
    val minMem = backend.calculateTotalMemory(sc) + 1024
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    val offer1 = createOffer("o1", "s1", minMem, minCpu)
    mesosOffers.add(offer1)

    val offer2 = createOffer("o2", "s1", minMem, 1);

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
