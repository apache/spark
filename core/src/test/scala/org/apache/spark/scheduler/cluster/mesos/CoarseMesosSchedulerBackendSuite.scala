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
import org.apache.spark.network.shuffle.mesos.MesosExternalShuffleClient

import scala.collection.JavaConverters._

import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}
import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.Scalar
import org.mockito.{ArgumentCaptor, Matchers}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SecurityManager, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.TaskSchedulerImpl

import scala.collection.immutable.HashMap


class CoarseMesosSchedulerBackendSuite extends SparkFunSuite
    with LocalSparkContext
    with MockitoSugar
    with BeforeAndAfter {

  private def createOfferId(offerId: String): OfferID = {
    OfferID.newBuilder().setValue(offerId).build()
  }

  private def createSlaveId(slaveId: String): SlaveID = {
    SlaveID.newBuilder().setValue(slaveId).build()
  }

  private def createExecutorId(executorId: String): ExecutorID = {
    ExecutorID.newBuilder().setValue(executorId).build()
  }

  private def createTaskId(taskId: String): TaskID = {
    TaskID.newBuilder().setValue(taskId).build()
  }

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
    builder.setId(createOfferId(offerId))
      .setFrameworkId(FrameworkID.newBuilder()
        .setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))
      .setHostname(s"host${slaveId}")
      .build()
  }

  private def createSchedulerBackend(
      taskScheduler: TaskSchedulerImpl,
      driver: SchedulerDriver,
      shuffleClient: MesosExternalShuffleClient): CoarseMesosSchedulerBackend = {
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
      override protected def getShuffleClient(): MesosExternalShuffleClient = {
        shuffleClient
      }
      markRegistered()
    }
    backend.start()
    backend
  }

  var sparkConf: SparkConf = _
  var driver: SchedulerDriver = _
  var taskScheduler: TaskSchedulerImpl = _
  var backend: CoarseMesosSchedulerBackend = _
  var externalShuffleClient: MesosExternalShuffleClient = _

  private def setBackend(sparkConfVars: Map[String, String] = null) {
    sparkConf = (new SparkConf)
      .setMaster("local[*]")
      .setAppName("test-mesos-dynamic-alloc")
      .setSparkHome("/path")

    if (sparkConfVars != null) {
      for (attr <- sparkConfVars) {
        sparkConf.set(attr._1, attr._2)
      }
    }

    sc = new SparkContext(sparkConf)

    driver = mock[SchedulerDriver]
    when(driver.start()).thenReturn(Protos.Status.DRIVER_RUNNING)
    taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)
    externalShuffleClient = mock[MesosExternalShuffleClient]

    backend = createSchedulerBackend(taskScheduler, driver, externalShuffleClient)
  }

  test("mesos supports killing and limiting executors") {
    setBackend()
    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val minMem = backend.calculateTotalMemory(sc)
    val minCpu = 4
    val offers = List((minMem, minCpu))

    // launches a task on a valid offer
    offerResources(offers)
    verifyTaskLaunched("o1")

    // kills executors
    backend.doRequestTotalExecutors(0)
    assert(backend.doKillExecutors(Seq("0")))
    val taskID0 = TaskID.newBuilder().setValue("0").build()
    verify(driver, times(1)).killTask(taskID0)

    // doesn't launch a new task when requested executors == 0
    offerResources(offers, 2)
    verifyDeclinedOffer(driver, createOfferId("o2"))
    assert(numSlavesWithTasks() == 1)

    // Launches a new task when requested executors is positive
    backend.doRequestTotalExecutors(2)
    offerResources(offers, 2)
    verifyTaskLaunched("o2")

    // Accounts for a lost slave
    assert(numSlavesWithTasks() == 2)
    backend.slaveLost(driver, SlaveID.newBuilder().setValue("s1").build())
    assert(numSlavesWithTasks() == 1)
  }

  // Verifies
  //   1. Launches a task on a valid offer
  //   2. Accounts for a killed task
  //   3. Launches a new task on a valid offer from the same slave
  //   4. Calls SchedulerDriver::reviveOffers when a task is lost
  test("mesos supports killing and relaunching tasks with executors") {
    setBackend()
    val minMem = backend.calculateTotalMemory(sc) + 1024
    val minCpu = 4

    val offer1 = (minMem, minCpu)
    val offer2 = (minMem, 1)
    offerResources(List(offer1, offer2))

    verifyTaskLaunched("o1")

    // Simulate task killed, executor no longer running

    val status = createTaskStatus("0", "s1", TaskState.TASK_KILLED)
    backend.statusUpdate(driver, status)
    assert(!backend.slaves.filter(_._2.taskIDs.nonEmpty).contains("s1"))

    offerResources(List(offer2))

    assert(backend.slaves.filter(_._2.taskIDs.nonEmpty).contains("s1"))
    verifyTaskLaunched("o2")

    verify(driver, times(1)).reviveOffers()
  }

  test("mesos supports spark.executor.cores") {
    val executorCores = 4
    setBackend(Map("spark.executor.cores" -> executorCores.toString))

    val executorMemory = backend.calculateTotalMemory(sc)
    val offers = List((executorMemory * 2, executorCores + 1))
    offerResources(offers)

    val taskInfos = verifyTaskLaunched("o1")
    assert(taskInfos.size() == 1)

    val cpus = backend.getResource(taskInfos.iterator().next().getResourcesList, "cpus")
    assert(cpus == executorCores)
  }

  test("mesos supports unset spark.executor.cores") {
    setBackend()

    val executorMemory = backend.calculateTotalMemory(sc)
    val offerCores = 10
    offerResources(List((executorMemory * 2, offerCores)))

    val taskInfos = verifyTaskLaunched("o1")
    assert(taskInfos.size() == 1)

    val cpus = backend.getResource(taskInfos.iterator().next().getResourcesList, "cpus")
    assert(cpus == offerCores)
  }

  test("mesos does not acquire more than spark.cores.max") {
    val maxCores = 10
    setBackend(Map("spark.cores.max" -> maxCores.toString))

    val executorMemory = backend.calculateTotalMemory(sc)
    offerResources(List((executorMemory, maxCores + 1)))

    val taskInfos = verifyTaskLaunched("o1")
    assert(taskInfos.size() == 1)

    val cpus = backend.getResource(taskInfos.iterator().next().getResourcesList, "cpus")
    assert(cpus == maxCores)
  }

  test("mesos declines offers that violate attribute constraints") {
    setBackend(Map("spark.mesos.constraints" -> "x:true"))
    offerResources(List((backend.calculateTotalMemory(sc), 4)))
    verifyDeclinedOffer(driver, createOfferId("o1"), true)
  }

  test("mesos assigns tasks round-robin on offers") {
    val executorCores = 4
    val maxCores = executorCores * 2
    setBackend(Map("spark.executor.cores" -> executorCores.toString,
      "spark.cores.max" -> maxCores.toString))

    val executorMemory = backend.calculateTotalMemory(sc)
    offerResources(List(
      (executorMemory * 2, executorCores * 2),
      (executorMemory * 2, executorCores * 2)))

    verifyTaskLaunched("o1")
    verifyTaskLaunched("o2")

    assert(numSlavesWithTasks() == 2)
  }

  test("mesos creates multiple executors on a single slave") {
    val executorCores = 4
    setBackend(Map("spark.executor.cores" -> executorCores.toString))

    // offer with room for two executors
    val executorMemory = backend.calculateTotalMemory(sc)
    offerResources(List((executorMemory * 2, executorCores * 2)))

    // verify two executors were started on a single offer
    val taskInfos = verifyTaskLaunched("o1")
    assert(taskInfos.size() == 2)
  }

  test("mesos doesn't register twice with the same shuffle service") {
    setBackend(Map("spark.shuffle.service.enabled" -> "true"))
    val (mem, cpu) = (backend.calculateTotalMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)
    verifyTaskLaunched("o1")

    val offer2 = createOffer("o2", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer2).asJava)
    verifyTaskLaunched("o2")

    assert(numSlavesWithTasks() == 1)

    val status1 = createTaskStatus("0", "s1", TaskState.TASK_RUNNING)
    backend.statusUpdate(driver, status1)

    val status2 = createTaskStatus("1", "s1", TaskState.TASK_RUNNING)
    backend.statusUpdate(driver, status2)
    verify(externalShuffleClient, times(1)).registerDriverWithShuffleService(anyString, anyInt)
  }

  test("mesos accounts for a terminated executor") {
    setBackend()

    val (mem, cpu) = (backend.calculateTotalMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)
    verifyTaskLaunched("o1")
    assert(numSlavesWithTasks() == 1)

    backend.executorLost(driver, createExecutorId("0"), createSlaveId("s1"), 0)
    assert(numSlavesWithTasks() == 0)
  }

  test("mesos accounts for a lost slave") {
    setBackend()

    val (mem, cpu) = (backend.calculateTotalMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)
    verifyTaskLaunched("o1")
    assert(numSlavesWithTasks() == 1)

    backend.slaveLost(driver, createSlaveId("s1"))
    assert(numSlavesWithTasks() == 0)
  }

  test("mesos kills an executor when told") {
    setBackend()

    val (mem, cpu) = (backend.calculateTotalMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)
    verifyTaskLaunched("o1")

    backend.doKillExecutors(List("0"))
    verify(driver, times(1)).killTask(createTaskId("0"))
  }

  private def verifyDeclinedOffer(driver: SchedulerDriver,
                                  offerId: OfferID,
                                  filter: Boolean = false): Unit = {
    if (filter) {
      verify(driver, times(1)).declineOffer(Matchers.eq(offerId), anyObject[Filters])
    } else {
      verify(driver, times(1)).declineOffer(Matchers.eq(offerId))
    }
  }

  private def offerResources(offers: List[Tuple2[Int, Int]], startId: Int = 1): Unit = {
    // TODO: how do I unpack this?
    val mesosOffers = offers.zipWithIndex.map(x =>
      createOffer(s"o${x._2 + startId}", s"s${x._2 + startId}", x._1._1, x._1._2))

    backend.resourceOffers(driver, mesosOffers.asJava)
  }

  private def numSlavesWithTasks(): Int = {
    backend.slaves.values.count(_.taskIDs.nonEmpty)
  }

  private def verifyTaskLaunched(offerId: String): java.util.Collection[TaskInfo] = {
    val captor = ArgumentCaptor.forClass(classOf[java.util.Collection[TaskInfo]])
    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(createOfferId(offerId))),
      captor.capture(),
      any[Filters])
    captor.getValue
  }

  private def createTaskStatus(taskId: String, slaveId: String, state: TaskState): TaskStatus = {
    TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(taskId).build())
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId).build())
      .setState(state)
      .build
  }
}
