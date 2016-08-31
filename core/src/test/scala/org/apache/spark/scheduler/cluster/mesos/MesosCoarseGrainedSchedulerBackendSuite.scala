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

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise
import scala.reflect.ClassTag

import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}
import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.Scalar
import org.mockito.{ArgumentCaptor, Matchers}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SecurityManager, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.network.shuffle.mesos.MesosExternalShuffleClient
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RemoveExecutor
import org.apache.spark.scheduler.TaskSchedulerImpl

class MesosCoarseGrainedSchedulerBackendSuite extends SparkFunSuite
    with LocalSparkContext
    with MockitoSugar
    with BeforeAndAfter {

  private var sparkConf: SparkConf = _
  private var driver: SchedulerDriver = _
  private var taskScheduler: TaskSchedulerImpl = _
  private var backend: MesosCoarseGrainedSchedulerBackend = _
  private var externalShuffleClient: MesosExternalShuffleClient = _
  private var driverEndpoint: RpcEndpointRef = _
  @volatile private var stopCalled = false

  test("mesos supports killing and limiting executors") {
    setBackend()
    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val minMem = backend.executorMemory(sc)
    val minCpu = 4
    val offers = List((minMem, minCpu))

    // launches a task on a valid offer
    offerResources(offers)
    verifyTaskLaunched("o1")

    // kills executors
    backend.doRequestTotalExecutors(0)
    assert(backend.doKillExecutors(Seq("0")))
    val taskID0 = createTaskId("0")
    verify(driver, times(1)).killTask(taskID0)

    // doesn't launch a new task when requested executors == 0
    offerResources(offers, 2)
    verifyDeclinedOffer(driver, createOfferId("o2"))

    // Launches a new task when requested executors is positive
    backend.doRequestTotalExecutors(2)
    offerResources(offers, 2)
    verifyTaskLaunched("o2")
  }

  test("mesos supports killing and relaunching tasks with executors") {
    setBackend()

    // launches a task on a valid offer
    val minMem = backend.executorMemory(sc) + 1024
    val minCpu = 4
    val offer1 = (minMem, minCpu)
    val offer2 = (minMem, 1)
    offerResources(List(offer1, offer2))
    verifyTaskLaunched("o1")

    // accounts for a killed task
    val status = createTaskStatus("0", "s1", TaskState.TASK_KILLED)
    backend.statusUpdate(driver, status)
    verify(driver, times(1)).reviveOffers()

    // Launches a new task on a valid offer from the same slave
    offerResources(List(offer2))
    verifyTaskLaunched("o2")
  }

  test("mesos supports spark.executor.cores") {
    val executorCores = 4
    setBackend(Map("spark.executor.cores" -> executorCores.toString))

    val executorMemory = backend.executorMemory(sc)
    val offers = List((executorMemory * 2, executorCores + 1))
    offerResources(offers)

    val taskInfos = verifyTaskLaunched("o1")
    assert(taskInfos.size() == 1)

    val cpus = backend.getResource(taskInfos.iterator().next().getResourcesList, "cpus")
    assert(cpus == executorCores)
  }

  test("mesos supports unset spark.executor.cores") {
    setBackend()

    val executorMemory = backend.executorMemory(sc)
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

    val executorMemory = backend.executorMemory(sc)
    offerResources(List((executorMemory, maxCores + 1)))

    val taskInfos = verifyTaskLaunched("o1")
    assert(taskInfos.size() == 1)

    val cpus = backend.getResource(taskInfos.iterator().next().getResourcesList, "cpus")
    assert(cpus == maxCores)
  }

  test("mesos declines offers that violate attribute constraints") {
    setBackend(Map("spark.mesos.constraints" -> "x:true"))
    offerResources(List((backend.executorMemory(sc), 4)))
    verifyDeclinedOffer(driver, createOfferId("o1"), true)
  }

  test("mesos declines offers with a filter when reached spark.cores.max") {
    val maxCores = 3
    setBackend(Map("spark.cores.max" -> maxCores.toString))

    val executorMemory = backend.executorMemory(sc)
    offerResources(List(
      (executorMemory, maxCores + 1),
      (executorMemory, maxCores + 1)))

    verifyTaskLaunched("o1")
    verifyDeclinedOffer(driver, createOfferId("o2"), true)
  }

  test("mesos assigns tasks round-robin on offers") {
    val executorCores = 4
    val maxCores = executorCores * 2
    setBackend(Map("spark.executor.cores" -> executorCores.toString,
      "spark.cores.max" -> maxCores.toString))

    val executorMemory = backend.executorMemory(sc)
    offerResources(List(
      (executorMemory * 2, executorCores * 2),
      (executorMemory * 2, executorCores * 2)))

    verifyTaskLaunched("o1")
    verifyTaskLaunched("o2")
  }

  test("mesos creates multiple executors on a single slave") {
    val executorCores = 4
    setBackend(Map("spark.executor.cores" -> executorCores.toString))

    // offer with room for two executors
    val executorMemory = backend.executorMemory(sc)
    offerResources(List((executorMemory * 2, executorCores * 2)))

    // verify two executors were started on a single offer
    val taskInfos = verifyTaskLaunched("o1")
    assert(taskInfos.size() == 2)
  }

  test("mesos doesn't register twice with the same shuffle service") {
    setBackend(Map("spark.shuffle.service.enabled" -> "true"))
    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)
    verifyTaskLaunched("o1")

    val offer2 = createOffer("o2", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer2).asJava)
    verifyTaskLaunched("o2")

    val status1 = createTaskStatus("0", "s1", TaskState.TASK_RUNNING)
    backend.statusUpdate(driver, status1)

    val status2 = createTaskStatus("1", "s1", TaskState.TASK_RUNNING)
    backend.statusUpdate(driver, status2)
    verify(externalShuffleClient, times(1))
      .registerDriverWithShuffleService(anyString, anyInt, anyLong, anyLong)
  }

  test("mesos kills an executor when told") {
    setBackend()

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)
    verifyTaskLaunched("o1")

    backend.doKillExecutors(List("0"))
    verify(driver, times(1)).killTask(createTaskId("0"))
  }

  test("weburi is set in created scheduler driver") {
    setBackend()
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)
    val driver = mock[SchedulerDriver]
    when(driver.start()).thenReturn(Protos.Status.DRIVER_RUNNING)
    val securityManager = mock[SecurityManager]

    val backend = new MesosCoarseGrainedSchedulerBackend(
        taskScheduler, sc, "master", securityManager) {
      override protected def createSchedulerDriver(
        masterUrl: String,
        scheduler: Scheduler,
        sparkUser: String,
        appName: String,
        conf: SparkConf,
        webuiUrl: Option[String] = None,
        checkpoint: Option[Boolean] = None,
        failoverTimeout: Option[Double] = None,
        frameworkId: Option[String] = None): SchedulerDriver = {
        markRegistered()
        assert(webuiUrl.isDefined)
        assert(webuiUrl.get.equals("http://webui"))
        driver
      }
    }

    backend.start()
  }

  test("Do not call removeExecutor() after backend is stopped") {
    setBackend()

    // launches a task on a valid offer
    val offers = List((backend.executorMemory(sc), 1))
    offerResources(offers)
    verifyTaskLaunched("o1")

    // launches a thread simulating status update
    val statusUpdateThread = new Thread {
      override def run(): Unit = {
        while (!stopCalled) {
          Thread.sleep(100)
        }

        val status = createTaskStatus("0", "s1", TaskState.TASK_FINISHED)
        backend.statusUpdate(driver, status)
      }
    }.start

    backend.stop()
    // Any method of the backend involving sending messages to the driver endpoint should not
    // be called after the backend is stopped.
    verify(driverEndpoint, never()).askWithRetry(isA(classOf[RemoveExecutor]))(any[ClassTag[_]])
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

  private def offerResources(offers: List[(Int, Int)], startId: Int = 1): Unit = {
    val mesosOffers = offers.zipWithIndex.map {case (offer, i) =>
      createOffer(s"o${i + startId}", s"s${i + startId}", offer._1, offer._2)}

    backend.resourceOffers(driver, mesosOffers.asJava)
  }

  private def verifyTaskLaunched(offerId: String): java.util.Collection[TaskInfo] = {
    val captor = ArgumentCaptor.forClass(classOf[java.util.Collection[TaskInfo]])
    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(createOfferId(offerId))),
      captor.capture())
    captor.getValue
  }

  private def createTaskStatus(taskId: String, slaveId: String, state: TaskState): TaskStatus = {
    TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(taskId).build())
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId).build())
      .setState(state)
      .build
  }


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
      shuffleClient: MesosExternalShuffleClient,
      endpoint: RpcEndpointRef): MesosCoarseGrainedSchedulerBackend = {
    val securityManager = mock[SecurityManager]

    val backend = new MesosCoarseGrainedSchedulerBackend(
        taskScheduler, sc, "master", securityManager) {
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

      override protected def getShuffleClient(): MesosExternalShuffleClient = shuffleClient

      override protected def createDriverEndpointRef(
          properties: ArrayBuffer[(String, String)]): RpcEndpointRef = endpoint

      // override to avoid race condition with the driver thread on `mesosDriver`
      override def startScheduler(newDriver: SchedulerDriver): Unit = {
        mesosDriver = newDriver
      }

      override def stopExecutors(): Unit = {
        stopCalled = true
      }

      markRegistered()
    }
    backend.start()
    backend
  }

  private def setBackend(sparkConfVars: Map[String, String] = null) {
    sparkConf = (new SparkConf)
      .setMaster("local[*]")
      .setAppName("test-mesos-dynamic-alloc")
      .setSparkHome("/path")
      .set("spark.mesos.driver.webui.url", "http://webui")

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
    driverEndpoint = mock[RpcEndpointRef]
    when(driverEndpoint.ask(any())(any())).thenReturn(Promise().future)

    backend = createSchedulerBackend(taskScheduler, driver, externalShuffleClient, driverEndpoint)
  }
}
