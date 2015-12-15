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

import java.nio.ByteBuffer
import java.util
import java.util.Collections

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.mesos.Protos.Value.Scalar
import org.apache.mesos.Protos.{TaskState, _}
import org.apache.mesos.{Protos, SchedulerDriver}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers => MMatchers}
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.mock.MockitoSugar

import org.apache.spark.scheduler.{ExecutorLossReason, TaskDescription, TaskSchedulerImpl, WorkerOffer}
import org.apache.spark.{LocalSparkContext, SecurityManager, SparkConf, SparkContext, SparkEnv, SparkFunSuite}


class CoarseMesosSchedulerBackendSuite extends SparkFunSuite
    with LocalSparkContext
    with Matchers
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
      override def start(): Unit = {
        mesosDriver = driver
      }
      override def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = None
      markRegistered()
    }
    backend.start()
    backend
  }

  var sparkConf: SparkConf = _
  var sparkEnv: SparkEnv = _
  var taskScheduler: TaskSchedulerImpl = _

  before {
    sparkConf = (new SparkConf)
      .setMaster("local[*]")
      .setAppName("test-mesos-dynamic-alloc")
      .setSparkHome("/path")
      .set("spark.testing", "1")
    sc = mock[SparkContext]
    sparkEnv = mock[SparkEnv]
    taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)
    when(taskScheduler.sc).thenReturn(sc)
    when(sc.executorMemory).thenReturn(100)
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    when(sc.env).thenReturn(sparkEnv)
    when(sc.conf).thenReturn(sparkConf)
  }

  test("mesos supports killing and limiting executors") {
    val driver = mock[SchedulerDriver]
    when(driver.start()).thenReturn(Protos.Status.DRIVER_RUNNING)

    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val backend = createSchedulerBackend(taskScheduler, driver)
    val minMem = backend.calculateTotalMemory(sc)
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("o1", "s1", minMem, minCpu))

    val taskID0 = TaskID.newBuilder().setValue("s1/0").build()

    backend.resourceOffers(driver, mesosOffers)
    verify(driver, times(1)).launchTasks(
      MMatchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
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
      .declineOffer(
        MMatchers.eq(OfferID.newBuilder().setValue("o2").build()),
        MMatchers.any(classOf[Filters]))

    // Verify we didn't launch any new executor
    assert(backend.slaveIdsWithExecutors.size === 1)

    backend.doRequestTotalExecutors(2)
    backend.resourceOffers(driver, mesosOffers2)
    verify(driver, times(1)).launchTasks(
      MMatchers.eq(Collections.singleton(mesosOffers2.get(0).getId)),
      any[util.Collection[TaskInfo]],
      any[Filters])

    assert(backend.slaveIdsWithExecutors.size === 2)
    backend.slaveLost(driver, SlaveID.newBuilder().setValue("s1").build())
    assert(backend.slaveIdsWithExecutors.size === 1)
  }

  test("mesos supports killing and relaunching tasks with executors") {
    val driver = mock[SchedulerDriver]
    when(driver.start()).thenReturn(Protos.Status.DRIVER_RUNNING)

    val backend = createSchedulerBackend(taskScheduler, driver)
    val minMem = backend.calculateTotalMemory(sc) + 1024
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    val offer1 = createOffer("o1", "s1", minMem, minCpu)
    mesosOffers.add(offer1)

    val offer2 = createOffer("o2", "s1", minMem, 1);

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      MMatchers.eq(Collections.singleton(offer1.getId)),
      anyObject(),
      anyObject[Filters])

    // Simulate task killed, executor no longer running
    val status = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue("s1/0").build())
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
      MMatchers.eq(Collections.singleton(offer2.getId)),
      anyObject(),
      anyObject[Filters])

    verify(driver, times(1)).reviveOffers()
  }

  test("launch multiple executors") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    val env = mock[SparkEnv]
    val sc = mock[SparkContext]
    when(taskScheduler.sc).thenReturn(sc)
    when(sc.executorMemory).thenReturn(100)
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    when(sc.env).thenReturn(env)
    val conf = new SparkConf
    conf.set("spark.driver.host", "localhost")
    conf.set("spark.driver.port", "1234")
    conf.set("spark.mesos.coarse.executors.max", "2")
    conf.set("spark.mesos.coarse.executor.cores.max", "2")
    conf.set("spark.mesos.coarse.cores.max", "2")
    conf.set("spark.testing", "1")
    when(sc.conf).thenReturn(conf)

    val securityManager = mock[SecurityManager]

    val backend = new CoarseMesosSchedulerBackend(taskScheduler, sc, "master", securityManager)

    val minMem = backend.calculateTotalMemory(sc)
    val minCpu = 2

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("1", "s1", minMem * 2, minCpu * 2))

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](1)
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      2
    ))

    val taskDesc = new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    val taskDesc2 = new TaskDescription(2L, 0, "s2", "n2", 0, ByteBuffer.wrap(new Array[Byte](0)))
    when(taskScheduler.resourceOffers(MMatchers.eq(expectedWorkerOffers)))
      .thenReturn(Seq(Seq(taskDesc, taskDesc2)))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)
    when(taskScheduler.sc).thenReturn(sc)

    val capture = ArgumentCaptor.forClass(classOf[util.Collection[TaskInfo]])
    when(
      driver.launchTasks(
        MMatchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        capture.capture(),
        MMatchers.any())
    ).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers)

    assert(capture.getValue.size() == 2)
    val iter = capture.getValue.iterator()
    val taskInfo = iter.next()
    taskInfo.getName should be("Task s1/0")
    val cpus = taskInfo.getResourcesList.get(0)
    cpus.getName should be("cpus")
    cpus.getScalar.getValue should be(2.0)
    taskInfo.getSlaveId.getValue should be("s1")

    val taskInfo2 = iter.next()
    taskInfo2.getName should be("Task s1/1")
    val cpus2 = taskInfo2.getResourcesList.get(0)
    cpus2.getName should be("cpus")
    cpus2.getScalar.getValue should be(2.0)
    taskInfo2.getSlaveId.getValue should be("s1")

    // Already capped the max executors, shouldn't launch a new one.
    val mesosOffers2 = new java.util.ArrayList[Offer]
    mesosOffers2.add(createOffer("1", "s1", minMem, minCpu))
    when(taskScheduler.resourceOffers(MMatchers.any(classOf[Seq[WorkerOffer]])))
      .thenReturn(Seq(Seq()))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)
    when(driver.declineOffer(
      MMatchers.eq(mesosOffers2.get(0).getId),
      MMatchers.any(classOf[Filters])))
      .thenReturn(Status.valueOf(1))
    backend.resourceOffers(driver, mesosOffers2)
  }
}
