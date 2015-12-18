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
import java.util.Arrays
import java.util.Collection
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.mesos.Protos.Value.Scalar
import org.apache.mesos.Protos._
import org.apache.mesos.SchedulerDriver
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers}
import org.scalatest.mock.MockitoSugar

import org.apache.spark.executor.MesosExecutorBackend
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerExecutorAdded,
  TaskDescription, TaskSchedulerImpl, WorkerOffer}
import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}

class MesosSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext with MockitoSugar {
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
      .setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))
      .setHostname(s"host${slaveId}")
      .build()
  }

  private def setupSparkContext(): SparkContext = {
    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1", new ExecutorInfo("host1", 2, Map.empty)))

    val sc = mock[SparkContext]
    when(sc.getSparkHome()).thenReturn(Option("/spark-home"))

    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.executorMemory).thenReturn(100)
    when(sc.listenerBus).thenReturn(listenerBus)
    sc
  }

  test("Use configured mesosExecutor.cores for ExecutorInfo") {
    val mesosExecutorCores = 3
    val conf = new SparkConf
    conf.set("spark.mesos.mesosExecutor.cores", mesosExecutorCores.toString)

    val sc = setupSparkContext()
    when(sc.conf).thenReturn(conf)
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val mesosSchedulerBackend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val resources = Arrays.asList(
      mesosSchedulerBackend.createResource("cpus", 4),
      mesosSchedulerBackend.createResource("mem", 1024))
    // uri is null.
    val (executorInfo, _) = mesosSchedulerBackend.createExecutorInfo(resources, "test-id")
    val executorResources = executorInfo.getResourcesList
    val cpus = executorResources.asScala.find(_.getName.equals("cpus")).get.getScalar.getValue

    assert(cpus === mesosExecutorCores)
  }

  test("check spark-class location correctly") {
    val conf = new SparkConf
    conf.set("spark.mesos.executor.home", "/mesos-home")
    val sc = setupSparkContext()
    when(sc.conf).thenReturn(conf)

    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val mesosSchedulerBackend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val resources = Arrays.asList(
      mesosSchedulerBackend.createResource("cpus", 4),
      mesosSchedulerBackend.createResource("mem", 1024))
    // uri is null.
    val (executorInfo, _) = mesosSchedulerBackend.createExecutorInfo(resources, "test-id")
    assert(executorInfo.getCommand.getValue ===
      s" /mesos-home/bin/spark-class ${classOf[MesosExecutorBackend].getName}")

    // uri exists.
    conf.set("spark.executor.uri", "hdfs:///test-app-1.0.0.tgz")
    val (executorInfo1, _) = mesosSchedulerBackend.createExecutorInfo(resources, "test-id")
    assert(executorInfo1.getCommand.getValue ===
      s"cd test-app-1*;  ./bin/spark-class ${classOf[MesosExecutorBackend].getName}")
  }

  test("spark docker properties correctly populate the DockerInfo message") {
    val taskScheduler = mock[TaskSchedulerImpl]

    val conf = new SparkConf()
      .set("spark.mesos.executor.docker.image", "spark/mock")
      .set("spark.mesos.executor.docker.volumes", "/a,/b:/b,/c:/c:rw,/d:ro,/e:/e:ro")
      .set("spark.mesos.executor.docker.portmaps", "80:8080,53:53:tcp")

    val sc = setupSparkContext()
    when(sc.conf).thenReturn(conf)

    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val (execInfo, _) = backend.createExecutorInfo(
      Arrays.asList(backend.createResource("cpus", 4)), "mockExecutor")
    assert(execInfo.getContainer.getDocker.getImage.equals("spark/mock"))
    val portmaps = execInfo.getContainer.getDocker.getPortMappingsList
    assert(portmaps.get(0).getHostPort.equals(80))
    assert(portmaps.get(0).getContainerPort.equals(8080))
    assert(portmaps.get(0).getProtocol.equals("tcp"))
    assert(portmaps.get(1).getHostPort.equals(53))
    assert(portmaps.get(1).getContainerPort.equals(53))
    assert(portmaps.get(1).getProtocol.equals("tcp"))
    val volumes = execInfo.getContainer.getVolumesList
    assert(volumes.get(0).getContainerPath.equals("/a"))
    assert(volumes.get(0).getMode.equals(Volume.Mode.RW))
    assert(volumes.get(1).getContainerPath.equals("/b"))
    assert(volumes.get(1).getHostPath.equals("/b"))
    assert(volumes.get(1).getMode.equals(Volume.Mode.RW))
    assert(volumes.get(2).getContainerPath.equals("/c"))
    assert(volumes.get(2).getHostPath.equals("/c"))
    assert(volumes.get(2).getMode.equals(Volume.Mode.RW))
    assert(volumes.get(3).getContainerPath.equals("/d"))
    assert(volumes.get(3).getMode.equals(Volume.Mode.RO))
    assert(volumes.get(4).getContainerPath.equals("/e"))
    assert(volumes.get(4).getHostPath.equals("/e"))
    assert(volumes.get(4).getMode.equals(Volume.Mode.RO))
  }

  test("mesos resource offers result in launching tasks") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]

    val sc = setupSparkContext()
    when(sc.conf).thenReturn(new SparkConf)

    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val minMem = backend.calculateTotalMemory(sc)
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("o1", "s1", minMem, minCpu))
    mesosOffers.add(createOffer("o2", "s2", minMem - 1, minCpu))
    mesosOffers.add(createOffer("o3", "s3", minMem, minCpu))

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](2)
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      (minCpu - backend.mesosExecutorCores).toInt))
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(2).getSlaveId.getValue,
      mesosOffers.get(2).getHostname,
      (minCpu - backend.mesosExecutorCores).toInt))
    val taskDesc = new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    when(taskScheduler.resourceOffers(expectedWorkerOffers)).thenReturn(Seq(Seq(taskDesc)))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val capture = ArgumentCaptor.forClass(classOf[Collection[TaskInfo]])
    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        capture.capture(),
        any(classOf[Filters]))).thenReturn(Status.valueOf(1))
    when(driver.declineOffer(mesosOffers.get(1).getId)).thenReturn(Status.valueOf(1))
    when(driver.declineOffer(mesosOffers.get(2).getId)).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
      capture.capture(),
      any(classOf[Filters]))
    verify(driver, times(1)).declineOffer(mesosOffers.get(1).getId)
    verify(driver, times(1)).declineOffer(mesosOffers.get(2).getId)
    assert(capture.getValue.size() === 1)
    val taskInfo = capture.getValue.iterator().next()
    assert(taskInfo.getName.equals("n1"))
    val cpus = taskInfo.getResourcesList.get(0)
    assert(cpus.getName.equals("cpus"))
    assert(cpus.getScalar.getValue.equals(2.0))
    assert(taskInfo.getSlaveId.getValue.equals("s1"))

    // Unwanted resources offered on an existing node. Make sure they are declined
    val mesosOffers2 = new java.util.ArrayList[Offer]
    mesosOffers2.add(createOffer("o1", "s1", minMem, minCpu))
    reset(taskScheduler)
    reset(driver)
    when(taskScheduler.resourceOffers(any(classOf[Seq[WorkerOffer]]))).thenReturn(Seq(Seq()))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)
    when(driver.declineOffer(mesosOffers2.get(0).getId)).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers2)
    verify(driver, times(1)).declineOffer(mesosOffers2.get(0).getId)
  }

  test("can handle multiple roles") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]

    val sc = setupSparkContext()
    when(sc.conf).thenReturn(new SparkConf)

    val id = 1
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setRole("prod")
      .setScalar(Scalar.newBuilder().setValue(500))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setRole("prod")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(1))
    builder.addResourcesBuilder()
      .setName("mem")
      .setRole("dev")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(600))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setRole("dev")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(2))
    val offer = builder.setId(OfferID.newBuilder().setValue(s"o${id.toString}").build())
      .setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(s"s${id.toString}"))
      .setHostname(s"host${id.toString}").build()

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(offer)

    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](1)
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      2 // Deducting 1 for executor
      ))

    val taskDesc = new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    when(taskScheduler.resourceOffers(expectedWorkerOffers)).thenReturn(Seq(Seq(taskDesc)))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(1)

    val capture = ArgumentCaptor.forClass(classOf[Collection[TaskInfo]])
    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        capture.capture(),
        any(classOf[Filters]))).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
      capture.capture(),
      any(classOf[Filters]))

    assert(capture.getValue.size() === 1)
    val taskInfo = capture.getValue.iterator().next()
    assert(taskInfo.getName.equals("n1"))
    assert(taskInfo.getResourcesCount === 1)
    val cpusDev = taskInfo.getResourcesList.get(0)
    assert(cpusDev.getName.equals("cpus"))
    assert(cpusDev.getScalar.getValue.equals(1.0))
    assert(cpusDev.getRole.equals("dev"))
    val executorResources = taskInfo.getExecutor.getResourcesList.asScala
    assert(executorResources.exists { r =>
      r.getName.equals("mem") && r.getScalar.getValue.equals(484.0) && r.getRole.equals("prod")
    })
    assert(executorResources.exists { r =>
      r.getName.equals("cpus") && r.getScalar.getValue.equals(1.0) && r.getRole.equals("prod")
    })
  }

  test("does not pass offers to TaskScheduler above spark.cores.max") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val conf = new SparkConf
    conf.set("spark.cores.max", "5")

    val sc = setupSparkContext()
    when(sc.conf).thenReturn(conf)

    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val minMem = backend.calculateTotalMemory(sc)
    val offers = List(createOffer("o1", "s1", minMem, 5), createOffer("o2", "s2", minMem, 10))

    val (usableOffers, workerOffers) = backend.usableWorkerOffers(driver, offers.asJava)

    assert(usableOffers === offers, "All offers are usable")
    // 1 core is set aside for the executor, so only 4 cores are available in the worker offer
    // the second Mesos offer is not translated to a worker offer since all available cores are
    // already consumed
    assert(workerOffers === List(new WorkerOffer("s1", "hosts1", 4)))
  }

  test("correctly accounts for mesossExecutor.cores when calculating total acquired cores") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val conf = new SparkConf
    conf.set("spark.cores.max", "6")
    conf.set("spark.mesos.mesosExecutor.cores", "3")

    val sc = setupSparkContext()
    when(sc.conf).thenReturn(conf)

    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val minMem = backend.calculateTotalMemory(sc)
    val offers = List(createOffer("o1", "s1", minMem, 8), createOffer("o2", "s2", minMem, 10))

    val (usableOffers, workerOffers) = backend.usableWorkerOffers(driver, offers.asJava)

    // 3 cores are set aside for the executor, so only 3 cores are available without going
    // above `spark.cores.max`.
    // the second Mesos offer is not translated to a worker offer since all available cores are
    // already consumed
    assert(workerOffers === List(new WorkerOffer("s1", "hosts1", 3)))
  }

  test("does not launch tasks above spark.cores.max") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val conf = new SparkConf
    conf.set("spark.cores.max", "5")

    val sc = setupSparkContext()
    when(sc.conf).thenReturn(conf)

    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val minMem = backend.calculateTotalMemory(sc)
    val offers = List(createOffer("o1", "s1", minMem, 5), createOffer("o2", "s2", minMem, 10))

    val (usableOffers, workerOffers) = backend.usableWorkerOffers(driver, offers.asJava)

    // there are two tasks that fit in the first offer
    val expectedTaskDescriptions = Seq(
      new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0))),
      new TaskDescription(2L, 0, "s1", "n2", 1, ByteBuffer.wrap(new Array[Byte](0))))

    when(taskScheduler.resourceOffers(workerOffers)).thenReturn(Seq(expectedTaskDescriptions))

    val captor = ArgumentCaptor.forClass(classOf[Collection[TaskInfo]])

    backend.resourceOffers(driver, offers.asJava)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(offers.head.getId)),
      captor.capture(),
      any(classOf[Filters]))

    val launchedTaskInfos = captor.getValue.asScala
    assert(launchedTaskInfos.size == 2)
    assert(launchedTaskInfos.head.getSlaveId.getValue == "s1")

    val launchedCores =
      for (taskInfo <- launchedTaskInfos) yield
        getResource(taskInfo.getResourcesList.asScala, "cpus")

    assert(launchedCores === Seq(2.0, 2.0))

    assert(backend.totalCoresAcquired == 5)
  }

  test("cores are released when tasks are done") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val conf = new SparkConf
    conf.set("spark.cores.max", "5")
    conf.set("spark.mesos.mesosExecutor.cores", "1")

    val sc = setupSparkContext()
    when(sc.conf).thenReturn(conf)

    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val minMem = backend.calculateTotalMemory(sc)
    val offers = List(createOffer("o1", "s1", minMem, 5), createOffer("o2", "s2", minMem, 10))

    val (usableOffers, workerOffers) = backend.usableWorkerOffers(driver, offers.asJava)

    // there are two tasks that fit in the first offer
    val expectedTaskDescriptions = Seq(
      new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0))),
      new TaskDescription(2L, 0, "s1", "n2", 1, ByteBuffer.wrap(new Array[Byte](0))))

    when(taskScheduler.resourceOffers(workerOffers)).thenReturn(Seq(expectedTaskDescriptions))

    val captor = ArgumentCaptor.forClass(classOf[Collection[TaskInfo]])

    backend.resourceOffers(driver, offers.asJava)

    // we should have used all cores we are allowed to use
    assert(backend.totalCoresAcquired == 5)

    val executorId = ExecutorID.newBuilder().setValue("hosts1").build()
    val statusBuilder = TaskStatus.newBuilder()
    statusBuilder.setTaskId(TaskID.newBuilder().setValue("1").build())
    statusBuilder.setState(TaskState.TASK_FINISHED)
    statusBuilder.setExecutorId(executorId)
    backend.statusUpdate(driver, statusBuilder.build())

    statusBuilder.setTaskId(TaskID.newBuilder().setValue("2").build())
    statusBuilder.setState(TaskState.TASK_FINISHED)
    statusBuilder.setExecutorId(executorId)
    backend.statusUpdate(driver, statusBuilder.build())

    // this is called by Mesos when an executor exits
    backend.executorLost(driver, executorId, SlaveID.newBuilder().setValue("s1").build(), 0)

    assert(backend.totalCoresAcquired == 0)
  }

  private def getResource(resources: Iterable[Resource], name: String) = {
    resources.filter(_.getName == name).map(_.getScalar.getValue).sum
  }
}
