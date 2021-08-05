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
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}
import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.Scalar
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong, eq => meq}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext,
  SparkFunSuite}
import org.apache.spark.deploy.mesos.config._
import org.apache.spark.executor.MesosExecutorBackend
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerExecutorAdded,
  TaskDescription, TaskSchedulerImpl, WorkerOffer}
import org.apache.spark.scheduler.cluster.ExecutorInfo

class MesosFineGrainedSchedulerBackendSuite
  extends SparkFunSuite with LocalSparkContext with MockitoSugar {

  test("weburi is set in created scheduler driver") {
    val conf = new SparkConf
    conf.set(DRIVER_WEBUI_URL, "http://webui")
    conf.set("spark.app.name", "name1")

    val sc = mock[SparkContext]
    when(sc.conf).thenReturn(conf)
    when(sc.sparkUser).thenReturn("sparkUser1")
    when(sc.appName).thenReturn("appName1")

    val taskScheduler = mock[TaskSchedulerImpl]
    val driver = mock[SchedulerDriver]
    when(driver.start()).thenReturn(Protos.Status.DRIVER_RUNNING)

    val backend = new MesosFineGrainedSchedulerBackend(taskScheduler, sc, "master") {
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

  test("Use configured mesosExecutor.cores for ExecutorInfo") {
    val mesosExecutorCores = 3.0
    val conf = new SparkConf
    conf.set(EXECUTOR_CORES, mesosExecutorCores)

    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1",
        new ExecutorInfo("host1", 2, Map.empty, Map.empty)))

    val sc = mock[SparkContext]
    when(sc.getSparkHome()).thenReturn(Option("/spark-home"))

    when(sc.conf).thenReturn(conf)
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.executorMemory).thenReturn(100)
    when(sc.listenerBus).thenReturn(listenerBus)
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val mesosSchedulerBackend = new MesosFineGrainedSchedulerBackend(taskScheduler, sc, "master")

    val resources = Arrays.asList(
      mesosSchedulerBackend.createResource("cpus", 4),
      mesosSchedulerBackend.createResource("mem", 1024))
    // uri is null.
    val (executorInfo, _) = mesosSchedulerBackend.createExecutorInfo(resources, "test-id")
    val executorResources = executorInfo.getResourcesList
    val cpus = executorResources.asScala.find(_.getName == "cpus").get.getScalar.getValue

    assert(cpus === mesosExecutorCores)
  }

  test("check spark-class location correctly") {
    val conf = new SparkConf
    conf.set(EXECUTOR_HOME, "/mesos-home")

    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1",
        new ExecutorInfo("host1", 2, Map.empty, Map.empty)))

    val sc = mock[SparkContext]
    when(sc.getSparkHome()).thenReturn(Option("/spark-home"))

    when(sc.conf).thenReturn(conf)
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.executorMemory).thenReturn(100)
    when(sc.listenerBus).thenReturn(listenerBus)
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val mesosSchedulerBackend = new MesosFineGrainedSchedulerBackend(taskScheduler, sc, "master")

    val resources = Arrays.asList(
      mesosSchedulerBackend.createResource("cpus", 4),
      mesosSchedulerBackend.createResource("mem", 1024))
    // uri is null.
    val (executorInfo, _) = mesosSchedulerBackend.createExecutorInfo(resources, "test-id")
    assert(executorInfo.getCommand.getValue ===
      s" /mesos-home/bin/spark-class ${classOf[MesosExecutorBackend].getName}")

    // uri exists.
    conf.set(EXECUTOR_URI, "hdfs:///test-app-1.0.0.tgz")
    val (executorInfo1, _) = mesosSchedulerBackend.createExecutorInfo(resources, "test-id")
    assert(executorInfo1.getCommand.getValue ===
      s"cd test-app-1*;  ./bin/spark-class ${classOf[MesosExecutorBackend].getName}")
  }

  test("spark docker properties correctly populate the DockerInfo message") {
    val taskScheduler = mock[TaskSchedulerImpl]

    val conf = new SparkConf()
      .set(EXECUTOR_DOCKER_IMAGE, "spark/mock")
      .set(EXECUTOR_DOCKER_FORCE_PULL_IMAGE, true)
      .set(EXECUTOR_DOCKER_VOLUMES, Seq("/a", "/b:/b", "/c:/c:rw", "/d:ro", "/e:/e:ro"))
      .set(EXECUTOR_DOCKER_PORT_MAPS, Seq("80:8080", "53:53:tcp"))

    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1",
        new ExecutorInfo("host1", 2, Map.empty, Map.empty)))

    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/spark-home"))
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.conf).thenReturn(conf)
    when(sc.listenerBus).thenReturn(listenerBus)

    val backend = new MesosFineGrainedSchedulerBackend(taskScheduler, sc, "master")

    val (execInfo, _) = backend.createExecutorInfo(
      Arrays.asList(backend.createResource("cpus", 4)), "mockExecutor")
    assert(execInfo.getContainer.getDocker.getImage.equals("spark/mock"))
    assert(execInfo.getContainer.getDocker.getForcePullImage)
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
    def createOffer(id: Int, mem: Int, cpu: Int): Offer = {
      val builder = Offer.newBuilder()
      builder.addResourcesBuilder()
        .setName("mem")
        .setType(Value.Type.SCALAR)
        .setScalar(Scalar.newBuilder().setValue(mem))
      builder.addResourcesBuilder()
        .setName("cpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Scalar.newBuilder().setValue(cpu))
      builder.setId(OfferID.newBuilder().setValue(s"o${id.toString}").build())
        .setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
        .setSlaveId(SlaveID.newBuilder().setValue(s"s${id.toString}"))
        .setHostname(s"host${id.toString}").build()
    }

    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]

    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1",
        new ExecutorInfo("host1", 2, Map.empty, Map.empty)))

    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.conf).thenReturn(new SparkConf)
    when(sc.listenerBus).thenReturn(listenerBus)

    val backend = new MesosFineGrainedSchedulerBackend(taskScheduler, sc, "master")

    val minMem = backend.executorMemory(sc)
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer(1, minMem, minCpu))
    mesosOffers.add(createOffer(2, minMem - 1, minCpu))
    mesosOffers.add(createOffer(3, minMem, minCpu))

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](2)
    expectedWorkerOffers += new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      (minCpu - backend.mesosExecutorCores).toInt
    )
    expectedWorkerOffers += new WorkerOffer(
      mesosOffers.get(2).getSlaveId.getValue,
      mesosOffers.get(2).getHostname,
      (minCpu - backend.mesosExecutorCores).toInt
    )
    val taskDesc = new TaskDescription(
      taskId = 1L,
      attemptNumber = 0,
      executorId = "s1",
      name = "n1",
      index = 0,
      partitionId = 0,
      addedFiles = mutable.Map.empty[String, Long],
      addedJars = mutable.Map.empty[String, Long],
      addedArchives = mutable.Map.empty[String, Long],
      properties = new Properties(),
      cpus = 1,
      resources = immutable.Map.empty[String, ResourceInformation],
      ByteBuffer.wrap(new Array[Byte](0)))
    when(taskScheduler.resourceOffers(
      expectedWorkerOffers.toIndexedSeq)).thenReturn(Seq(Seq(taskDesc)))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val capture = ArgumentCaptor.forClass(classOf[Collection[TaskInfo]])
    when(
      driver.launchTasks(
        meq(Collections.singleton(mesosOffers.get(0).getId)),
        capture.capture(),
        any(classOf[Filters])
      )
    ).thenReturn(Status.valueOf(1))
    when(driver.declineOffer(mesosOffers.get(1).getId)).thenReturn(Status.valueOf(1))
    when(driver.declineOffer(mesosOffers.get(2).getId)).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      meq(Collections.singleton(mesosOffers.get(0).getId)),
      capture.capture(),
      any(classOf[Filters])
    )
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
    mesosOffers2.add(createOffer(1, minMem, minCpu))
    reset(taskScheduler)
    reset(driver)
    when(taskScheduler.resourceOffers(any(classOf[IndexedSeq[WorkerOffer]]), any[Boolean]))
      .thenReturn(Seq(Seq()))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)
    when(driver.declineOffer(mesosOffers2.get(0).getId)).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers2)
    verify(driver, times(1)).declineOffer(mesosOffers2.get(0).getId)
  }

  test("can handle multiple roles") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]

    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1",
        new ExecutorInfo("host1", 2, Map.empty, Map.empty)))

    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.conf).thenReturn(new SparkConf)
    when(sc.listenerBus).thenReturn(listenerBus)

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

    val backend = new MesosFineGrainedSchedulerBackend(taskScheduler, sc, "master")

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](1)
    expectedWorkerOffers += new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      2 // Deducting 1 for executor
    )

    val taskDesc = new TaskDescription(
      taskId = 1L,
      attemptNumber = 0,
      executorId = "s1",
      name = "n1",
      index = 0,
      partitionId = 0,
      addedFiles = mutable.Map.empty[String, Long],
      addedJars = mutable.Map.empty[String, Long],
      addedArchives = mutable.Map.empty[String, Long],
      properties = new Properties(),
      cpus = 1,
      resources = immutable.Map.empty[String, ResourceInformation],
      ByteBuffer.wrap(new Array[Byte](0)))
    when(taskScheduler.resourceOffers(
      expectedWorkerOffers.toIndexedSeq)).thenReturn(Seq(Seq(taskDesc)))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(1)

    val capture = ArgumentCaptor.forClass(classOf[Collection[TaskInfo]])
    when(
      driver.launchTasks(
        meq(Collections.singleton(mesosOffers.get(0).getId)),
        capture.capture(),
        any(classOf[Filters])
      )
    ).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      meq(Collections.singleton(mesosOffers.get(0).getId)),
      capture.capture(),
      any(classOf[Filters])
    )

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
}
