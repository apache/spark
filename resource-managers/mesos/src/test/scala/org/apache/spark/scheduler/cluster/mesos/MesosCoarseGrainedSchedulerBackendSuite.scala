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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.reflect.ClassTag

import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}
import org.apache.mesos.Protos._
import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SecurityManager, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.network.shuffle.mesos.MesosExternalShuffleClient
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RemoveExecutor}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.mesos.Utils._

class MesosCoarseGrainedSchedulerBackendSuite extends SparkFunSuite
    with LocalSparkContext
    with MockitoSugar
    with BeforeAndAfter
    with ScalaFutures {

  private var sparkConf: SparkConf = _
  private var driver: SchedulerDriver = _
  private var taskScheduler: TaskSchedulerImpl = _
  private var backend: MesosCoarseGrainedSchedulerBackend = _
  private var externalShuffleClient: MesosExternalShuffleClient = _
  private var driverEndpoint: RpcEndpointRef = _
  @volatile private var stopCalled = false

  // All 'requests' to the scheduler run immediately on the same thread, so
  // demand that all futures have their value available immediately.
  implicit override val patienceConfig = PatienceConfig(timeout = Duration(0, TimeUnit.SECONDS))

  test("mesos supports killing and limiting executors") {
    setBackend()
    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val minMem = backend.executorMemory(sc)
    val minCpu = 4
    val offers = List(Resources(minMem, minCpu))

    // launches a task on a valid offer
    offerResources(offers)
    verifyTaskLaunched(driver, "o1")

    // kills executors
    assert(backend.doRequestTotalExecutors(0).futureValue)
    assert(backend.doKillExecutors(Seq("0")).futureValue)
    val taskID0 = createTaskId("0")
    verify(driver, times(1)).killTask(taskID0)

    // doesn't launch a new task when requested executors == 0
    offerResources(offers, 2)
    verifyDeclinedOffer(driver, createOfferId("o2"))

    // Launches a new task when requested executors is positive
    backend.doRequestTotalExecutors(2)
    offerResources(offers, 2)
    verifyTaskLaunched(driver, "o2")
  }

  test("mesos supports killing and relaunching tasks with executors") {
    setBackend()

    // launches a task on a valid offer
    val minMem = backend.executorMemory(sc) + 1024
    val minCpu = 4
    val offer1 = Resources(minMem, minCpu)
    val offer2 = Resources(minMem, 1)
    offerResources(List(offer1, offer2))
    verifyTaskLaunched(driver, "o1")

    // accounts for a killed task
    val status = createTaskStatus("0", "s1", TaskState.TASK_KILLED)
    backend.statusUpdate(driver, status)
    verify(driver, times(1)).reviveOffers()

    // Launches a new task on a valid offer from the same slave
    offerResources(List(offer2))
    verifyTaskLaunched(driver, "o2")
  }

  test("mesos supports spark.executor.cores") {
    val executorCores = 4
    setBackend(Map("spark.executor.cores" -> executorCores.toString))

    val executorMemory = backend.executorMemory(sc)
    val offers = List(Resources(executorMemory * 2, executorCores + 1))
    offerResources(offers)

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.length == 1)

    val cpus = backend.getResource(taskInfos.head.getResourcesList, "cpus")
    assert(cpus == executorCores)
  }

  test("mesos supports unset spark.executor.cores") {
    setBackend()

    val executorMemory = backend.executorMemory(sc)
    val offerCores = 10
    offerResources(List(Resources(executorMemory * 2, offerCores)))

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.length == 1)

    val cpus = backend.getResource(taskInfos.head.getResourcesList, "cpus")
    assert(cpus == offerCores)
  }

  test("mesos does not acquire more than spark.cores.max") {
    val maxCores = 10
    setBackend(Map("spark.cores.max" -> maxCores.toString))

    val executorMemory = backend.executorMemory(sc)
    offerResources(List(Resources(executorMemory, maxCores + 1)))

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.length == 1)

    val cpus = backend.getResource(taskInfos.head.getResourcesList, "cpus")
    assert(cpus == maxCores)
  }

  test("mesos does not acquire gpus if not specified") {
    setBackend()

    val executorMemory = backend.executorMemory(sc)
    offerResources(List(Resources(executorMemory, 1, 1)))

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.length == 1)

    val gpus = backend.getResource(taskInfos.head.getResourcesList, "gpus")
    assert(gpus == 0.0)
  }


  test("mesos does not acquire more than spark.mesos.gpus.max") {
    val maxGpus = 5
    setBackend(Map("spark.mesos.gpus.max" -> maxGpus.toString))

    val executorMemory = backend.executorMemory(sc)
    offerResources(List(Resources(executorMemory, 1, maxGpus + 1)))

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.length == 1)

    val gpus = backend.getResource(taskInfos.head.getResourcesList, "gpus")
    assert(gpus == maxGpus)
  }


  test("mesos declines offers that violate attribute constraints") {
    setBackend(Map("spark.mesos.constraints" -> "x:true"))
    offerResources(List(Resources(backend.executorMemory(sc), 4)))
    verifyDeclinedOffer(driver, createOfferId("o1"), true)
  }

  test("mesos declines offers with a filter when reached spark.cores.max") {
    val maxCores = 3
    setBackend(Map("spark.cores.max" -> maxCores.toString))

    val executorMemory = backend.executorMemory(sc)
    offerResources(List(
      Resources(executorMemory, maxCores + 1),
      Resources(executorMemory, maxCores + 1)))

    verifyTaskLaunched(driver, "o1")
    verifyDeclinedOffer(driver, createOfferId("o2"), true)
  }

  test("mesos declines offers with a filter when maxCores not a multiple of executor.cores") {
    val maxCores = 4
    val executorCores = 3
    setBackend(Map(
      "spark.cores.max" -> maxCores.toString,
      "spark.executor.cores" -> executorCores.toString
    ))
    val executorMemory = backend.executorMemory(sc)
    offerResources(List(
      Resources(executorMemory, maxCores + 1),
      Resources(executorMemory, maxCores + 1)
    ))
    verifyTaskLaunched(driver, "o1")
    verifyDeclinedOffer(driver, createOfferId("o2"), true)
  }

  test("mesos declines offers with a filter when reached spark.cores.max with executor.cores") {
    val maxCores = 4
    val executorCores = 2
    setBackend(Map(
      "spark.cores.max" -> maxCores.toString,
      "spark.executor.cores" -> executorCores.toString
    ))
    val executorMemory = backend.executorMemory(sc)
    offerResources(List(
      Resources(executorMemory, maxCores + 1),
      Resources(executorMemory, maxCores + 1),
      Resources(executorMemory, maxCores + 1)
    ))
    verifyTaskLaunched(driver, "o1")
    verifyTaskLaunched(driver, "o2")
    verifyDeclinedOffer(driver, createOfferId("o3"), true)
  }

  test("mesos assigns tasks round-robin on offers") {
    val executorCores = 4
    val maxCores = executorCores * 2
    setBackend(Map("spark.executor.cores" -> executorCores.toString,
      "spark.cores.max" -> maxCores.toString))

    val executorMemory = backend.executorMemory(sc)
    offerResources(List(
      Resources(executorMemory * 2, executorCores * 2),
      Resources(executorMemory * 2, executorCores * 2)))

    verifyTaskLaunched(driver, "o1")
    verifyTaskLaunched(driver, "o2")
  }

  test("mesos creates multiple executors on a single slave") {
    val executorCores = 4
    setBackend(Map("spark.executor.cores" -> executorCores.toString))

    // offer with room for two executors
    val executorMemory = backend.executorMemory(sc)
    offerResources(List(Resources(executorMemory * 2, executorCores * 2)))

    // verify two executors were started on a single offer
    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.length == 2)
  }

  test("mesos doesn't register twice with the same shuffle service") {
    setBackend(Map("spark.shuffle.service.enabled" -> "true"))
    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)
    verifyTaskLaunched(driver, "o1")

    val offer2 = createOffer("o2", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer2).asJava)
    verifyTaskLaunched(driver, "o2")

    val status1 = createTaskStatus("0", "s1", TaskState.TASK_RUNNING)
    backend.statusUpdate(driver, status1)

    val status2 = createTaskStatus("1", "s1", TaskState.TASK_RUNNING)
    backend.statusUpdate(driver, status2)
    verify(externalShuffleClient, times(1))
      .registerDriverWithShuffleService(anyString, anyInt, anyLong, anyLong)
  }

  test("Port offer decline when there is no appropriate range") {
    setBackend(Map(BLOCK_MANAGER_PORT.key -> "30100"))
    val offeredPorts = (31100L, 31200L)
    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu, Some(offeredPorts))
    backend.resourceOffers(driver, List(offer1).asJava)
    verify(driver, times(1)).declineOffer(offer1.getId)
  }

  test("Port offer accepted when ephemeral ports are used") {
    setBackend()
    val offeredPorts = (31100L, 31200L)
    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu, Some(offeredPorts))
    backend.resourceOffers(driver, List(offer1).asJava)
    verifyTaskLaunched(driver, "o1")
  }

  test("Port offer accepted with user defined port numbers") {
    val port = 30100
    setBackend(Map(BLOCK_MANAGER_PORT.key -> s"$port"))
    val offeredPorts = (30000L, 31000L)
    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu, Some(offeredPorts))
    backend.resourceOffers(driver, List(offer1).asJava)
    val taskInfo = verifyTaskLaunched(driver, "o1")

    val taskPortResources = taskInfo.head.getResourcesList.asScala.
    find(r => r.getType == Value.Type.RANGES && r.getName == "ports")

    val isPortInOffer = (r: Resource) => {
      r.getRanges().getRangeList
        .asScala.exists(range => range.getBegin == port && range.getEnd == port)
    }
    assert(taskPortResources.exists(isPortInOffer))
  }

  test("mesos kills an executor when told") {
    setBackend()

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)
    verifyTaskLaunched(driver, "o1")

    backend.doKillExecutors(List("0"))
    verify(driver, times(1)).killTask(createTaskId("0"))
  }

  test("weburi is set in created scheduler driver") {
    initializeSparkConf()
    sc = new SparkContext(sparkConf)

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

  test("honors unset spark.mesos.containerizer") {
    setBackend(Map("spark.mesos.executor.docker.image" -> "test"))

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.head.getContainer.getType == ContainerInfo.Type.DOCKER)
  }

  test("honors spark.mesos.containerizer=\"mesos\"") {
    setBackend(Map(
      "spark.mesos.executor.docker.image" -> "test",
      "spark.mesos.containerizer" -> "mesos"))

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.head.getContainer.getType == ContainerInfo.Type.MESOS)
  }

  test("docker settings are reflected in created tasks") {
    setBackend(Map(
      "spark.mesos.executor.docker.image" -> "some_image",
      "spark.mesos.executor.docker.forcePullImage" -> "true",
      "spark.mesos.executor.docker.volumes" -> "/host_vol:/container_vol:ro",
      "spark.mesos.executor.docker.portmaps" -> "8080:80:tcp"
    ))

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    val launchedTasks = verifyTaskLaunched(driver, "o1")
    assert(launchedTasks.size == 1)

    val containerInfo = launchedTasks.head.getContainer
    assert(containerInfo.getType == ContainerInfo.Type.DOCKER)

    val volumes = containerInfo.getVolumesList.asScala
    assert(volumes.size == 1)

    val volume = volumes.head
    assert(volume.getHostPath == "/host_vol")
    assert(volume.getContainerPath == "/container_vol")
    assert(volume.getMode == Volume.Mode.RO)

    val dockerInfo = containerInfo.getDocker

    val portMappings = dockerInfo.getPortMappingsList.asScala
    assert(portMappings.size == 1)

    val portMapping = portMappings.head
    assert(portMapping.getHostPort == 8080)
    assert(portMapping.getContainerPort == 80)
    assert(portMapping.getProtocol == "tcp")
  }

  test("force-pull-image option is disabled by default") {
    setBackend(Map(
      "spark.mesos.executor.docker.image" -> "some_image"
    ))

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    val launchedTasks = verifyTaskLaunched(driver, "o1")
    assert(launchedTasks.size == 1)

    val containerInfo = launchedTasks.head.getContainer
    assert(containerInfo.getType == ContainerInfo.Type.DOCKER)

    val dockerInfo = containerInfo.getDocker

    assert(dockerInfo.getImage == "some_image")
    assert(!dockerInfo.getForcePullImage)
  }

  test("mesos supports spark.executor.uri") {
    val url = "spark.spark.spark.com"
    setBackend(Map(
      "spark.executor.uri" -> url
    ), null)

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    val launchedTasks = verifyTaskLaunched(driver, "o1")
    assert(launchedTasks.head.getCommand.getUrisList.asScala(0).getValue == url)
  }

  test("mesos supports setting fetcher cache") {
    val url = "spark.spark.spark.com"
    setBackend(Map(
      "spark.mesos.fetcherCache.enable" -> "true",
      "spark.executor.uri" -> url
    ), null)
    val offers = List(Resources(backend.executorMemory(sc), 1))
    offerResources(offers)
    val launchedTasks = verifyTaskLaunched(driver, "o1")
    val uris = launchedTasks.head.getCommand.getUrisList
    assert(uris.size() == 1)
    assert(uris.asScala.head.getCache)
  }

  test("mesos supports disabling fetcher cache") {
    val url = "spark.spark.spark.com"
    setBackend(Map(
      "spark.mesos.fetcherCache.enable" -> "false",
      "spark.executor.uri" -> url
    ), null)
    val offers = List(Resources(backend.executorMemory(sc), 1))
    offerResources(offers)
    val launchedTasks = verifyTaskLaunched(driver, "o1")
    val uris = launchedTasks.head.getCommand.getUrisList
    assert(uris.size() == 1)
    assert(!uris.asScala.head.getCache)
  }

  test("mesos sets task name to spark.app.name") {
    setBackend()

    val offers = List(Resources(backend.executorMemory(sc), 1))
    offerResources(offers)
    val launchedTasks = verifyTaskLaunched(driver, "o1")

    // Add " 0" to the taskName to match the executor number that is appended
    assert(launchedTasks.head.getName == "test-mesos-dynamic-alloc 0")
  }

  test("mesos sets configurable labels on tasks") {
    val taskLabelsString = "mesos:test,label:test"
    setBackend(Map(
      "spark.mesos.task.labels" -> taskLabelsString
    ))

    // Build up the labels
    val taskLabels = Protos.Labels.newBuilder()
      .addLabels(Protos.Label.newBuilder()
        .setKey("mesos").setValue("test").build())
      .addLabels(Protos.Label.newBuilder()
        .setKey("label").setValue("test").build())
      .build()

    val offers = List(Resources(backend.executorMemory(sc), 1))
    offerResources(offers)
    val launchedTasks = verifyTaskLaunched(driver, "o1")

    val labels = launchedTasks.head.getLabels

    assert(launchedTasks.head.getLabels.equals(taskLabels))
  }

  test("mesos ignored invalid labels and sets configurable labels on tasks") {
    val taskLabelsString = "mesos:test,label:test,incorrect:label:here"
    setBackend(Map(
      "spark.mesos.task.labels" -> taskLabelsString
    ))

    // Build up the labels
    val taskLabels = Protos.Labels.newBuilder()
      .addLabels(Protos.Label.newBuilder()
        .setKey("mesos").setValue("test").build())
      .addLabels(Protos.Label.newBuilder()
        .setKey("label").setValue("test").build())
      .build()

    val offers = List(Resources(backend.executorMemory(sc), 1))
    offerResources(offers)
    val launchedTasks = verifyTaskLaunched(driver, "o1")

    val labels = launchedTasks.head.getLabels

    assert(launchedTasks.head.getLabels.equals(taskLabels))
  }

  test("mesos supports spark.mesos.network.name") {
    setBackend(Map(
      "spark.mesos.network.name" -> "test-network-name"
    ))

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    val launchedTasks = verifyTaskLaunched(driver, "o1")
    val networkInfos = launchedTasks.head.getContainer.getNetworkInfosList
    assert(networkInfos.size == 1)
    assert(networkInfos.get(0).getName == "test-network-name")
  }

  test("supports spark.scheduler.minRegisteredResourcesRatio") {
    val expectedCores = 1
    setBackend(Map(
      "spark.cores.max" -> expectedCores.toString,
      "spark.scheduler.minRegisteredResourcesRatio" -> "1.0"))

    val offers = List(Resources(backend.executorMemory(sc), expectedCores))
    offerResources(offers)
    val launchedTasks = verifyTaskLaunched(driver, "o1")
    assert(!backend.isReady)

    registerMockExecutor(launchedTasks(0).getTaskId.getValue, "s1", expectedCores)
    assert(backend.isReady)
  }

  private case class Resources(mem: Int, cpus: Int, gpus: Int = 0)

  private def registerMockExecutor(executorId: String, slaveId: String, cores: Integer) = {
    val mockEndpointRef = mock[RpcEndpointRef]
    val mockAddress = mock[RpcAddress]
    val message = RegisterExecutor(executorId, mockEndpointRef, slaveId, cores, Map.empty)

    backend.driverEndpoint.askSync[Boolean](message)
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

  private def offerResources(offers: List[Resources], startId: Int = 1): Unit = {
    val mesosOffers = offers.zipWithIndex.map {case (offer, i) =>
      createOffer(s"o${i + startId}", s"s${i + startId}", offer.mem, offer.cpus, None, offer.gpus)}

    backend.resourceOffers(driver, mesosOffers.asJava)
  }

  private def createTaskStatus(taskId: String, slaveId: String, state: TaskState): TaskStatus = {
    TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(taskId).build())
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId).build())
      .setState(state)
      .build
  }

  private def createSchedulerBackend(
      taskScheduler: TaskSchedulerImpl,
      driver: SchedulerDriver,
      shuffleClient: MesosExternalShuffleClient) = {
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

      // override to avoid race condition with the driver thread on `mesosDriver`
      override def startScheduler(newDriver: SchedulerDriver): Unit = {}

      override def stopExecutors(): Unit = {
        stopCalled = true
      }
    }
    backend.start()
    backend.registered(driver, Utils.TEST_FRAMEWORK_ID, Utils.TEST_MASTER_INFO)
    backend
  }

  private def initializeSparkConf(
    sparkConfVars: Map[String, String] = null,
    home: String = "/path"): Unit = {
    sparkConf = (new SparkConf)
      .setMaster("local[*]")
      .setAppName("test-mesos-dynamic-alloc")
      .set("spark.mesos.driver.webui.url", "http://webui")

    if (home != null) {
      sparkConf.setSparkHome(home)
    }

    if (sparkConfVars != null) {
      sparkConf.setAll(sparkConfVars)
    }
  }

  private def setBackend(sparkConfVars: Map[String, String] = null, home: String = "/path") {
    initializeSparkConf(sparkConfVars, home)
    sc = new SparkContext(sparkConf)

    driver = mock[SchedulerDriver]
    when(driver.start()).thenReturn(Protos.Status.DRIVER_RUNNING)

    taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)

    externalShuffleClient = mock[MesosExternalShuffleClient]

    backend = createSchedulerBackend(taskScheduler, driver, externalShuffleClient)
  }
}
