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

import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}
import org.apache.mesos.Protos._
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, anyString, eq => meq}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{LocalSparkContext, SecurityManager, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.mesos.{config => mesosConfig}
import org.apache.spark.internal.config._
import org.apache.spark.network.shuffle.mesos.MesosExternalBlockStoreClient
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor}
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
  private var externalShuffleClient: MesosExternalBlockStoreClient = _
  private var driverEndpoint: RpcEndpointRef = _
  @volatile private var stopCalled = false

  // All 'requests' to the scheduler run immediately on the same thread, so
  // demand that all futures have their value available immediately.
  implicit override val patienceConfig = PatienceConfig(timeout = Duration(0, TimeUnit.SECONDS))

  test("mesos supports killing and limiting executors") {
    setBackend()
    sparkConf.set(DRIVER_HOST_ADDRESS, "driverHost")
    sparkConf.set(DRIVER_PORT, 1234)

    val minMem = backend.executorMemory(sc)
    val minCpu = 4
    val offers = List(Resources(minMem, minCpu))

    // launches a task on a valid offer
    offerResources(offers)
    verifyTaskLaunched(driver, "o1")

    val totalExecs = Map(ResourceProfile.getOrCreateDefaultProfile(sparkConf) -> 0)
    // kills executors
    val defaultResourceProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    assert(backend.doRequestTotalExecutors(Map(defaultResourceProfile -> 0)).futureValue)
    assert(backend.doKillExecutors(Seq("0")).futureValue)
    val taskID0 = createTaskId("0")
    verify(driver, times(1)).killTask(taskID0)

    // doesn't launch a new task when requested executors == 0
    offerResources(offers, 2)
    verifyDeclinedOffer(driver, createOfferId("o2"))

    // Launches a new task when requested executors is positive
    backend.doRequestTotalExecutors(Map(defaultResourceProfile -> 2))
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

    // Launches a new task on a valid offer from the same agent
    offerResources(List(offer2))
    verifyTaskLaunched(driver, "o2")
  }

  test("mesos supports spark.executor.cores") {
    val executorCores = 4
    setBackend(Map(EXECUTOR_CORES.key -> executorCores.toString))

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
    setBackend(Map(CORES_MAX.key -> maxCores.toString))

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
    setBackend(Map(mesosConfig.MAX_GPUS.key -> maxGpus.toString))

    val executorMemory = backend.executorMemory(sc)
    offerResources(List(Resources(executorMemory, 1, maxGpus + 1)))

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.length == 1)

    val gpus = backend.getResource(taskInfos.head.getResourcesList, "gpus")
    assert(gpus == maxGpus)
  }


  test("mesos declines offers that violate attribute constraints") {
    setBackend(Map(mesosConfig.CONSTRAINTS.key -> "x:true"))
    offerResources(List(Resources(backend.executorMemory(sc), 4)))
    verifyDeclinedOffer(driver, createOfferId("o1"), true)
  }

  test("mesos declines offers with a filter when reached spark.cores.max") {
    val maxCores = 3
    setBackend(Map(CORES_MAX.key -> maxCores.toString))

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
      CORES_MAX.key -> maxCores.toString,
      EXECUTOR_CORES.key -> executorCores.toString
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
      CORES_MAX.key -> maxCores.toString,
      EXECUTOR_CORES.key -> executorCores.toString
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
    setBackend(Map(EXECUTOR_CORES.key -> executorCores.toString,
      CORES_MAX.key -> maxCores.toString))

    val executorMemory = backend.executorMemory(sc)
    offerResources(List(
      Resources(executorMemory * 2, executorCores * 2),
      Resources(executorMemory * 2, executorCores * 2)))

    verifyTaskLaunched(driver, "o1")
    verifyTaskLaunched(driver, "o2")
  }

  test("mesos creates multiple executors on a single agent") {
    val executorCores = 4
    setBackend(Map(EXECUTOR_CORES.key -> executorCores.toString))

    // offer with room for two executors
    val executorMemory = backend.executorMemory(sc)
    offerResources(List(Resources(executorMemory * 2, executorCores * 2)))

    // verify two executors were started on a single offer
    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.length == 2)
  }

  test("mesos doesn't register twice with the same shuffle service") {
    setBackend(Map(SHUFFLE_SERVICE_ENABLED.key -> "true"))
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

  test("failover timeout is set in created scheduler driver") {
    val failoverTimeoutIn = 3600.0
    initializeSparkConf(Map(mesosConfig.DRIVER_FAILOVER_TIMEOUT.key -> failoverTimeoutIn.toString))
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
        assert(failoverTimeout.isDefined)
        assert(failoverTimeout.get.equals(failoverTimeoutIn))
        driver
      }
    }

    backend.start()
  }

  test("honors unset spark.mesos.containerizer") {
    setBackend(Map(mesosConfig.EXECUTOR_DOCKER_IMAGE.key -> "test"))

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.head.getContainer.getType == ContainerInfo.Type.DOCKER)
  }

  test("honors spark.mesos.containerizer=\"mesos\"") {
    setBackend(Map(
      mesosConfig.EXECUTOR_DOCKER_IMAGE.key -> "test",
      mesosConfig.CONTAINERIZER.key -> "mesos"))

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    val taskInfos = verifyTaskLaunched(driver, "o1")
    assert(taskInfos.head.getContainer.getType == ContainerInfo.Type.MESOS)
  }

  test("docker settings are reflected in created tasks") {
    setBackend(Map(
      mesosConfig.EXECUTOR_DOCKER_IMAGE.key -> "some_image",
      mesosConfig.EXECUTOR_DOCKER_FORCE_PULL_IMAGE.key -> "true",
      mesosConfig.EXECUTOR_DOCKER_VOLUMES.key -> "/host_vol:/container_vol:ro",
      mesosConfig.EXECUTOR_DOCKER_PORT_MAPS.key -> "8080:80:tcp"
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
      mesosConfig.EXECUTOR_DOCKER_IMAGE.key -> "some_image"
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
      mesosConfig.EXECUTOR_URI.key -> url
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
      mesosConfig.ENABLE_FETCHER_CACHE.key -> "true",
      mesosConfig.EXECUTOR_URI.key -> url
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
      mesosConfig.ENABLE_FETCHER_CACHE.key -> "false",
      mesosConfig.EXECUTOR_URI.key -> url
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
      mesosConfig.TASK_LABELS.key -> taskLabelsString
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

  test("mesos supports spark.mesos.network.name and spark.mesos.network.labels") {
    setBackend(Map(
      mesosConfig.NETWORK_NAME.key -> "test-network-name",
      mesosConfig.NETWORK_LABELS.key -> "key1:val1,key2:val2"
    ))

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    val launchedTasks = verifyTaskLaunched(driver, "o1")
    val networkInfos = launchedTasks.head.getContainer.getNetworkInfosList
    assert(networkInfos.size == 1)
    assert(networkInfos.get(0).getName == "test-network-name")
    assert(networkInfos.get(0).getLabels.getLabels(0).getKey == "key1")
    assert(networkInfos.get(0).getLabels.getLabels(0).getValue == "val1")
    assert(networkInfos.get(0).getLabels.getLabels(1).getKey == "key2")
    assert(networkInfos.get(0).getLabels.getLabels(1).getValue == "val2")
  }

  test("SPARK-28778 '--hostname' shouldn't be set for executor when virtual network is enabled") {
    setBackend()
    val (mem, cpu) = (backend.executorMemory(sc), 4)
    val offer = createOffer("o1", "s1", mem, cpu)

    assert(backend.createCommand(offer, cpu, "test").getValue.contains("--hostname"))
    sc.stop()

    setBackend(Map("spark.executor.uri" -> "hdfs://test/executor.jar"))
    assert(backend.createCommand(offer, cpu, "test").getValue.contains("--hostname"))
    sc.stop()

    setBackend(Map("spark.mesos.network.name" -> "test"))
    assert(!backend.createCommand(offer, cpu, "test").getValue.contains("--hostname"))
    sc.stop()

    setBackend(Map(
      "spark.mesos.network.name" -> "test",
      "spark.executor.uri" -> "hdfs://test/executor.jar"
    ))
    assert(!backend.createCommand(offer, cpu, "test").getValue.contains("--hostname"))
    sc.stop()
  }

  test("supports spark.scheduler.minRegisteredResourcesRatio") {
    val expectedCores = 1
    setBackend(Map(
      CORES_MAX.key -> expectedCores.toString,
      SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO.key -> "1.0"))

    val offers = List(Resources(backend.executorMemory(sc), expectedCores))
    offerResources(offers)
    val launchedTasks = verifyTaskLaunched(driver, "o1")
    assert(!backend.isReady)

    registerMockExecutor(launchedTasks(0).getTaskId.getValue, "s1", expectedCores)
    assert(backend.isReady)
  }

  test("supports data locality with dynamic allocation") {
    setBackend(Map(
      DYN_ALLOCATION_ENABLED.key -> "true",
      DYN_ALLOCATION_TESTING.key -> "true",
      LOCALITY_WAIT.key -> "1s"))

    assert(backend.getExecutorIds().isEmpty)

    val defaultProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
    val defaultProf = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    backend.requestTotalExecutors(
      Map(defaultProfileId -> 2),
      Map(defaultProfileId -> 2),
      Map(defaultProfileId -> Map("hosts10" -> 1, "hosts11" -> 1)))

    // Offer non-local resources, which should be rejected
    offerResourcesAndVerify(1, false)
    offerResourcesAndVerify(2, false)

    // Offer local resource
    offerResourcesAndVerify(10, true)

    // Wait longer than spark.locality.wait
    Thread.sleep(2000)

    // Offer non-local resource, which should be accepted
    offerResourcesAndVerify(1, true)

    // Update total executors
    backend.requestTotalExecutors(
      Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID -> 3),
      Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID -> 2),
      Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID ->
        Map("hosts10" -> 1, "hosts11" -> 1, "hosts12" -> 1)))

    // Offer non-local resources, which should be rejected
    offerResourcesAndVerify(3, false)

    // Wait longer than spark.locality.wait
    Thread.sleep(2000)

    // Update total executors
    backend.requestTotalExecutors(
      Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID -> 4),
      Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID -> 4),
      Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID ->
            Map("hosts10" -> 1, "hosts11" -> 1, "hosts12" -> 1, "hosts13" -> 1)))

    // Offer non-local resources, which should be rejected
    offerResourcesAndVerify(3, false)

    // Offer local resource
    offerResourcesAndVerify(13, true)

    // Wait longer than spark.locality.wait
    Thread.sleep(2000)

    // Offer non-local resource, which should be accepted
    offerResourcesAndVerify(2, true)
  }

  test("Creates an env-based reference secrets.") {
    val launchedTasks = launchExecutorTasks(
      configEnvBasedRefSecrets(mesosConfig.executorSecretConfig))
    verifyEnvBasedRefSecrets(launchedTasks)
  }

  test("Creates an env-based value secrets.") {
    val launchedTasks = launchExecutorTasks(
      configEnvBasedValueSecrets(mesosConfig.executorSecretConfig))
    verifyEnvBasedValueSecrets(launchedTasks)
  }

  test("Creates file-based reference secrets.") {
    val launchedTasks = launchExecutorTasks(
      configFileBasedRefSecrets(mesosConfig.executorSecretConfig))
    verifyFileBasedRefSecrets(launchedTasks)
  }

  test("Creates a file-based value secrets.") {
    val launchedTasks = launchExecutorTasks(
      configFileBasedValueSecrets(mesosConfig.executorSecretConfig))
    verifyFileBasedValueSecrets(launchedTasks)
  }

  private def launchExecutorTasks(sparkConfVars: Map[String, String]): List[TaskInfo] = {
    setBackend(sparkConfVars)

    val (mem, cpu) = (backend.executorMemory(sc), 4)

    val offer1 = createOffer("o1", "s1", mem, cpu)
    backend.resourceOffers(driver, List(offer1).asJava)

    verifyTaskLaunched(driver, "o1")
  }

  private case class Resources(mem: Int, cpus: Int, gpus: Int = 0)

  private def registerMockExecutor(executorId: String, agentId: String, cores: Integer) = {
    val mockEndpointRef = mock[RpcEndpointRef]
    val mockAddress = mock[RpcAddress]
    val message = RegisterExecutor(executorId, mockEndpointRef, agentId, cores, Map.empty,
      Map.empty, Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)

    backend.driverEndpoint.askSync[Boolean](message)
  }

  private def verifyDeclinedOffer(driver: SchedulerDriver,
      offerId: OfferID,
      filter: Boolean = false): Unit = {
    if (filter) {
      verify(driver, times(1)).declineOffer(meq(offerId), any[Filters]())
    } else {
      verify(driver, times(1)).declineOffer(meq(offerId))
    }
  }

  private def offerResources(offers: List[Resources], startId: Int = 1): Unit = {
    val mesosOffers = offers.zipWithIndex.map {case (offer, i) =>
      createOffer(s"o${i + startId}", s"s${i + startId}", offer.mem, offer.cpus, None, offer.gpus)}

    backend.resourceOffers(driver, mesosOffers.asJava)
  }

  private def offerResourcesAndVerify(id: Int, expectAccept: Boolean): Unit = {
    offerResources(List(Resources(backend.executorMemory(sc), 1)), id)
    if (expectAccept) {
      val numExecutors = backend.getExecutorIds().size
      val launchedTasks = verifyTaskLaunched(driver, s"o$id")
      assert(s"s$id" == launchedTasks.head.getSlaveId.getValue)
      registerMockExecutor(launchedTasks.head.getTaskId.getValue, s"s$id", 1)
      assert(backend.getExecutorIds().size == numExecutors + 1)
    } else {
      verifyTaskNotLaunched(driver, s"o$id")
    }
  }

  private def createTaskStatus(taskId: String, agentId: String, state: TaskState): TaskStatus = {
    TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(taskId).build())
      .setSlaveId(SlaveID.newBuilder().setValue(agentId).build())
      .setState(state)
      .build
  }

  private def createSchedulerBackend(
      taskScheduler: TaskSchedulerImpl,
      driver: SchedulerDriver,
      shuffleClient: MesosExternalBlockStoreClient) = {
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

      override protected def getShuffleClient(): MesosExternalBlockStoreClient = shuffleClient

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
      .set(mesosConfig.DRIVER_WEBUI_URL, "http://webui")

    if (home != null) {
      sparkConf.setSparkHome(home)
    }

    if (sparkConfVars != null) {
      sparkConf.setAll(sparkConfVars)
    }
  }

  private def setBackend(sparkConfVars: Map[String, String] = null,
      home: String = "/path"): Unit = {
    initializeSparkConf(sparkConfVars, home)
    sc = new SparkContext(sparkConf)

    driver = mock[SchedulerDriver]
    when(driver.start()).thenReturn(Protos.Status.DRIVER_RUNNING)

    taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.excludedNodes).thenReturn(Set[String]())
    when(taskScheduler.sc).thenReturn(sc)

    externalShuffleClient = mock[MesosExternalBlockStoreClient]

    backend = createSchedulerBackend(taskScheduler, driver, externalShuffleClient)
  }
}
