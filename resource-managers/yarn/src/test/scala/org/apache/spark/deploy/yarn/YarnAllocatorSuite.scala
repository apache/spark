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

package org.apache.spark.deploy.yarn

import java.util
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.net.{Node, NodeBase}
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.yarn.ResourceRequestHelper._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.config._
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceProfile, TaskResourceRequests}
import org.apache.spark.resource.ResourceUtils.{AMOUNT, GPU}
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.DecommissionExecutorsOnHost
import org.apache.spark.util.ManualClock

class MockResolver extends SparkRackResolver(SparkHadoopUtil.get.conf) {

  override def resolve(hostName: String): String = {
    if (hostName == "host3") "/rack2" else "/rack1"
  }

  override def resolve(hostNames: Seq[String]): Seq[Node] =
    hostNames.map(n => new NodeBase(n, resolve(n)))

}

class YarnAllocatorSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach {
  val conf = new YarnConfiguration()
  val sparkConf = new SparkConf()
  sparkConf.set(DRIVER_HOST_ADDRESS, "localhost")
  sparkConf.set(DRIVER_PORT, 4040)
  sparkConf.set(SPARK_JARS, Seq("notarealjar.jar"))
  sparkConf.set("spark.yarn.launchContainers", "false")
  sparkConf.set(DECOMMISSION_ENABLED.key, "true")

  val appAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0)

  // Resource returned by YARN.  YARN can give larger containers than requested, so give 6 cores
  // instead of the 5 requested and 3 GB instead of the 2 requested.
  val containerResource = Resource.newInstance(3072, 6)

  var rmClient: AMRMClient[ContainerRequest] = _

  var clock: ManualClock = _

  var containerNum = 0

  // priority has to be 0 to match default profile id
  val RM_REQUEST_PRIORITY = Priority.newInstance(0)
  val defaultRPId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
  var defaultRP = ResourceProfile.getOrCreateDefaultProfile(sparkConf)

  var rpcEndPoint: RpcEndpointRef = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    rmClient = AMRMClient.createAMRMClient()
    rmClient.init(conf)
    rmClient.start()
    clock = new ManualClock()
  }

  override def afterEach(): Unit = {
    try {
      rmClient.stop()
    } finally {
      super.afterEach()
    }
  }

  class MockSplitInfo(host: String) extends SplitInfo(null, host, null, 1, null) {
    override def hashCode(): Int = 0
    override def equals(other: Any): Boolean = false
  }

  def createAllocator(
      maxExecutors: Int = 5,
      rmClient: AMRMClient[ContainerRequest] = rmClient,
      additionalConfigs: Map[String, String] = Map()): (YarnAllocator, SparkConf) = {
    val args = Array(
      "--jar", "somejar.jar",
      "--class", "SomeClass")
    val sparkConfClone = sparkConf.clone()
    sparkConfClone
      .set(EXECUTOR_INSTANCES, maxExecutors)
      .set(EXECUTOR_CORES, 5)
      .set(EXECUTOR_MEMORY, 2048L)

    for ((name, value) <- additionalConfigs) {
      sparkConfClone.set(name, value)
    }
    // different spark confs means we need to reinit the default profile
    ResourceProfile.clearDefaultProfile()
    defaultRP = ResourceProfile.getOrCreateDefaultProfile(sparkConfClone)

    rpcEndPoint = mock(classOf[RpcEndpointRef])
    val allocator = new YarnAllocator(
      "not used",
      rpcEndPoint,
      conf,
      sparkConfClone,
      rmClient,
      appAttemptId,
      new SecurityManager(sparkConf),
      Map(),
      new MockResolver(),
      clock)
    (allocator, sparkConfClone)
  }

  def createContainer(
      host: String,
      containerNumber: Int = containerNum,
      resource: Resource = containerResource,
      priority: Priority = RM_REQUEST_PRIORITY): Container = {
    val  containerId: ContainerId = ContainerId.newContainerId(appAttemptId, containerNum)
    containerNum += 1
    val nodeId = NodeId.newInstance(host, 1000)
    Container.newInstance(containerId, nodeId, "", resource, priority, null)
  }

  def createContainers(hosts: Seq[String], containerIds: Seq[Int]): Seq[Container] = {
    hosts.zip(containerIds).map{case (host, id) => createContainer(host, id)}
  }

  def createContainerStatus(
      containerId: ContainerId,
      exitStatus: Int,
      containerState: ContainerState = ContainerState.COMPLETE,
      diagnostics: String = "diagnostics"): ContainerStatus = {
    ContainerStatus.newInstance(containerId, containerState, diagnostics, exitStatus)
  }


  test("single container allocated") {
    // request a single container and receive it
    val (handler, _) = createAllocator(1)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (1)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    val hostTocontainer = handler.allocatedHostToContainersMapPerRPId(defaultRPId)
    hostTocontainer.get("host1").get should contain(container.getId)

    val size = rmClient.getMatchingRequests(container.getPriority, "host1", containerResource).size
    size should be (0)
  }

  test("single container allocated with ResourceProfile") {
    assume(isYarnResourceTypesAvailable())
    val yarnResources = Seq(sparkConf.get(YARN_GPU_DEVICE))
    ResourceRequestTestHelper.initializeResourceTypes(yarnResources)
    // create default profile so we get a different id to test below
    val defaultRProf = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    val execReq = new ExecutorResourceRequests().resource("gpu", 6)
    val taskReq = new TaskResourceRequests().resource("gpu", 1)
    val rprof = new ResourceProfile(execReq.requests, taskReq.requests)
    // request a single container and receive it
    val (handler, _) = createAllocator(0)

    val resourceProfileToTotalExecs = mutable.HashMap(defaultRProf -> 0, rprof -> 1)
    val numLocalityAwareTasksPerResourceProfileId = mutable.HashMap(rprof.id -> 0)
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map.empty, Set.empty)

    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (1)

    val container = createContainer("host1", priority = Priority.newInstance(rprof.id))
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    val hostTocontainer = handler.allocatedHostToContainersMapPerRPId(rprof.id)
    hostTocontainer.get("host1").get should contain(container.getId)

    val size = rmClient.getMatchingRequests(container.getPriority, "host1", containerResource).size
    size should be (0)

    ResourceProfile.reInitDefaultProfile(sparkConf)
  }

  test("multiple containers allocated with ResourceProfiles") {
    assume(isYarnResourceTypesAvailable())
    val yarnResources = Seq(sparkConf.get(YARN_GPU_DEVICE), sparkConf.get(YARN_FPGA_DEVICE))
    ResourceRequestTestHelper.initializeResourceTypes(yarnResources)
    // create default profile so we get a different id to test below
    val defaultRProf = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    val execReq = new ExecutorResourceRequests().resource("gpu", 6)
    val taskReq = new TaskResourceRequests().resource("gpu", 1)
    val rprof = new ResourceProfile(execReq.requests, taskReq.requests)

    val execReq2 = new ExecutorResourceRequests().memory("8g").resource("fpga", 2)
    val taskReq2 = new TaskResourceRequests().resource("fpga", 1)
    val rprof2 = new ResourceProfile(execReq2.requests, taskReq2.requests)


    // request a single container and receive it
    val (handler, _) = createAllocator(1)
    val resourceProfileToTotalExecs = mutable.HashMap(defaultRProf -> 0, rprof -> 1, rprof2 -> 2)
    val numLocalityAwareTasksPerResourceProfileId = mutable.HashMap(rprof.id -> 0, rprof2.id -> 0)
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map.empty, Set.empty)

    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (3)

    val containerResourcerp2 = Resource.newInstance(10240, 5)

    val container = createContainer("host1", priority = Priority.newInstance(rprof.id))
    val container2 = createContainer("host2", resource = containerResourcerp2,
      priority = Priority.newInstance(rprof2.id))
    val container3 = createContainer("host3", resource = containerResourcerp2,
      priority = Priority.newInstance(rprof2.id))
    handler.handleAllocatedContainers(Array(container, container2, container3))

    handler.getNumExecutorsRunning should be (3)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host2")
    handler.allocatedContainerToHostMap.get(container3.getId).get should be ("host3")

    val hostTocontainer = handler.allocatedHostToContainersMapPerRPId(rprof.id)
    hostTocontainer.get("host1").get should contain(container.getId)
    val hostTocontainer2 = handler.allocatedHostToContainersMapPerRPId(rprof2.id)
    hostTocontainer2.get("host2").get should contain(container2.getId)
    hostTocontainer2.get("host3").get should contain(container3.getId)

    val size = rmClient.getMatchingRequests(container.getPriority, "host1", containerResource).size
    size should be (0)

    ResourceProfile.reInitDefaultProfile(sparkConf)
  }

  test("custom resource requested from yarn") {
    assume(isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(List("gpu"))

    val mockAmClient = mock(classOf[AMRMClient[ContainerRequest]])
    val (handler, _) = createAllocator(1, mockAmClient,
      Map(s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}${GPU}.${AMOUNT}" -> "2G"))

    handler.updateResourceRequests()
    val defaultResource = handler.rpIdToYarnResource.get(defaultRPId)
    val container = createContainer("host1", resource = defaultResource)
    handler.handleAllocatedContainers(Array(container))

    // get amount of memory and vcores from resource, so effectively skipping their validation
    val expectedResources = Resource.newInstance(defaultResource.getMemory(),
      defaultResource.getVirtualCores)
    setResourceRequests(Map("gpu" -> "2G"), expectedResources)
    val captor = ArgumentCaptor.forClass(classOf[ContainerRequest])

    verify(mockAmClient).addContainerRequest(captor.capture())
    val containerRequest: ContainerRequest = captor.getValue
    assert(containerRequest.getCapability === expectedResources)
  }

  test("custom spark resource mapped to yarn resource configs") {
    assume(isYarnResourceTypesAvailable())
    val yarnMadeupResource = "yarn.io/madeup"
    val yarnResources = Seq(sparkConf.get(YARN_GPU_DEVICE), sparkConf.get(YARN_FPGA_DEVICE),
      yarnMadeupResource)
    ResourceRequestTestHelper.initializeResourceTypes(yarnResources)
    val mockAmClient = mock(classOf[AMRMClient[ContainerRequest]])
    val madeupConfigName = s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}${yarnMadeupResource}.${AMOUNT}"
    val sparkResources =
      Map(EXECUTOR_GPU_ID.amountConf -> "3",
        EXECUTOR_FPGA_ID.amountConf -> "2",
        madeupConfigName -> "5")
    val (handler, _) = createAllocator(1, mockAmClient, sparkResources)

    handler.updateResourceRequests()
    val defaultResource = handler.rpIdToYarnResource.get(defaultRPId)
    val yarnRInfo = ResourceRequestTestHelper.getResources(defaultResource)
    val allResourceInfo = yarnRInfo.map( rInfo => (rInfo.name -> rInfo.value) ).toMap
    assert(allResourceInfo.get(sparkConf.get(YARN_GPU_DEVICE)).nonEmpty)
    assert(allResourceInfo.get(sparkConf.get(YARN_GPU_DEVICE)).get === 3)
    assert(allResourceInfo.get(sparkConf.get(YARN_FPGA_DEVICE)).nonEmpty)
    assert(allResourceInfo.get(sparkConf.get(YARN_FPGA_DEVICE)).get === 2)
    assert(allResourceInfo.get(yarnMadeupResource).nonEmpty)
    assert(allResourceInfo.get(yarnMadeupResource).get === 5)
  }

  test("gpu/fpga spark resource mapped to custom yarn resource") {
    assume(isYarnResourceTypesAvailable())
    val gpuCustomName = "custom/gpu"
    val fpgaCustomName = "custom/fpga"
    val originalGpu = sparkConf.get(YARN_GPU_DEVICE)
    val originalFpga = sparkConf.get(YARN_FPGA_DEVICE)
    try {
      sparkConf.set(YARN_GPU_DEVICE.key, gpuCustomName)
      sparkConf.set(YARN_FPGA_DEVICE.key, fpgaCustomName)
      val yarnResources = Seq(gpuCustomName, fpgaCustomName)
      ResourceRequestTestHelper.initializeResourceTypes(yarnResources)
      val mockAmClient = mock(classOf[AMRMClient[ContainerRequest]])
      val sparkResources =
        Map(EXECUTOR_GPU_ID.amountConf -> "3",
          EXECUTOR_FPGA_ID.amountConf -> "2")
      val (handler, _) = createAllocator(1, mockAmClient, sparkResources)

      handler.updateResourceRequests()
      val defaultResource = handler.rpIdToYarnResource.get(defaultRPId)
      val yarnRInfo = ResourceRequestTestHelper.getResources(defaultResource)
      val allResourceInfo = yarnRInfo.map(rInfo => (rInfo.name -> rInfo.value)).toMap
      assert(allResourceInfo.get(gpuCustomName).nonEmpty)
      assert(allResourceInfo.get(gpuCustomName).get === 3)
      assert(allResourceInfo.get(fpgaCustomName).nonEmpty)
      assert(allResourceInfo.get(fpgaCustomName).get === 2)
    } finally {
      sparkConf.set(YARN_GPU_DEVICE.key, originalGpu)
      sparkConf.set(YARN_FPGA_DEVICE.key, originalFpga)
    }
  }

  test("container should not be created if requested number if met") {
    // request a single container and receive it
    val (handler, _) = createAllocator(1)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (1)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    val hostTocontainer = handler.allocatedHostToContainersMapPerRPId(defaultRPId)
    hostTocontainer.get("host1").get should contain(container.getId)

    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container2))
    handler.getNumExecutorsRunning should be (1)
  }

  test("some containers allocated") {
    // request a few containers and receive some of them
    val (handler, _) = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host1")
    val container3 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2, container3))

    handler.getNumExecutorsRunning should be (3)
    handler.allocatedContainerToHostMap.get(container1.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container3.getId).get should be ("host2")
    val hostTocontainer = handler.allocatedHostToContainersMapPerRPId(defaultRPId)
    hostTocontainer.get("host1").get should contain(container1.getId)
    hostTocontainer.get("host1").get should contain (container2.getId)
    hostTocontainer.get("host2").get should contain (container3.getId)
  }

  test("receive more containers than requested") {
    val (handler, _) = createAllocator(2)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (2)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    val container3 = createContainer("host4")
    handler.handleAllocatedContainers(Array(container1, container2, container3))

    handler.getNumExecutorsRunning should be (2)
    handler.allocatedContainerToHostMap.get(container1.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host2")
    handler.allocatedContainerToHostMap.contains(container3.getId) should be (false)
    val hostTocontainer = handler.allocatedHostToContainersMapPerRPId(defaultRPId)
    hostTocontainer.get("host1").get should contain(container1.getId)
    hostTocontainer.get("host2").get should contain (container2.getId)
    hostTocontainer.contains("host4") should be (false)
  }

  test("decrease total requested executors") {
    val (handler, _) = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (4)

    val resourceProfileToTotalExecs = mutable.HashMap(defaultRP -> 3)
    val numLocalityAwareTasksPerResourceProfileId = mutable.HashMap(defaultRPId -> 0)
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getNumContainersPendingAllocate should be (3)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    val hostTocontainer = handler.allocatedHostToContainersMapPerRPId(defaultRPId)
    hostTocontainer.get("host1").get should contain(container.getId)

    resourceProfileToTotalExecs(defaultRP) = 2
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getNumContainersPendingAllocate should be (1)
  }

  test("decrease total requested executors to less than currently running") {
    val (handler, _) = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (4)

    val resourceProfileToTotalExecs = mutable.HashMap(defaultRP -> 3)
    val numLocalityAwareTasksPerResourceProfileId = mutable.HashMap(defaultRPId -> 0)
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getNumContainersPendingAllocate should be (3)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    handler.getNumExecutorsRunning should be (2)

    resourceProfileToTotalExecs(defaultRP) = 1
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getNumContainersPendingAllocate should be (0)
    handler.getNumExecutorsRunning should be (2)
  }

  test("kill executors") {
    val (handler, _) = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    val resourceProfileToTotalExecs = mutable.HashMap(defaultRP -> 1)
    val numLocalityAwareTasksPerResourceProfileId = mutable.HashMap(defaultRPId -> 0)
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map.empty, Set.empty)
    handler.executorIdToContainer.keys.foreach { id => handler.killExecutor(id ) }

    val statuses = Seq(container1, container2).map { c =>
      ContainerStatus.newInstance(c.getId(), ContainerState.COMPLETE, "Finished", 0)
    }
    handler.updateResourceRequests()
    handler.processCompletedContainers(statuses)
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (1)
  }

  test("kill same executor multiple times") {
    val (handler, _) = createAllocator(2)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (2)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))
    handler.getNumExecutorsRunning should be (2)
    handler.getNumContainersPendingAllocate should be (0)

    val executorToKill = handler.executorIdToContainer.keys.head
    handler.killExecutor(executorToKill)
    handler.getNumExecutorsRunning should be (1)
    handler.killExecutor(executorToKill)
    handler.killExecutor(executorToKill)
    handler.killExecutor(executorToKill)
    handler.getNumExecutorsRunning should be (1)
    val resourceProfileToTotalExecs = mutable.HashMap(defaultRP -> 2)
    val numLocalityAwareTasksPerResourceProfileId = mutable.HashMap(defaultRPId -> 0)
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getNumContainersPendingAllocate should be (1)
  }

  test("process same completed container multiple times") {
    val (handler, _) = createAllocator(2)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (2)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))
    handler.getNumExecutorsRunning should be (2)
    handler.getNumContainersPendingAllocate should be (0)

    val statuses = Seq(container1, container1, container2).map { c =>
      ContainerStatus.newInstance(c.getId(), ContainerState.COMPLETE, "Finished", 0)
    }
    handler.processCompletedContainers(statuses)
    handler.getNumExecutorsRunning should be (0)

  }

  test("lost executor removed from backend") {
    val (handler, _) = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    val resourceProfileToTotalExecs = mutable.HashMap(defaultRP -> 2)
    val numLocalityAwareTasksPerResourceProfileId = mutable.HashMap(defaultRPId -> 0)
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map(), Set.empty)

    val statuses = Seq(container1, container2).map { c =>
      ContainerStatus.newInstance(c.getId(), ContainerState.COMPLETE, "Failed", -1)
    }
    handler.updateResourceRequests()
    handler.processCompletedContainers(statuses)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (2)
    handler.getNumExecutorsFailed should be (2)
    handler.getNumUnexpectedContainerRelease should be (2)
  }

  test("excluded nodes reflected in amClient requests") {
    // Internally we track the set of excluded nodes, but yarn wants us to send *changes*
    // to it. Note the YARN api uses the term blacklist for excluded nodes.
    // This makes sure we are sending the right updates.
    val mockAmClient = mock(classOf[AMRMClient[ContainerRequest]])
    val (handler, _) = createAllocator(4, mockAmClient)
    val resourceProfileToTotalExecs = mutable.HashMap(defaultRP -> 1)
    val numLocalityAwareTasksPerResourceProfileId = mutable.HashMap(defaultRPId -> 0)
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map(), Set("hostA"))
    verify(mockAmClient).updateBlacklist(Seq("hostA").asJava, Seq[String]().asJava)

    val excludedNodes = Set(
      "hostA",
      "hostB"
    )

    resourceProfileToTotalExecs(defaultRP) = 2
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map(), excludedNodes)
    verify(mockAmClient).updateBlacklist(Seq("hostB").asJava, Seq[String]().asJava)
    resourceProfileToTotalExecs(defaultRP) = 3
    handler.requestTotalExecutorsWithPreferredLocalities(resourceProfileToTotalExecs.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap, Map(), Set.empty)
    verify(mockAmClient).updateBlacklist(Seq[String]().asJava, Seq("hostA", "hostB").asJava)
  }

  test("window based failure executor counting") {
    sparkConf.set(EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS, 100 * 1000L)
    val (handler, _) = createAllocator(4)

    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getNumContainersPendingAllocate should be (4)

    val containers = Seq(
      createContainer("host1"),
      createContainer("host2"),
      createContainer("host3"),
      createContainer("host4")
    )
    handler.handleAllocatedContainers(containers)

    val failedStatuses = containers.map { c =>
      ContainerStatus.newInstance(c.getId, ContainerState.COMPLETE, "Failed", -1)
    }

    handler.getNumExecutorsFailed should be (0)

    clock.advance(100 * 1000L)
    handler.processCompletedContainers(failedStatuses.slice(0, 1))
    handler.getNumExecutorsFailed should be (1)

    clock.advance(101 * 1000L)
    handler.getNumExecutorsFailed should be (0)

    handler.processCompletedContainers(failedStatuses.slice(1, 3))
    handler.getNumExecutorsFailed should be (2)

    clock.advance(50 * 1000L)
    handler.processCompletedContainers(failedStatuses.slice(3, 4))
    handler.getNumExecutorsFailed should be (3)

    clock.advance(51 * 1000L)
    handler.getNumExecutorsFailed should be (1)

    clock.advance(50 * 1000L)
    handler.getNumExecutorsFailed should be (0)
  }

  test("SPARK-26269: YarnAllocator should have same excludeOnFailure behaviour with YARN") {
    val rmClientSpy = spy(rmClient)
    val maxExecutors = 11

    val (handler, _) = createAllocator(
      maxExecutors,
      rmClientSpy,
      Map(
        YARN_EXECUTOR_LAUNCH_EXCLUDE_ON_FAILURE_ENABLED.key -> "true",
        MAX_FAILED_EXEC_PER_NODE.key -> "0"))
    handler.updateResourceRequests()

    val hosts = (0 until maxExecutors).map(i => s"host$i")
    val ids = 0 to maxExecutors
    val containers = createContainers(hosts, ids)

    val nonExcludedStatuses = Seq(
      ContainerExitStatus.SUCCESS,
      ContainerExitStatus.PREEMPTED,
      ContainerExitStatus.KILLED_EXCEEDED_VMEM,
      ContainerExitStatus.KILLED_EXCEEDED_PMEM,
      ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
      ContainerExitStatus.KILLED_BY_APPMASTER,
      ContainerExitStatus.KILLED_AFTER_APP_COMPLETION,
      ContainerExitStatus.ABORTED,
      ContainerExitStatus.DISKS_FAILED)

    val nonExcludedContainerStatuses = nonExcludedStatuses.zipWithIndex.map {
      case (exitStatus, idx) => createContainerStatus(containers(idx).getId, exitStatus)
    }

    val EXCLUDED_EXIT_CODE = 1
    val excludedStatuses = Seq(ContainerExitStatus.INVALID, EXCLUDED_EXIT_CODE)

    val excludedContainerStatuses = excludedStatuses.zip(9 until maxExecutors).map {
      case (exitStatus, idx) => createContainerStatus(containers(idx).getId, exitStatus)
    }

    handler.handleAllocatedContainers(containers.slice(0, 9))
    handler.processCompletedContainers(nonExcludedContainerStatuses)
    verify(rmClientSpy, never())
      .updateBlacklist(hosts.slice(0, 9).asJava, Collections.emptyList())

    handler.handleAllocatedContainers(containers.slice(9, 11))
    handler.processCompletedContainers(excludedContainerStatuses)
    verify(rmClientSpy)
      .updateBlacklist(hosts.slice(9, 10).asJava, Collections.emptyList())
    verify(rmClientSpy)
      .updateBlacklist(hosts.slice(10, 11).asJava, Collections.emptyList())
  }

  test("SPARK-28577#YarnAllocator.resource.memory should include offHeapSize " +
    "when offHeapEnabled is true.") {
    val originalOffHeapEnabled = sparkConf.get(MEMORY_OFFHEAP_ENABLED)
    val originalOffHeapSize = sparkConf.get(MEMORY_OFFHEAP_SIZE)
    val executorMemory = sparkConf.get(EXECUTOR_MEMORY).toInt
    val offHeapMemoryInMB = 1024L
    val offHeapMemoryInByte = offHeapMemoryInMB * 1024 * 1024
    try {
      sparkConf.set(MEMORY_OFFHEAP_ENABLED, true)
      sparkConf.set(MEMORY_OFFHEAP_SIZE, offHeapMemoryInByte)
      val (handler, _) = createAllocator(maxExecutors = 1,
        additionalConfigs = Map(EXECUTOR_MEMORY.key -> executorMemory.toString))
      val defaultResource = handler.rpIdToYarnResource.get(defaultRPId)
      val memory = defaultResource.getMemory
      assert(memory ==
        executorMemory + offHeapMemoryInMB + ResourceProfile.MEMORY_OVERHEAD_MIN_MIB)
    } finally {
      sparkConf.set(MEMORY_OFFHEAP_ENABLED, originalOffHeapEnabled)
      sparkConf.set(MEMORY_OFFHEAP_SIZE, originalOffHeapSize)
    }
  }

  test("SPARK-38194: Configurable memory overhead factor") {
    val executorMemory = sparkConf.get(EXECUTOR_MEMORY).toLong
    try {
      sparkConf.set(EXECUTOR_MEMORY_OVERHEAD_FACTOR, 0.5)
      val (handler, _) = createAllocator(maxExecutors = 1,
        additionalConfigs = Map(EXECUTOR_MEMORY.key -> executorMemory.toString))
      val defaultResource = handler.rpIdToYarnResource.get(defaultRPId)
      val memory = defaultResource.getMemory
      assert(memory == (executorMemory * 1.5).toLong)
    } finally {
      sparkConf.set(EXECUTOR_MEMORY_OVERHEAD_FACTOR, 0.1)
    }
  }

  test("SPARK-38194: Memory overhead takes precedence over factor") {
    val executorMemory = sparkConf.get(EXECUTOR_MEMORY)
    try {
      sparkConf.set(EXECUTOR_MEMORY_OVERHEAD_FACTOR, 0.5)
      sparkConf.set(EXECUTOR_MEMORY_OVERHEAD, (executorMemory * 0.4).toLong)
      val (handler, _) = createAllocator(maxExecutors = 1,
        additionalConfigs = Map(EXECUTOR_MEMORY.key -> executorMemory.toString))
      val defaultResource = handler.rpIdToYarnResource.get(defaultRPId)
      val memory = defaultResource.getMemory
      assert(memory == (executorMemory * 1.4).toLong)
    } finally {
      sparkConf.set(EXECUTOR_MEMORY_OVERHEAD_FACTOR, 0.1)
    }
  }

  test("Test YARN container decommissioning") {
    val rmClient: AMRMClient[ContainerRequest] = AMRMClient.createAMRMClient()
    val rmClientSpy = spy(rmClient)
    val allocateResponse = mock(classOf[AllocateResponse])
    val (handler, sparkConfClone) = createAllocator(3, rmClientSpy)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    val container3 = createContainer("host3")
    val containerList =
      new util.ArrayList[Container](Seq(container1, container2, container3).asJava)

    // Return 3 containers allocated by YARN for the first heart beat
    when(allocateResponse.getAllocatedContainers).thenReturn(containerList)

    // No nodes are in DECOMMISSIONING state in the first heart beat so return empty list
    when(allocateResponse.getUpdatedNodes).thenReturn(new util.ArrayList[NodeReport]())
    // when().thenReturn doesn't work on spied class. We will use doAnswer for this.
    val allocateResponseAnswer = new Answer[AnyRef]() {
      @throws[Throwable]
      override def answer(invocationOnMock: InvocationOnMock): AllocateResponse = {
        allocateResponse
      }
    }
    doAnswer(allocateResponseAnswer).when(rmClientSpy)
      .allocate(org.mockito.ArgumentMatchers.anyFloat())

    handler.allocateResources()
    // No DecommissionExecutor message should be sent
    verify(rpcEndPoint, times(0)).
      send(DecommissionExecutorsOnHost(org.mockito.ArgumentMatchers.any()))

    handler.getNumExecutorsRunning should be (3)
    handler.allocatedContainerToHostMap(container1.getId) should be ("host1")
    handler.allocatedContainerToHostMap(container2.getId) should be ("host2")
    handler.allocatedContainerToHostMap(container3.getId) should be ("host3")
    val allocatedHostToContainersMap = handler.allocatedHostToContainersMapPerRPId(defaultRPId)
    allocatedHostToContainersMap("host1") should contain (container1.getId)
    allocatedHostToContainersMap("host2") should contain (container2.getId)
    allocatedHostToContainersMap("host3") should contain (container3.getId)

    // No new containers in this heartbeat
    when(allocateResponse.getAllocatedContainers).thenReturn(new util.ArrayList[Container]())
    val nodeReport = mock(classOf[NodeReport])
    val nodeId = mock(classOf[NodeId])
    val nodeReportList = new util.ArrayList[NodeReport](Seq(nodeReport).asJava)

    // host1 is now in DECOMMISSIONING state
    val httpAddress1 = "host1:420"
    when(nodeReport.getNodeState).thenReturn(NodeState.DECOMMISSIONING)
    when(nodeReport.getNodeId).thenReturn(nodeId)
    when(nodeId.getHost).thenReturn("host1")
    when(allocateResponse.getUpdatedNodes).thenReturn(nodeReportList)

    handler.allocateResources()
    verify(rpcEndPoint, times(1)).
      send(DecommissionExecutorsOnHost(org.mockito.ArgumentMatchers.any()))

    // Test with config disabled
    sparkConf.remove(DECOMMISSION_ENABLED.key)

    // host2 is now in DECOMMISSIONING state
    val httpAddress2 = "host2:420"
    when(nodeReport.getNodeId).thenReturn(nodeId)
    when(nodeId.getHost).thenReturn("host2")
    when(nodeReport.getNodeId).thenReturn(nodeId)

    // No DecommissionExecutor message should be sent when config is set to false
    verify(rpcEndPoint, times(1)).
      send(DecommissionExecutorsOnHost(org.mockito.ArgumentMatchers.any()))
  }
}
