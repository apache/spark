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

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.util.ManualClock

class MockResolver extends SparkRackResolver {

  override def resolve(conf: Configuration, hostName: String): String = {
    if (hostName == "host3") "/rack2" else "/rack1"
  }

}

class YarnAllocatorSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach {
  val conf = new YarnConfiguration()
  val sparkConf = new SparkConf()
  sparkConf.set("spark.driver.host", "localhost")
  sparkConf.set("spark.driver.port", "4040")
  sparkConf.set(SPARK_JARS, Seq("notarealjar.jar"))
  sparkConf.set("spark.yarn.launchContainers", "false")

  val appAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0)

  // Resource returned by YARN.  YARN can give larger containers than requested, so give 6 cores
  // instead of the 5 requested and 3 GB instead of the 2 requested.
  val containerResource = Resource.newInstance(3072, 6)

  var rmClient: AMRMClient[ContainerRequest] = _

  var clock: ManualClock = _

  var containerNum = 0

  override def beforeEach() {
    super.beforeEach()
    rmClient = AMRMClient.createAMRMClient()
    rmClient.init(conf)
    rmClient.start()
    clock = new ManualClock()
  }

  override def afterEach() {
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
      additionalConfigs: Map[String, String] = Map()): YarnAllocator = {
    val args = Array(
      "--jar", "somejar.jar",
      "--class", "SomeClass")
    val sparkConfClone = sparkConf.clone()
    sparkConfClone
      .set("spark.executor.instances", maxExecutors.toString)
      .set("spark.executor.cores", "5")
      .set("spark.executor.memory", "2048")

    for ((name, value) <- additionalConfigs) {
      sparkConfClone.set(name, value)
    }

    new YarnAllocator(
      "not used",
      mock(classOf[RpcEndpointRef]),
      conf,
      sparkConfClone,
      rmClient,
      appAttemptId,
      new SecurityManager(sparkConf),
      Map(),
      new MockResolver(),
      clock)
  }

  def createContainer(
      host: String,
      containerId: ContainerId = ContainerId.newContainerId(appAttemptId, containerNum),
      resource: Resource = containerResource): Container = {
    containerNum += 1
    val nodeId = NodeId.newInstance(host, 1000)
    Container.newInstance(containerId, nodeId, "", resource, RM_REQUEST_PRIORITY, null)
  }

  def createContainers(hosts: Seq[String], containerIds: Seq[ContainerId]): Seq[Container] = {
    hosts.zip(containerIds).map{case (host, id) => createContainer(host, id)}
  }


  test("single container allocated") {
    // request a single container and receive it
    val handler = createAllocator(1)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (1)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container.getId)

    val size = rmClient.getMatchingRequests(container.getPriority, "host1", containerResource).size
    size should be (0)
  }

  test("custom resource requested from yarn") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    ResourceRequestTestHelper.initializeResourceTypes(List("gpu"))

    val mockAmClient = mock(classOf[AMRMClient[ContainerRequest]])
    val handler = createAllocator(1, mockAmClient,
      Map(YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "gpu" -> "2G"))

    handler.updateResourceRequests()
    val container = createContainer("host1", resource = handler.resource)
    handler.handleAllocatedContainers(Array(container))

    // get amount of memory and vcores from resource, so effectively skipping their validation
    val expectedResources = Resource.newInstance(handler.resource.getMemory(),
      handler.resource.getVirtualCores)
    ResourceRequestHelper.setResourceRequests(Map("gpu" -> "2G"), expectedResources)
    val captor = ArgumentCaptor.forClass(classOf[ContainerRequest])

    verify(mockAmClient).addContainerRequest(captor.capture())
    val containerRequest: ContainerRequest = captor.getValue
    assert(containerRequest.getCapability === expectedResources)
  }

  test("container should not be created if requested number if met") {
    // request a single container and receive it
    val handler = createAllocator(1)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (1)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container.getId)

    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container2))
    handler.getNumExecutorsRunning should be (1)
  }

  test("some containers allocated") {
    // request a few containers and receive some of them
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host1")
    val container3 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2, container3))

    handler.getNumExecutorsRunning should be (3)
    handler.allocatedContainerToHostMap.get(container1.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container3.getId).get should be ("host2")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container1.getId)
    handler.allocatedHostToContainersMap.get("host1").get should contain (container2.getId)
    handler.allocatedHostToContainersMap.get("host2").get should contain (container3.getId)
  }

  test("receive more containers than requested") {
    val handler = createAllocator(2)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (2)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    val container3 = createContainer("host4")
    handler.handleAllocatedContainers(Array(container1, container2, container3))

    handler.getNumExecutorsRunning should be (2)
    handler.allocatedContainerToHostMap.get(container1.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host2")
    handler.allocatedContainerToHostMap.contains(container3.getId) should be (false)
    handler.allocatedHostToContainersMap.get("host1").get should contain (container1.getId)
    handler.allocatedHostToContainersMap.get("host2").get should contain (container2.getId)
    handler.allocatedHostToContainersMap.contains("host4") should be (false)
  }

  test("decrease total requested executors") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    handler.requestTotalExecutorsWithPreferredLocalities(3, 0, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getPendingAllocate.size should be (3)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container.getId)

    handler.requestTotalExecutorsWithPreferredLocalities(2, 0, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getPendingAllocate.size should be (1)
  }

  test("decrease total requested executors to less than currently running") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    handler.requestTotalExecutorsWithPreferredLocalities(3, 0, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getPendingAllocate.size should be (3)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    handler.getNumExecutorsRunning should be (2)

    handler.requestTotalExecutorsWithPreferredLocalities(1, 0, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getPendingAllocate.size should be (0)
    handler.getNumExecutorsRunning should be (2)
  }

  test("kill executors") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    handler.requestTotalExecutorsWithPreferredLocalities(1, 0, Map.empty, Set.empty)
    handler.executorIdToContainer.keys.foreach { id => handler.killExecutor(id ) }

    val statuses = Seq(container1, container2).map { c =>
      ContainerStatus.newInstance(c.getId(), ContainerState.COMPLETE, "Finished", 0)
    }
    handler.updateResourceRequests()
    handler.processCompletedContainers(statuses)
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (1)
  }

  test("kill same executor multiple times") {
    val handler = createAllocator(2)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (2)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))
    handler.getNumExecutorsRunning should be (2)
    handler.getPendingAllocate.size should be (0)

    val executorToKill = handler.executorIdToContainer.keys.head
    handler.killExecutor(executorToKill)
    handler.getNumExecutorsRunning should be (1)
    handler.killExecutor(executorToKill)
    handler.killExecutor(executorToKill)
    handler.killExecutor(executorToKill)
    handler.getNumExecutorsRunning should be (1)
    handler.requestTotalExecutorsWithPreferredLocalities(2, 0, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getPendingAllocate.size should be (1)
  }

  test("process same completed container multiple times") {
    val handler = createAllocator(2)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (2)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))
    handler.getNumExecutorsRunning should be (2)
    handler.getPendingAllocate.size should be (0)

    val statuses = Seq(container1, container1, container2).map { c =>
      ContainerStatus.newInstance(c.getId(), ContainerState.COMPLETE, "Finished", 0)
    }
    handler.processCompletedContainers(statuses)
    handler.getNumExecutorsRunning should be (0)

  }

  test("lost executor removed from backend") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    handler.requestTotalExecutorsWithPreferredLocalities(2, 0, Map(), Set.empty)

    val statuses = Seq(container1, container2).map { c =>
      ContainerStatus.newInstance(c.getId(), ContainerState.COMPLETE, "Failed", -1)
    }
    handler.updateResourceRequests()
    handler.processCompletedContainers(statuses)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (2)
    handler.getNumExecutorsFailed should be (2)
    handler.getNumUnexpectedContainerRelease should be (2)
  }

  test("blacklisted nodes reflected in amClient requests") {
    // Internally we track the set of blacklisted nodes, but yarn wants us to send *changes*
    // to the blacklist.  This makes sure we are sending the right updates.
    val mockAmClient = mock(classOf[AMRMClient[ContainerRequest]])
    val handler = createAllocator(4, mockAmClient)
    handler.requestTotalExecutorsWithPreferredLocalities(1, 0, Map(), Set("hostA"))
    verify(mockAmClient).updateBlacklist(Seq("hostA").asJava, Seq[String]().asJava)

    val blacklistedNodes = Set(
      "hostA",
      "hostB"
    )
    handler.requestTotalExecutorsWithPreferredLocalities(2, 0, Map(), blacklistedNodes)
    verify(mockAmClient).updateBlacklist(Seq("hostB").asJava, Seq[String]().asJava)

    handler.requestTotalExecutorsWithPreferredLocalities(3, 0, Map(), Set.empty)
    verify(mockAmClient).updateBlacklist(Seq[String]().asJava, Seq("hostA", "hostB").asJava)
  }

  test("window based failure executor counting") {
    sparkConf.set("spark.yarn.executor.failuresValidityInterval", "100s")
    val handler = createAllocator(4)

    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

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

  test("SPARK-26296: YarnAllocator should have same blacklist behaviour with YARN") {
    val rmClientSpy = spy(rmClient)
    val maxExecutors = 11

    val handler = createAllocator(
      maxExecutors,
      rmClientSpy,
      Map(
        "spark.yarn.blacklist.executor.launch.blacklisting.enabled" -> "true",
        "spark.blacklist.application.maxFailedExecutorsPerNode" -> "0"))
    handler.updateResourceRequests()

    val hosts = (0 until maxExecutors).map(i => s"host$i")
    val ids = (0 to maxExecutors).map(i => ContainerId.newContainerId(appAttemptId, i))
    val containers = createContainers(hosts, ids)
    handler.handleAllocatedContainers(containers.slice(0, 9))
    val cs0 = ContainerStatus.newInstance(containers(0).getId, ContainerState.COMPLETE,
      "success", ContainerExitStatus.SUCCESS)
    val cs1 = ContainerStatus.newInstance(containers(1).getId, ContainerState.COMPLETE,
      "preempted", ContainerExitStatus.PREEMPTED)
    val cs2 = ContainerStatus.newInstance(containers(2).getId, ContainerState.COMPLETE,
      "killed_exceeded_vmem", ContainerExitStatus.KILLED_EXCEEDED_VMEM)
    val cs3 = ContainerStatus.newInstance(containers(3).getId, ContainerState.COMPLETE,
      "killed_exceeded_pmem", ContainerExitStatus.KILLED_EXCEEDED_PMEM)
    val cs4 = ContainerStatus.newInstance(containers(4).getId, ContainerState.COMPLETE,
      "killed_by_resourcemanager", ContainerExitStatus.KILLED_BY_RESOURCEMANAGER)
    val cs5 = ContainerStatus.newInstance(containers(5).getId, ContainerState.COMPLETE,
      "killed_by_appmaster", ContainerExitStatus.KILLED_BY_APPMASTER)
    val cs6 = ContainerStatus.newInstance(containers(6).getId, ContainerState.COMPLETE,
      "killed_after_app_completion", ContainerExitStatus.KILLED_AFTER_APP_COMPLETION)
    val cs7 = ContainerStatus.newInstance(containers(7).getId, ContainerState.COMPLETE,
      "aborted", ContainerExitStatus.ABORTED)
    val cs8 = ContainerStatus.newInstance(containers(8).getId, ContainerState.COMPLETE,
      "disk_failed", ContainerExitStatus.DISKS_FAILED)
    handler.processCompletedContainers(Seq(cs0, cs1, cs2, cs3, cs4, cs5, cs6, cs7, cs8))

    verify(rmClientSpy, never())
      .updateBlacklist(hosts.slice(0, 9).asJava, Collections.emptyList())

    handler.handleAllocatedContainers(Array(containers(9)))
    val cs9 = ContainerStatus.newInstance(containers(9).getId, ContainerState.COMPLETE,
      "invalid", ContainerExitStatus.INVALID)
    handler.processCompletedContainers(Seq(cs9))
    verify(rmClientSpy)
      .updateBlacklist(Seq(hosts(9)).asJava, Collections.emptyList())

    handler.handleAllocatedContainers(Array(containers(10)))
    val UNKNOWN_EXIT_CODE = 1
    val cs10 = ContainerStatus.newInstance(containers(10).getId, ContainerState.COMPLETE,
      "unknown_exit_code", UNKNOWN_EXIT_CODE)
    handler.processCompletedContainers(Seq(cs10))
    verify(rmClientSpy)
      .updateBlacklist(Seq(hosts(10)).asJava, Collections.emptyList())
  }
}
