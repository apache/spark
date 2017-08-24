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
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.YarnAllocator._
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

  var containerNum = 0

  override def beforeEach() {
    super.beforeEach()
    rmClient = AMRMClient.createAMRMClient()
    rmClient.init(conf)
    rmClient.start()
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
      rmClient: AMRMClient[ContainerRequest] = rmClient): YarnAllocator = {
    val args = Array(
      "--jar", "somejar.jar",
      "--class", "SomeClass")
    val sparkConfClone = sparkConf.clone()
      .set("spark.executor.instances", maxExecutors.toString)
      .set("spark.executor.cores", "5")
      .set("spark.executor.memory", "2048")
    new YarnAllocator(
      "not used",
      mock(classOf[RpcEndpointRef]),
      conf,
      sparkConfClone,
      rmClient,
      appAttemptId,
      new SecurityManager(sparkConf),
      Map(),
      new MockResolver())
  }

  def createContainer(host: String): Container = {
    // When YARN 2.6+ is required, avoid deprecation by using version with long second arg
    val containerId = ContainerId.newInstance(appAttemptId, containerNum)
    containerNum += 1
    val nodeId = NodeId.newInstance(host, 1000)
    Container.newInstance(containerId, nodeId, "", containerResource, RM_REQUEST_PRIORITY, null)
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
    handler.processCompletedContainers(statuses.toSeq)
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (1)
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
    handler.processCompletedContainers(statuses.toSeq)
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

    handler.requestTotalExecutorsWithPreferredLocalities(2, 0, Map(), Set("hostA", "hostB"))
    verify(mockAmClient).updateBlacklist(Seq("hostB").asJava, Seq[String]().asJava)

    handler.requestTotalExecutorsWithPreferredLocalities(3, 0, Map(), Set())
    verify(mockAmClient).updateBlacklist(Seq[String]().asJava, Seq("hostA", "hostB").asJava)
  }

  test("memory exceeded diagnostic regexes") {
    val diagnostics =
      "Container [pid=12465,containerID=container_1412887393566_0003_01_000002] is running " +
        "beyond physical memory limits. Current usage: 2.1 MB of 2 GB physical memory used; " +
        "5.8 GB of 4.2 GB virtual memory used. Killing container."
    val vmemMsg = memLimitExceededLogMessage(diagnostics, VMEM_EXCEEDED_PATTERN)
    val pmemMsg = memLimitExceededLogMessage(diagnostics, PMEM_EXCEEDED_PATTERN)
    assert(vmemMsg.contains("5.8 GB of 4.2 GB virtual memory used."))
    assert(pmemMsg.contains("2.1 MB of 2 GB physical memory used."))
  }

  test("window based failure executor counting") {
    sparkConf.set("spark.yarn.executor.failuresValidityInterval", "100s")
    val handler = createAllocator(4)
    val clock = new ManualClock(0L)
    handler.setClock(clock)

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

  test("SPARK-18769: limit requests according to RM's available resources") {
    // These match the configuration in createAllocator()
    val containerCpu = 5
    val containerMem = (2048 + YarnSparkHadoopUtil.MEMORY_OVERHEAD_MIN).toInt

    // Available resources returned by the mock client for the test.
    val normal = Resource.newInstance(containerMem * 10, containerCpu * 10)
    val lowMem = Resource.newInstance(containerMem * 2, containerCpu * 5)
    val lowCpu = Resource.newInstance(containerMem * 5, containerCpu * 1)
    val empty = Resource.newInstance(0, 0)

    val client = mock(classOf[AMRMClient[ContainerRequest]])
    val response = mock(classOf[AllocateResponse])
    when(response.getAllocatedContainers()).thenReturn(Collections.emptyList(),
      Collections.emptyList())
    when(response.getAvailableResources()).thenReturn(normal)
    when(client.allocate(anyFloat())).thenReturn(response)

    // Keep track of how many container requests were added to the client. The request count
    // needs to be reset to 0 after an allocation request, since the allocator only adds requests
    // and rely on the AMRMClient to clean up state internally.
    var requestCount = 0
    when(client.addContainerRequest(any(classOf[ContainerRequest]))).thenAnswer(
      new Answer[Unit]() {
        override def answer(unused: InvocationOnMock): Unit = {
          requestCount += 1
        }
      }
    )

    val allocator = createAllocator(0, client)

    // First allocation should not create any requests.
    allocator.allocateResources()
    assert(requestCount === 0)

    // Request 2 executors.
    allocator.requestTotalExecutorsWithPreferredLocalities(2, 0, Map(), Set())
    allocator.allocateResources()
    assert(requestCount === 2)
    requestCount = 0

    // Switch to "low memory" resources.
    when(response.getAvailableResources()).thenReturn(lowMem)
    allocator.allocateResources()
    requestCount = 0

    // Try to allocate a new container, verify that only 2 requests remain since that's what
    // "lowMem" supports.
    allocator.requestTotalExecutorsWithPreferredLocalities(3, 0, Map(), Set())
    allocator.allocateResources()
    assert(requestCount === 2)
    requestCount = 0

    // Switch to "low cpu" resources.
    when(response.getAvailableResources()).thenReturn(lowCpu)
    allocator.allocateResources()
    requestCount = 0

    // This will cause the number of requests to fall to 1, since that's all "lowCpu" allows to run.
    allocator.requestTotalExecutorsWithPreferredLocalities(3, 0, Map(), Set())
    allocator.allocateResources()
    assert(requestCount === 1)
    requestCount = 0

    // Switch to empty.
    when(response.getAvailableResources()).thenReturn(empty)
    allocator.allocateResources()
    requestCount = 0

    allocator.requestTotalExecutorsWithPreferredLocalities(3, 0, Map(), Set())
    allocator.allocateResources()
    assert(requestCount === 0)
    requestCount = 0

    // Switch back to normal.
    when(response.getAvailableResources()).thenReturn(normal)
    allocator.allocateResources()
    requestCount = 0

    allocator.requestTotalExecutorsWithPreferredLocalities(3, 0, Map(), Set())
    allocator.allocateResources()
    assert(requestCount === 3)

    // Switch bach to low CPU, and mock some state so that there are a few pending allocation
    // requests. This should cause these requests to be removed.
    when(response.getAvailableResources()).thenReturn(lowCpu)
    allocator.allocateResources()
    requestCount = 0

    val pending = (1 to 10).map { i =>
      val res = Resource.newInstance(containerMem, containerCpu)
      new ContainerRequest(res, Array("*"), null, YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)
    }.toList.asJava
    doReturn(List(pending).asJava)
      .when(client)
      .getMatchingRequests(any(classOf[Priority]), anyString(), any(classOf[Resource]))

    allocator.requestTotalExecutorsWithPreferredLocalities(1, 0, Map(), Set())
    allocator.allocateResources()
    verify(client, times(9)).removeContainerRequest(any(classOf[ContainerRequest]))
  }

}
